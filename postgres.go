package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	databaseStartupTimeout = 5 * time.Second
	uploadDatabaseTimeout  = 5 * time.Second
	databaseTxAttempts     = 2
)

func openPostgres(parent context.Context, databaseURL string) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, errors.New("postgres configuration is invalid")
	}
	poolConfig.ConnConfig.RuntimeParams["application_name"] = "s3-media-upload"

	ctx, cancel := context.WithTimeout(parent, databaseStartupTimeout)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, errors.New("postgres pool creation failed")
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, errors.New("postgres startup ping failed")
	}
	if err := checkSchemaV1(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

func checkSchemaV1(ctx context.Context, pool *pgxpool.Pool) error {
	var count int
	var latest int64
	err := pool.QueryRow(
		ctx,
		"SELECT count(*), COALESCE(max(version), 0) FROM schema_migrations",
	).Scan(&count, &latest)
	if err != nil {
		return errors.New("schema ledger check failed")
	}
	return validateSchemaV1(count, latest)
}

func validateSchemaV1(count int, latest int64) error {
	if count != 1 || latest != 1 {
		return errors.New("schema ledger must contain exactly version 1")
	}
	return nil
}

type storedUpload struct {
	Upload            uploadRepresentation
	StagingKey        string
	MaxWriteExpiresAt time.Time
	DatabaseNow       time.Time
}

const uploadRecordColumns = `
	upload_id::text,
	staging_key,
	state,
	declared_size,
	declared_content_type,
	upload_deadline,
	max_write_expires_at`

var errCompletionOutcomeUnknown = errors.New("completion outcome unknown")

type completionRecovery struct {
	finalizingDueBy *time.Time
}

func createOrReplayUpload(
	ctx context.Context,
	pool *pgxpool.Pool,
	presign func(context.Context, string, string) (uploadRequest, error),
	command createUploadCommand,
) (createUploadResult, error) {
	uploadID, err := newUploadUUID()
	if err != nil {
		return createUploadResult{}, errServiceUnavailable
	}
	stagingKey := "staging/" + uploadID
	request, err := presign(ctx, stagingKey, command.ContentType)
	if err != nil {
		return createUploadResult{}, uploadSigningError(err)
	}

	tx, databaseContext, cancelDatabase, err := beginUploadTransaction(ctx, pool)
	if err != nil {
		return createUploadResult{}, errServiceUnavailable
	}
	defer func() {
		_ = tx.Rollback(databaseContext)
		cancelDatabase()
	}()

	inserted, err := scanStoredUpload(tx.QueryRow(databaseContext, `
		WITH sampled AS MATERIALIZED (
			SELECT clock_timestamp() AS now
		)
		INSERT INTO uploads (
			upload_id,
			idempotency_key,
			staging_key,
			declared_size,
			declared_content_type,
			state,
			created_at,
			upload_deadline,
			max_write_expires_at
		)
		SELECT
			$1::uuid,
			$2::uuid,
			$3::text,
			$4::bigint,
			$5::text,
			'pending',
			sampled.now,
			sampled.now + interval '24 hours',
			$6::timestamptz
		FROM sampled
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING `+uploadRecordColumns+`, created_at`,
		uploadID,
		command.IdempotencyKey,
		stagingKey,
		command.SizeBytes,
		command.ContentType,
		request.ExpiresAt,
	))
	if err == nil {
		if err := tx.Commit(databaseContext); err != nil {
			cancelDatabase()
			return recoverUnknownUploadCommit(
				ctx,
				pool,
				command,
				uploadID,
				stagingKey,
				request,
				true,
			)
		}
		cancelDatabase()
		if !time.Now().UTC().Before(request.ExpiresAt) {
			return createUploadResult{}, errServiceUnavailable
		}
		inserted.Upload.UploadRequest = &request
		return createUploadResult{Upload: inserted.Upload, Created: true}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return createUploadResult{}, errServiceUnavailable
	}

	existing, err := scanStoredUpload(tx.QueryRow(databaseContext, `
		SELECT `+uploadRecordColumns+`, clock_timestamp()
		FROM uploads
		WHERE idempotency_key = $1::uuid`,
		command.IdempotencyKey,
	))
	if err != nil {
		return createUploadResult{}, errServiceUnavailable
	}
	if err := tx.Rollback(databaseContext); err != nil {
		cancelDatabase()
		return createUploadResult{}, errServiceUnavailable
	}
	cancelDatabase()
	if !uploadMatchesCommand(existing, command) {
		return createUploadResult{}, errIdempotencyKeyReused
	}
	if !uploadAcceptsWrites(existing) {
		if existing.Upload.State == "ready" || existing.Upload.State == "rejected" || existing.Upload.State == "expired" {
			representation, err := getUploadByID(ctx, pool, existing.Upload.UploadID)
			if err != nil {
				return createUploadResult{}, err
			}
			return createUploadResult{Upload: representation}, nil
		}
		return createUploadResult{Upload: existing.Upload}, nil
	}

	replayRequest, err := presign(ctx, existing.StagingKey, existing.Upload.DeclaredContentType)
	if err != nil {
		return createUploadResult{}, uploadSigningError(err)
	}
	return authorizeReplay(ctx, pool, command, existing, replayRequest)
}

func authorizeReplay(
	ctx context.Context,
	pool *pgxpool.Pool,
	command createUploadCommand,
	expected storedUpload,
	request uploadRequest,
) (createUploadResult, error) {
	tx, databaseContext, cancelDatabase, err := beginUploadTransaction(ctx, pool)
	if err != nil {
		return createUploadResult{}, errServiceUnavailable
	}
	defer func() {
		_ = tx.Rollback(databaseContext)
		cancelDatabase()
	}()

	locked, err := scanStoredUpload(tx.QueryRow(databaseContext, `
		SELECT `+uploadRecordColumns+`, created_at
		FROM uploads
		WHERE idempotency_key = $1::uuid
		FOR UPDATE`,
		command.IdempotencyKey,
	))
	if err != nil {
		return createUploadResult{}, errServiceUnavailable
	}
	if err := tx.QueryRow(databaseContext, "SELECT clock_timestamp()").Scan(&locked.DatabaseNow); err != nil {
		return createUploadResult{}, errServiceUnavailable
	}
	locked.DatabaseNow = locked.DatabaseNow.UTC()
	if !uploadMatchesCommand(locked, command) {
		if err := tx.Rollback(databaseContext); err != nil {
			cancelDatabase()
			return createUploadResult{}, errServiceUnavailable
		}
		cancelDatabase()
		return createUploadResult{}, errIdempotencyKeyReused
	}
	if locked.Upload.UploadID != expected.Upload.UploadID ||
		locked.StagingKey != expected.StagingKey ||
		!uploadAcceptsWrites(locked) {
		if err := tx.Rollback(databaseContext); err != nil {
			cancelDatabase()
			return createUploadResult{}, errServiceUnavailable
		}
		cancelDatabase()
		if locked.Upload.State == "ready" || locked.Upload.State == "rejected" || locked.Upload.State == "expired" {
			representation, err := getUploadByID(ctx, pool, locked.Upload.UploadID)
			if err != nil {
				return createUploadResult{}, err
			}
			return createUploadResult{Upload: representation}, nil
		}
		return createUploadResult{Upload: locked.Upload}, nil
	}

	result, err := tx.Exec(databaseContext, `
		UPDATE uploads
		SET max_write_expires_at = GREATEST(max_write_expires_at, $2::timestamptz)
		WHERE upload_id = $1::uuid`,
		locked.Upload.UploadID,
		request.ExpiresAt,
	)
	if err != nil || result.RowsAffected() != 1 {
		return createUploadResult{}, errServiceUnavailable
	}
	if err := tx.Commit(databaseContext); err != nil {
		cancelDatabase()
		return recoverUnknownUploadCommit(
			ctx,
			pool,
			command,
			locked.Upload.UploadID,
			locked.StagingKey,
			request,
			false,
		)
	}
	cancelDatabase()
	if !time.Now().UTC().Before(request.ExpiresAt) {
		return createUploadResult{}, errServiceUnavailable
	}
	locked.Upload.UploadRequest = &request
	return createUploadResult{Upload: locked.Upload}, nil
}

func recoverUnknownUploadCommit(
	ctx context.Context,
	pool *pgxpool.Pool,
	command createUploadCommand,
	expectedUploadID string,
	expectedStagingKey string,
	request uploadRequest,
	created bool,
) (createUploadResult, error) {
	databaseContext, cancelDatabase := context.WithTimeout(ctx, uploadDatabaseTimeout)
	defer cancelDatabase()
	var horizonCoversRequest bool
	current, err := scanStoredUpload(
		pool.QueryRow(databaseContext, `
			SELECT `+uploadRecordColumns+`, clock_timestamp(),
				max_write_expires_at >= $2::timestamptz
			FROM uploads
			WHERE idempotency_key = $1::uuid`,
			command.IdempotencyKey,
			request.ExpiresAt,
		),
		&horizonCoversRequest,
	)
	if err != nil {
		return createUploadResult{}, errServiceUnavailable
	}
	if !uploadMatchesCommand(current, command) {
		return createUploadResult{}, errIdempotencyKeyReused
	}

	expectedCapability := current.Upload.UploadID == expectedUploadID &&
		current.StagingKey == expectedStagingKey
	if uploadAcceptsWrites(current) {
		if !expectedCapability ||
			!horizonCoversRequest ||
			!current.DatabaseNow.Before(request.ExpiresAt) {
			return createUploadResult{}, errServiceUnavailable
		}
		current.Upload.UploadRequest = &request
	}
	if current.Upload.State == "ready" || current.Upload.State == "rejected" || current.Upload.State == "expired" {
		representation, err := getUploadByID(ctx, pool, current.Upload.UploadID)
		if err != nil {
			return createUploadResult{}, err
		}
		current.Upload = representation
	}
	return createUploadResult{
		Upload:  current.Upload,
		Created: created && current.Upload.UploadID == expectedUploadID,
	}, nil
}

func completeUploadByID(
	ctx context.Context,
	pool *pgxpool.Pool,
	uploadID string,
) (completeUploadResult, error) {
	result, recovery, err := completeUploadAttempt(ctx, pool, uploadID)
	if !errors.Is(err, errCompletionOutcomeUnknown) {
		return result, err
	}
	return recoverCompletionOutcome(ctx, pool, uploadID, recovery)
}

func completeUploadAttempt(
	ctx context.Context,
	pool *pgxpool.Pool,
	uploadID string,
) (completeUploadResult, completionRecovery, error) {
	tx, databaseContext, cancelDatabase, err := beginUploadTransaction(ctx, pool)
	if err != nil {
		return completeUploadResult{}, completionRecovery{}, errServiceUnavailable
	}
	defer func() {
		_ = tx.Rollback(databaseContext)
		cancelDatabase()
	}()

	locked, err := scanStoredUpload(tx.QueryRow(databaseContext, `
		SELECT `+uploadRecordColumns+`, created_at
		FROM uploads
		WHERE upload_id::text = $1
		FOR UPDATE`,
		uploadID,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Rollback(databaseContext); err != nil {
			cancelDatabase()
			return completeUploadResult{}, completionRecovery{}, errServiceUnavailable
		}
		cancelDatabase()
		return completeUploadResult{}, completionRecovery{}, errUploadNotFound
	}
	if err != nil {
		return completeUploadResult{}, completionRecovery{}, errServiceUnavailable
	}
	if err := tx.QueryRow(databaseContext, "SELECT clock_timestamp()").Scan(&locked.DatabaseNow); err != nil {
		return completeUploadResult{}, completionRecovery{}, errServiceUnavailable
	}
	locked.DatabaseNow = locked.DatabaseNow.UTC()

	result := completeUploadResult{Upload: locked.Upload}
	recovery := completionRecovery{}
	var rowsAffected int64
	switch locked.Upload.State {
	case "pending":
		if locked.DatabaseNow.Before(locked.Upload.UploadDeadline) {
			commandTag, updateErr := tx.Exec(databaseContext, `
				UPDATE uploads
				SET state = 'finalizing',
				    completion_requested_at = $2::timestamptz,
				    reconcile_after = $2::timestamptz
				WHERE upload_id = $1::uuid
				  AND state = 'pending'
				  AND upload_deadline > $2::timestamptz`,
				locked.Upload.UploadID,
				locked.DatabaseNow,
			)
			err = updateErr
			rowsAffected = commandTag.RowsAffected()
			result.Upload.State = "finalizing"
			result.Transition = "pending_to_finalizing"
		} else {
			applied, updateErr := transitionPendingExpiredLocked(databaseContext, tx, locked)
			err = updateErr
			if applied {
				rowsAffected = 1
			}
			result.Upload.State = "expired"
			result.Upload.Failure = &uploadFailure{Code: "upload_expired"}
			result.Transition = "pending_to_expired"
		}
	case "finalizing":
		recovery.finalizingDueBy = &locked.DatabaseNow
		commandTag, updateErr := tx.Exec(databaseContext, `
			UPDATE uploads
			SET reconcile_after = LEAST(reconcile_after, $2::timestamptz)
			WHERE upload_id = $1::uuid
			  AND state = 'finalizing'`,
			locked.Upload.UploadID,
			locked.DatabaseNow,
		)
		err = updateErr
		rowsAffected = commandTag.RowsAffected()
	case "ready", "rejected", "expired":
		if err := tx.Rollback(databaseContext); err != nil {
			cancelDatabase()
			return completeUploadResult{}, completionRecovery{}, errServiceUnavailable
		}
		cancelDatabase()
		representation, err := getUploadByID(ctx, pool, result.Upload.UploadID)
		if err != nil {
			return completeUploadResult{}, completionRecovery{}, err
		}
		result.Upload = representation
		return result, completionRecovery{}, nil
	default:
		return completeUploadResult{}, completionRecovery{}, errServiceUnavailable
	}
	if err != nil {
		return completeUploadResult{}, completionRecovery{}, errServiceUnavailable
	}
	if rowsAffected != 1 {
		_ = tx.Rollback(databaseContext)
		cancelDatabase()
		return completeUploadResult{}, recovery, errCompletionOutcomeUnknown
	}
	if err := tx.Commit(databaseContext); err != nil {
		cancelDatabase()
		return completeUploadResult{}, recovery, errCompletionOutcomeUnknown
	}
	cancelDatabase()
	return result, completionRecovery{}, nil
}

func recoverCompletionOutcome(
	ctx context.Context,
	pool *pgxpool.Pool,
	uploadID string,
	recovery completionRecovery,
) (completeUploadResult, error) {
	current, reconcileAfter, err := readCompletionUpload(ctx, pool, uploadID)
	if err != nil {
		return completeUploadResult{}, errServiceUnavailable
	}
	if completionOutcomeIsDurable(current.Upload.State, reconcileAfter, recovery) {
		return completionResult(ctx, pool, current)
	}
	result, secondRecovery, err := completeUploadAttempt(ctx, pool, uploadID)
	if !errors.Is(err, errCompletionOutcomeUnknown) {
		if err != nil {
			return completeUploadResult{}, errServiceUnavailable
		}
		return result, nil
	}
	current, reconcileAfter, err = readCompletionUpload(ctx, pool, uploadID)
	if err != nil || !completionOutcomeIsDurable(current.Upload.State, reconcileAfter, secondRecovery) {
		return completeUploadResult{}, errServiceUnavailable
	}
	return completionResult(ctx, pool, current)
}

func completionResult(
	ctx context.Context,
	pool *pgxpool.Pool,
	current storedUpload,
) (completeUploadResult, error) {
	if current.Upload.State != "ready" && current.Upload.State != "rejected" && current.Upload.State != "expired" {
		return completeUploadResult{Upload: current.Upload}, nil
	}
	representation, err := getUploadByID(ctx, pool, current.Upload.UploadID)
	if err != nil {
		return completeUploadResult{}, err
	}
	return completeUploadResult{Upload: representation}, nil
}

func readCompletionUpload(
	ctx context.Context,
	pool *pgxpool.Pool,
	uploadID string,
) (storedUpload, *time.Time, error) {
	databaseContext, cancelDatabase := context.WithTimeout(ctx, uploadDatabaseTimeout)
	defer cancelDatabase()
	var reconcileAfter *time.Time
	upload, err := scanStoredUpload(pool.QueryRow(databaseContext, `
		SELECT `+uploadRecordColumns+`, clock_timestamp(), reconcile_after
		FROM uploads
		WHERE upload_id::text = $1`,
		uploadID,
	), &reconcileAfter)
	if err != nil {
		return storedUpload{}, nil, err
	}
	if reconcileAfter != nil {
		utc := reconcileAfter.UTC()
		reconcileAfter = &utc
	}
	return upload, reconcileAfter, nil
}

func completionOutcomeIsDurable(
	state string,
	reconcileAfter *time.Time,
	recovery completionRecovery,
) bool {
	switch state {
	case "finalizing":
		return recovery.finalizingDueBy == nil ||
			(reconcileAfter != nil && !reconcileAfter.After(*recovery.finalizingDueBy))
	case "ready", "rejected", "expired":
		return true
	default:
		return false
	}
}

func getUploadByID(
	ctx context.Context,
	pool *pgxpool.Pool,
	uploadID string,
) (uploadRepresentation, error) {
	databaseContext, cancelDatabase := context.WithTimeout(ctx, uploadDatabaseTimeout)
	defer cancelDatabase()
	var upload uploadRepresentation
	var rejectionClass, rejectionReason *string
	var imageSize *int64
	var imageContentType *string
	var imageWidth, imageHeight *int
	err := pool.QueryRow(databaseContext, `
		SELECT
			u.upload_id::text,
			u.state,
			u.declared_size,
			u.declared_content_type,
			u.upload_deadline,
			u.rejection_class,
			u.rejection_reason,
			c.encoded_size,
			CASE c.image_format
				WHEN 'jpeg' THEN 'image/jpeg'
				WHEN 'png' THEN 'image/png'
			END,
			c.width,
			c.height
		FROM uploads u
		LEFT JOIN upload_candidates c
		  ON c.upload_id = u.upload_id
		 AND c.object_key = u.final_key
		WHERE u.upload_id::text = $1`,
		uploadID,
	).Scan(
		&upload.UploadID,
		&upload.State,
		&upload.DeclaredSizeBytes,
		&upload.DeclaredContentType,
		&upload.UploadDeadline,
		&rejectionClass,
		&rejectionReason,
		&imageSize,
		&imageContentType,
		&imageWidth,
		&imageHeight,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return uploadRepresentation{}, errUploadNotFound
	}
	if err != nil {
		return uploadRepresentation{}, errServiceUnavailable
	}
	upload.UploadDeadline = upload.UploadDeadline.UTC()
	switch upload.State {
	case "ready":
		if imageSize == nil || imageContentType == nil || imageWidth == nil || imageHeight == nil {
			return uploadRepresentation{}, errServiceUnavailable
		}
		upload.Image = &uploadImage{
			SizeBytes:   *imageSize,
			ContentType: *imageContentType,
			Width:       *imageWidth,
			Height:      *imageHeight,
		}
	case "rejected":
		if rejectionClass == nil || rejectionReason == nil {
			return uploadRepresentation{}, errServiceUnavailable
		}
		failureCode := publicFailureCode(*rejectionClass, *rejectionReason)
		if failureCode == "" {
			return uploadRepresentation{}, errServiceUnavailable
		}
		upload.Failure = &uploadFailure{Code: failureCode}
	}
	return decorateUploadRepresentation(upload), nil
}

func authorizeContentRead(
	ctx context.Context,
	pool *pgxpool.Pool,
	presign func(context.Context, string) (string, time.Time, error),
	uploadID string,
) (contentReadResult, error) {
	databaseContext, cancelDatabase := context.WithTimeout(ctx, uploadDatabaseTimeout)
	var result contentReadResult
	var finalKey, rejectionClass, rejectionReason *string
	err := pool.QueryRow(databaseContext, `
		SELECT
			upload_id::text,
			state,
			final_key,
			rejection_class,
			rejection_reason
		FROM uploads
		WHERE upload_id::text = $1`,
		uploadID,
	).Scan(
		&result.UploadID,
		&result.State,
		&finalKey,
		&rejectionClass,
		&rejectionReason,
	)
	cancelDatabase()
	if errors.Is(err, pgx.ErrNoRows) {
		return contentReadResult{}, errUploadNotFound
	}
	if err != nil {
		return contentReadResult{}, errServiceUnavailable
	}
	switch result.State {
	case "ready":
		if finalKey == nil {
			return contentReadResult{}, errServiceUnavailable
		}
		result.URL, result.ExpiresAt, err = presign(ctx, *finalKey)
		if err != nil {
			if errors.Is(err, errContentSigningInvalid) {
				return contentReadResult{}, err
			}
			return contentReadResult{}, errServiceUnavailable
		}
	case "rejected":
		if rejectionClass == nil || rejectionReason == nil {
			return contentReadResult{}, errServiceUnavailable
		}
		result.FailureCode = publicFailureCode(*rejectionClass, *rejectionReason)
		if result.FailureCode == "" {
			return contentReadResult{}, errServiceUnavailable
		}
	case "pending", "finalizing", "expired":
	default:
		return contentReadResult{}, errServiceUnavailable
	}
	return result, nil
}

func publicFailureCode(class, reason string) string {
	switch class {
	case "invalid_input":
		switch reason {
		case "object_too_large", "dimensions_limit_exceeded", "pixel_limit_exceeded":
			return "image_too_large"
		case "declared_size_mismatch", "invalid_image_encoding", "declared_content_type_mismatch", "malformed_image":
			return "invalid_image"
		}
	case "internal_invariant":
		if reason == "decoder_invariant_mismatch" || reason == "candidate_integrity_mismatch" {
			return "upload_processing_failed"
		}
	}
	return ""
}

func beginUploadTransaction(
	ctx context.Context,
	pool *pgxpool.Pool,
) (pgx.Tx, context.Context, context.CancelFunc, error) {
	databaseContext, cancelDatabase := context.WithTimeout(ctx, uploadDatabaseTimeout)
	tx, err := pool.BeginTx(databaseContext, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		cancelDatabase()
		return nil, nil, nil, err
	}
	if _, err := tx.Exec(databaseContext, "SET LOCAL lock_timeout = '1s'"); err != nil {
		_ = tx.Rollback(databaseContext)
		cancelDatabase()
		return nil, nil, nil, err
	}
	if _, err := tx.Exec(databaseContext, "SET LOCAL statement_timeout = '5s'"); err != nil {
		_ = tx.Rollback(databaseContext)
		cancelDatabase()
		return nil, nil, nil, err
	}
	return tx, databaseContext, cancelDatabase, nil
}

func retryUploadTransaction(
	ctx context.Context,
	pool *pgxpool.Pool,
	apply func(context.Context, pgx.Tx) (bool, error),
) (bool, bool, error) {
	for attempt := 0; attempt < databaseTxAttempts; attempt++ {
		tx, databaseContext, cancelDatabase, err := beginUploadTransaction(ctx, pool)
		if err != nil {
			if retryableTransactionError(err) && attempt+1 < databaseTxAttempts {
				continue
			}
			return false, false, err
		}

		commit, err := apply(databaseContext, tx)
		if err != nil || !commit {
			_ = tx.Rollback(databaseContext)
			cancelDatabase()
			if err != nil && retryableTransactionError(err) && attempt+1 < databaseTxAttempts {
				continue
			}
			return false, false, err
		}

		err = tx.Commit(databaseContext)
		cancelDatabase()
		if err == nil {
			return true, false, nil
		}
		if retryableTransactionError(err) && attempt+1 < databaseTxAttempts {
			continue
		}
		return false, !retryableTransactionError(err), err
	}
	return false, false, errors.New("database transaction retry exhausted")
}

func retryableTransactionError(err error) bool {
	var postgresError *pgconn.PgError
	return errors.As(err, &postgresError) &&
		(postgresError.Code == "40001" || postgresError.Code == "40P01")
}

func uploadSigningError(err error) error {
	if errors.Is(err, errUploadSigningInvalid) {
		return errUploadSigningInvalid
	}
	return errServiceUnavailable
}

func scanStoredUpload(row pgx.Row, trailing ...any) (storedUpload, error) {
	var upload storedUpload
	destinations := []any{
		&upload.Upload.UploadID,
		&upload.StagingKey,
		&upload.Upload.State,
		&upload.Upload.DeclaredSizeBytes,
		&upload.Upload.DeclaredContentType,
		&upload.Upload.UploadDeadline,
		&upload.MaxWriteExpiresAt,
		&upload.DatabaseNow,
	}
	destinations = append(destinations, trailing...)
	if err := row.Scan(destinations...); err != nil {
		return storedUpload{}, err
	}
	upload.Upload.UploadDeadline = upload.Upload.UploadDeadline.UTC()
	upload.MaxWriteExpiresAt = upload.MaxWriteExpiresAt.UTC()
	upload.DatabaseNow = upload.DatabaseNow.UTC()
	upload.Upload = decorateUploadRepresentation(upload.Upload)
	return upload, nil
}

func decorateUploadRepresentation(upload uploadRepresentation) uploadRepresentation {
	if upload.State == "expired" {
		upload.Failure = &uploadFailure{Code: "upload_expired"}
	}
	return upload
}

func uploadMatchesCommand(upload storedUpload, command createUploadCommand) bool {
	return upload.Upload.DeclaredSizeBytes == command.SizeBytes &&
		upload.Upload.DeclaredContentType == command.ContentType
}

func uploadAcceptsWrites(upload storedUpload) bool {
	return (upload.Upload.State == "pending" || upload.Upload.State == "finalizing") &&
		upload.DatabaseNow.Before(upload.Upload.UploadDeadline)
}

func newUploadUUID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	raw[6] = raw[6]&0x0f | 0x40
	raw[8] = raw[8]&0x3f | 0x80
	encoded := hex.EncodeToString(raw[:])
	return encoded[:8] + "-" +
		encoded[8:12] + "-" +
		encoded[12:16] + "-" +
		encoded[16:20] + "-" +
		encoded[20:], nil
}
