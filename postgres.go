package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	databaseStartupTimeout = 5 * time.Second
	uploadDatabaseTimeout  = 5 * time.Second
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
	return createUploadResult{
		Upload:  current.Upload,
		Created: created && current.Upload.UploadID == expectedUploadID,
	}, nil
}

func getUploadByID(
	ctx context.Context,
	pool *pgxpool.Pool,
	uploadID string,
) (uploadRepresentation, error) {
	databaseContext, cancelDatabase := context.WithTimeout(ctx, uploadDatabaseTimeout)
	defer cancelDatabase()
	var upload uploadRepresentation
	err := pool.QueryRow(databaseContext, `
		SELECT
			upload_id::text,
			state,
			declared_size,
			declared_content_type,
			upload_deadline
		FROM uploads
		WHERE upload_id::text = $1`,
		uploadID,
	).Scan(
		&upload.UploadID,
		&upload.State,
		&upload.DeclaredSizeBytes,
		&upload.DeclaredContentType,
		&upload.UploadDeadline,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return uploadRepresentation{}, errUploadNotFound
	}
	if err != nil {
		return uploadRepresentation{}, errServiceUnavailable
	}
	upload.UploadDeadline = upload.UploadDeadline.UTC()
	return upload, nil
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
	return upload, nil
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
