package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"expvar"
	"log/slog"
	"net"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const finalizerIdleWait = time.Second

var errCandidateRegistrationOutcomeUnknown = errors.New("candidate registration outcome unknown")

type finalizer struct {
	logger     *slog.Logger
	pool       *pgxpool.Pool
	storage    *s3.Client
	bucket     string
	claimLease time.Duration
}

type finalizeClaim struct {
	UploadID            string
	StagingKey          string
	DeclaredSize        int64
	DeclaredContentType string
	UploadDeadline      time.Time
	MaxWriteExpiresAt   time.Time
	Token               string
	ExpiresAt           time.Time
	FailureStreak       int
}

type trackedCandidate struct {
	SHA256                  [32]byte
	ObjectKey               string
	EncodedSize             int64
	ValidationPolicyVersion int16
	Format                  string
	Width                   int
	Height                  int
}

type retryUpdate struct {
	FailureStreak int
	Delay         time.Duration
	NextAttemptAt time.Time
}

type tombstoneTarget struct {
	ObjectKey       string
	CandidateSHA256 []byte
	DeleteNotBefore time.Time
}

func runFinalizer(
	ctx context.Context,
	logger *slog.Logger,
	pool *pgxpool.Pool,
	storage *s3.Client,
	bucket string,
	claimLease time.Duration,
) {
	worker := &finalizer{
		logger:     logger,
		pool:       pool,
		storage:    storage,
		bucket:     bucket,
		claimLease: claimLease,
	}
	runFinalizerLoop(ctx, worker.processNext)
}

func runFinalizerLoop(ctx context.Context, processNext func(context.Context) (bool, error)) {
	for {
		if ctx.Err() != nil {
			return
		}
		processed, _ := processNext(ctx)
		if ctx.Err() != nil {
			return
		}
		if processed {
			continue
		}
		timer := time.NewTimer(finalizerIdleWait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
	}
}

func (f *finalizer) processNext(ctx context.Context) (bool, error) {
	claim, ok, err := claimNextFinalizing(ctx, f.pool, f.claimLease)
	if err != nil || !ok {
		return false, err
	}
	finishClaim := f.phase(claim, "claim")
	finishClaim("claimed")

	candidates, err := loadTrackedCandidates(ctx, f.pool, claim)
	if err != nil {
		return true, f.retry(ctx, claim, "candidate_verify", classifyDatabaseError(err, false))
	}
	for _, candidate := range candidates {
		active, err := renewFinalizeClaim(ctx, f.pool, &claim, f.claimLease)
		if err != nil {
			return true, f.retry(ctx, claim, "candidate_verify", classifyDatabaseError(err, true))
		}
		if !active {
			return true, nil
		}
		finish := f.phase(claim, "candidate_verify")
		snapshot, readErr := captureS3Object(
			ctx,
			f.storage,
			f.bucket,
			candidate.ObjectKey,
			maxUploadSizeBytes,
		)
		if readErr != nil {
			if storageObjectMissing(readErr) {
				finish("missing")
				continue
			}
			finish("retry")
			return true, f.retry(ctx, claim, "candidate_verify", classifyStorageError(readErr, false))
		}
		if !candidateMatchesSnapshot(candidate, snapshot) {
			finish("mismatch")
			return true, f.reject(ctx, claim, candidateMismatchFailure(candidate, snapshot))
		}
		finish("verified")
		return true, f.ready(ctx, claim, candidate)
	}

	active, err := renewFinalizeClaim(ctx, f.pool, &claim, f.claimLease)
	if err != nil {
		return true, f.retry(ctx, claim, "staging_observe", classifyDatabaseError(err, true))
	}
	if !active {
		return true, nil
	}
	observedHorizon, expiryEligible, active, err := prepareMissingObservation(ctx, f.pool, claim)
	if err != nil {
		return true, f.retry(ctx, claim, "staging_observe", classifyDatabaseError(err, false))
	}
	if !active {
		return true, nil
	}
	finishObserve := f.phase(claim, "staging_observe")
	finishCapture := f.phase(claim, "staging_capture")
	staging, readErr := captureS3Object(
		ctx,
		f.storage,
		f.bucket,
		claim.StagingKey,
		maxUploadSizeBytes,
	)
	if readErr != nil {
		finishCapture("retry")
		if storageObjectMissing(readErr) {
			finishObserve("missing")
			if expiryEligible {
				return true, f.expireMissing(ctx, claim, observedHorizon, candidates)
			}
			return true, f.retry(ctx, claim, "staging_observe", "transient")
		}
		finishObserve("retry")
		return true, f.retry(ctx, claim, "staging_capture", classifyStorageError(readErr, false))
	}
	finishCapture("captured")
	finishObserve("observed")

	finishValidate := f.phase(claim, "validate")
	validated, failure, validationErr := validateImageBytes(
		staging.Bytes,
		staging.ContentLength,
		claim.DeclaredSize,
		claim.DeclaredContentType,
	)
	if validationErr != nil {
		finishValidate("retry")
		return true, f.retry(ctx, claim, "validate", "transient")
	}
	if failure != nil {
		finishValidate("rejected")
		return true, f.reject(ctx, claim, *failure)
	}
	finishValidate("validated")

	candidate := trackedCandidate{
		SHA256:                  validated.SHA256,
		ObjectKey:               "media/" + claim.UploadID + "/" + hex.EncodeToString(validated.SHA256[:]),
		EncodedSize:             validated.SizeBytes,
		ValidationPolicyVersion: 1,
		Format:                  validated.Format,
		Width:                   validated.Width,
		Height:                  validated.Height,
	}
	finishTrack := f.phase(claim, "candidate_track")
	active, mismatch, err := registerCandidate(ctx, f.pool, claim, candidate)
	if err != nil {
		finishTrack("retry")
		return true, f.retry(ctx, claim, "candidate_track", classifyDatabaseError(err, true))
	}
	if !active {
		finishTrack("stale")
		return true, nil
	}
	if mismatch {
		finishTrack("mismatch")
		return true, f.reject(ctx, claim, validationFailure{
			Class:  "internal_invariant",
			Reason: "decoder_invariant_mismatch",
			Phase:  "decode",
			Evidence: map[string]any{
				"policy_version": 1,
				"format":         validated.Format,
				"width":          validated.Width,
				"height":         validated.Height,
			},
		})
	}
	finishTrack("tracked")

	active, err = renewFinalizeClaim(ctx, f.pool, &claim, f.claimLease)
	if err != nil {
		return true, f.retry(ctx, claim, "candidate_put", classifyDatabaseError(err, true))
	}
	if !active {
		return true, nil
	}
	finishPut := f.phase(claim, "candidate_put")
	putErr := putCandidateObject(
		ctx,
		f.storage,
		f.bucket,
		candidate.ObjectKey,
		validated.Bytes,
		validated.ContentType,
	)
	if putErr != nil {
		finishPut("unknown")
	} else {
		finishPut("stored")
	}

	active, err = renewFinalizeClaim(ctx, f.pool, &claim, f.claimLease)
	if err != nil {
		return true, f.retry(ctx, claim, "candidate_verify", classifyDatabaseError(err, true))
	}
	if !active {
		return true, nil
	}
	finishVerify := f.phase(claim, "candidate_verify")
	published, verifyErr := captureS3Object(
		ctx,
		f.storage,
		f.bucket,
		candidate.ObjectKey,
		maxUploadSizeBytes,
	)
	if verifyErr != nil {
		finishVerify("retry")
		if putErr != nil {
			return true, f.retry(ctx, claim, "candidate_put", classifyStorageError(putErr, true))
		}
		if storageObjectMissing(verifyErr) {
			return true, f.retry(ctx, claim, "candidate_verify", "ambiguous")
		}
		return true, f.retry(ctx, claim, "candidate_verify", classifyStorageError(verifyErr, false))
	}
	if !candidateMatchesSnapshot(candidate, published) {
		finishVerify("mismatch")
		return true, f.reject(ctx, claim, candidateMismatchFailure(candidate, published))
	}
	finishVerify("verified")
	return true, f.ready(ctx, claim, candidate)
}

func (f *finalizer) phase(claim finalizeClaim, phase string) func(string) {
	started := time.Now()
	f.logger.Info(
		"finalize.phase",
		"upload_id", claim.UploadID,
		"phase", phase,
		"edge", "start",
	)
	return func(outcome string) {
		f.logger.Info(
			"finalize.phase",
			"upload_id", claim.UploadID,
			"phase", phase,
			"edge", "finish",
			"outcome", outcome,
			"duration_ms", time.Since(started).Milliseconds(),
			"failure_streak", claim.FailureStreak,
		)
	}
}

func (f *finalizer) retry(ctx context.Context, claim finalizeClaim, phase, errorClass string) error {
	update, applied, err := scheduleFinalizeRetry(ctx, f.pool, claim, phase, errorClass)
	if err != nil || !applied {
		return err
	}
	serviceMetrics.Get("retries_scheduled_total").(*expvar.Map).
		Get("finalizer_" + errorClass).(*expvar.Int).Add(1)
	f.logger.Info(
		"retry.scheduled",
		"upload_id", claim.UploadID,
		"retry_owner", "finalizer",
		"phase", phase,
		"error_class", errorClass,
		"failure_streak", update.FailureStreak,
		"delay_ms", update.Delay.Milliseconds(),
		"next_attempt_at", update.NextAttemptAt,
	)
	return nil
}

func (f *finalizer) ready(ctx context.Context, claim finalizeClaim, candidate trackedCandidate) error {
	finish := f.phase(claim, "terminal_commit")
	committed, err := transitionFinalizeReady(ctx, f.pool, claim, candidate)
	if err != nil {
		finish("retry")
		return f.retry(ctx, claim, "terminal_commit", classifyDatabaseError(err, true))
	}
	if !committed {
		finish("stale")
		return nil
	}
	finish("ready")
	serviceMetrics.Get("upload_transitions_total").(*expvar.Map).
		Get("finalizing_to_ready").(*expvar.Int).Add(1)
	f.logger.Info(
		"upload.transition",
		"upload_id", claim.UploadID,
		"state_from", "finalizing",
		"state_to", "ready",
		"trigger", "finalizer",
	)
	return nil
}

func (f *finalizer) reject(ctx context.Context, claim finalizeClaim, failure validationFailure) error {
	finish := f.phase(claim, "terminal_commit")
	committed, err := transitionFinalizeRejected(ctx, f.pool, claim, failure)
	if err != nil {
		finish("retry")
		return f.retry(ctx, claim, "terminal_commit", classifyDatabaseError(err, true))
	}
	if !committed {
		finish("stale")
		return nil
	}
	finish("rejected")
	serviceMetrics.Get("upload_transitions_total").(*expvar.Map).
		Get("finalizing_to_rejected").(*expvar.Int).Add(1)
	serviceMetrics.Get("validation_rejects_total").(*expvar.Map).
		Get(failure.Reason).(*expvar.Int).Add(1)
	f.logger.Info(
		"upload.transition",
		"upload_id", claim.UploadID,
		"state_from", "finalizing",
		"state_to", "rejected",
		"trigger", "finalizer",
		"reason_code", failure.Reason,
	)
	return nil
}

func claimNextFinalizing(
	ctx context.Context,
	pool *pgxpool.Pool,
	lease time.Duration,
) (finalizeClaim, bool, error) {
	token, err := newUploadUUID()
	if err != nil {
		return finalizeClaim{}, false, err
	}
	tx, databaseContext, cancelDatabase, err := beginUploadTransaction(ctx, pool)
	if err != nil {
		return finalizeClaim{}, false, err
	}
	defer func() {
		_ = tx.Rollback(databaseContext)
		cancelDatabase()
	}()

	var claim finalizeClaim
	err = tx.QueryRow(databaseContext, `
		SELECT
			upload_id::text,
			staging_key,
			declared_size,
			declared_content_type,
			upload_deadline,
			max_write_expires_at,
			reconcile_failure_streak
		FROM uploads
		WHERE state = 'finalizing'
		  AND reconcile_after <= clock_timestamp()
		  AND (claim_token IS NULL OR claim_expires_at <= clock_timestamp())
		ORDER BY reconcile_after, upload_id
		FOR UPDATE SKIP LOCKED
		LIMIT 1`).Scan(
		&claim.UploadID,
		&claim.StagingKey,
		&claim.DeclaredSize,
		&claim.DeclaredContentType,
		&claim.UploadDeadline,
		&claim.MaxWriteExpiresAt,
		&claim.FailureStreak,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return finalizeClaim{}, false, nil
	}
	if err != nil {
		return finalizeClaim{}, false, err
	}
	var databaseNow time.Time
	if err := tx.QueryRow(databaseContext, "SELECT clock_timestamp()").Scan(&databaseNow); err != nil {
		return finalizeClaim{}, false, err
	}
	claim.Token = token
	claim.ExpiresAt = databaseNow.Add(lease)
	result, err := tx.Exec(databaseContext, `
		UPDATE uploads
		SET claim_token = $2::uuid,
		    claim_expires_at = $3::timestamptz
		WHERE upload_id = $1::uuid
		  AND state = 'finalizing'`,
		claim.UploadID,
		claim.Token,
		claim.ExpiresAt,
	)
	if err != nil || result.RowsAffected() != 1 {
		return finalizeClaim{}, false, err
	}
	if err := tx.Commit(databaseContext); err != nil {
		cancelDatabase()
		return confirmFinalizeClaim(ctx, pool, claim)
	}
	cancelDatabase()
	claim.UploadDeadline = claim.UploadDeadline.UTC()
	claim.MaxWriteExpiresAt = claim.MaxWriteExpiresAt.UTC()
	claim.ExpiresAt = claim.ExpiresAt.UTC()
	return claim, true, nil
}

func confirmFinalizeClaim(
	ctx context.Context,
	pool *pgxpool.Pool,
	expected finalizeClaim,
) (finalizeClaim, bool, error) {
	databaseContext, cancelDatabase := context.WithTimeout(ctx, uploadDatabaseTimeout)
	defer cancelDatabase()
	var claim finalizeClaim
	err := pool.QueryRow(databaseContext, `
		SELECT
			upload_id::text,
			staging_key,
			declared_size,
			declared_content_type,
			upload_deadline,
			max_write_expires_at,
			claim_token::text,
			claim_expires_at,
			reconcile_failure_streak
		FROM uploads
		WHERE upload_id = $1::uuid
		  AND state = 'finalizing'
		  AND claim_token = $2::uuid
		  AND claim_expires_at > clock_timestamp()`,
		expected.UploadID,
		expected.Token,
	).Scan(
		&claim.UploadID,
		&claim.StagingKey,
		&claim.DeclaredSize,
		&claim.DeclaredContentType,
		&claim.UploadDeadline,
		&claim.MaxWriteExpiresAt,
		&claim.Token,
		&claim.ExpiresAt,
		&claim.FailureStreak,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return finalizeClaim{}, false, nil
	}
	if err != nil {
		return finalizeClaim{}, false, err
	}
	claim.UploadDeadline = claim.UploadDeadline.UTC()
	claim.MaxWriteExpiresAt = claim.MaxWriteExpiresAt.UTC()
	claim.ExpiresAt = claim.ExpiresAt.UTC()
	return claim, true, nil
}

func renewFinalizeClaim(
	ctx context.Context,
	pool *pgxpool.Pool,
	claim *finalizeClaim,
	lease time.Duration,
) (bool, error) {
	tx, databaseContext, cancelDatabase, err := beginUploadTransaction(ctx, pool)
	if err != nil {
		return false, err
	}
	defer func() {
		_ = tx.Rollback(databaseContext)
		cancelDatabase()
	}()
	var currentExpiry time.Time
	err = tx.QueryRow(databaseContext, `
		SELECT claim_expires_at
		FROM uploads
		WHERE upload_id = $1::uuid
		  AND state = 'finalizing'
		  AND claim_token = $2::uuid
		FOR UPDATE`,
		claim.UploadID,
		claim.Token,
	).Scan(&currentExpiry)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var databaseNow time.Time
	if err := tx.QueryRow(databaseContext, "SELECT clock_timestamp()").Scan(&databaseNow); err != nil {
		return false, err
	}
	if !databaseNow.Before(currentExpiry) {
		return false, nil
	}
	nextExpiry := databaseNow.Add(lease)
	result, err := tx.Exec(databaseContext, `
		UPDATE uploads
		SET claim_expires_at = $3::timestamptz
		WHERE upload_id = $1::uuid
		  AND state = 'finalizing'
		  AND claim_token = $2::uuid`,
		claim.UploadID,
		claim.Token,
		nextExpiry,
	)
	if err != nil || result.RowsAffected() != 1 {
		return false, err
	}
	if err := tx.Commit(databaseContext); err != nil {
		cancelDatabase()
		storedExpiry, confirmed, confirmErr := confirmFinalizeRenewal(ctx, pool, *claim, nextExpiry)
		if confirmed {
			claim.ExpiresAt = storedExpiry
		}
		return confirmed, confirmErr
	}
	cancelDatabase()
	claim.ExpiresAt = nextExpiry.UTC()
	return true, nil
}

func confirmFinalizeRenewal(
	ctx context.Context,
	pool *pgxpool.Pool,
	claim finalizeClaim,
	expectedExpiry time.Time,
) (time.Time, bool, error) {
	databaseContext, cancelDatabase := context.WithTimeout(ctx, uploadDatabaseTimeout)
	defer cancelDatabase()
	var storedExpiry time.Time
	err := pool.QueryRow(databaseContext, `
		SELECT claim_expires_at
		FROM uploads
		WHERE upload_id = $1::uuid
		  AND state = 'finalizing'
		  AND claim_token = $2::uuid
		  AND claim_expires_at >= $3::timestamptz
		  AND claim_expires_at > clock_timestamp()`,
		claim.UploadID,
		claim.Token,
		expectedExpiry,
	).Scan(&storedExpiry)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	return storedExpiry.UTC(), true, nil
}

func loadTrackedCandidates(
	ctx context.Context,
	pool *pgxpool.Pool,
	claim finalizeClaim,
) ([]trackedCandidate, error) {
	databaseContext, cancelDatabase := context.WithTimeout(ctx, uploadDatabaseTimeout)
	defer cancelDatabase()
	rows, err := pool.Query(databaseContext, `
		SELECT
			c.sha256,
			c.object_key,
			c.encoded_size,
			c.validation_policy_version,
			c.image_format,
			c.width,
			c.height
		FROM upload_candidates c
		JOIN uploads u ON u.upload_id = c.upload_id
		WHERE c.upload_id = $1::uuid
		  AND u.state = 'finalizing'
		  AND u.claim_token = $2::uuid
		  AND u.claim_expires_at > clock_timestamp()
		ORDER BY c.object_key`,
		claim.UploadID,
		claim.Token,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var candidates []trackedCandidate
	for rows.Next() {
		candidate, err := scanTrackedCandidate(rows)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, candidate)
	}
	return candidates, rows.Err()
}

func registerCandidate(
	ctx context.Context,
	pool *pgxpool.Pool,
	claim finalizeClaim,
	candidate trackedCandidate,
) (bool, bool, error) {
	var active, mismatch bool
	committed, outcomeUnknown, err := retryUploadTransaction(
		ctx,
		pool,
		func(databaseContext context.Context, tx pgx.Tx) (bool, error) {
			active, mismatch = false, false
			var lockErr error
			_, active, lockErr = lockFinalizeParent(databaseContext, tx, claim)
			if lockErr != nil || !active {
				return false, lockErr
			}
			_, err := tx.Exec(databaseContext, `
				INSERT INTO upload_candidates (
					upload_id,
					sha256,
					object_key,
					encoded_size,
					validation_policy_version,
					image_format,
					width,
					height,
					registered_at
				)
				VALUES (
					$1::uuid,
					$2::bytea,
					$3::text,
					$4::bigint,
					$5::smallint,
					$6::text,
					$7::integer,
					$8::integer,
					clock_timestamp()
				)
				ON CONFLICT (upload_id, sha256) DO NOTHING`,
				claim.UploadID,
				candidate.SHA256[:],
				candidate.ObjectKey,
				candidate.EncodedSize,
				candidate.ValidationPolicyVersion,
				candidate.Format,
				candidate.Width,
				candidate.Height,
			)
			if err != nil {
				return false, err
			}
			stored, err := scanTrackedCandidate(tx.QueryRow(databaseContext, `
				SELECT
					sha256,
					object_key,
					encoded_size,
					validation_policy_version,
					image_format,
					width,
					height
				FROM upload_candidates
				WHERE upload_id = $1::uuid
				  AND sha256 = $2::bytea
				FOR UPDATE`,
				claim.UploadID,
				candidate.SHA256[:],
			))
			if err != nil {
				return false, err
			}
			mismatch = !sameCandidate(stored, candidate)
			return !mismatch, nil
		},
	)
	if outcomeUnknown {
		exact, confirmErr := candidateTracked(ctx, pool, claim.UploadID, candidate)
		if confirmErr != nil {
			return false, false, confirmErr
		}
		if !exact {
			return false, false, errCandidateRegistrationOutcomeUnknown
		}
		return true, false, nil
	}
	if err != nil {
		return false, false, err
	}
	if mismatch {
		return true, true, nil
	}
	return active && committed, false, nil
}

func candidateTracked(
	ctx context.Context,
	pool *pgxpool.Pool,
	uploadID string,
	expected trackedCandidate,
) (bool, error) {
	databaseContext, cancelDatabase := context.WithTimeout(ctx, uploadDatabaseTimeout)
	defer cancelDatabase()
	stored, err := scanTrackedCandidate(pool.QueryRow(databaseContext, `
		SELECT
			sha256,
			object_key,
			encoded_size,
			validation_policy_version,
			image_format,
			width,
			height
		FROM upload_candidates
		WHERE upload_id = $1::uuid
		  AND sha256 = $2::bytea`,
		uploadID,
		expected.SHA256[:],
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return sameCandidate(stored, expected), nil
}

func scanTrackedCandidate(row pgx.Row) (trackedCandidate, error) {
	var candidate trackedCandidate
	var digest []byte
	err := row.Scan(
		&digest,
		&candidate.ObjectKey,
		&candidate.EncodedSize,
		&candidate.ValidationPolicyVersion,
		&candidate.Format,
		&candidate.Width,
		&candidate.Height,
	)
	if err != nil {
		return trackedCandidate{}, err
	}
	if len(digest) != len(candidate.SHA256) {
		return trackedCandidate{}, errors.New("invalid candidate digest")
	}
	copy(candidate.SHA256[:], digest)
	return candidate, nil
}

func sameCandidate(left, right trackedCandidate) bool {
	return left.SHA256 == right.SHA256 &&
		left.ObjectKey == right.ObjectKey &&
		left.EncodedSize == right.EncodedSize &&
		left.ValidationPolicyVersion == right.ValidationPolicyVersion &&
		left.Format == right.Format &&
		left.Width == right.Width &&
		left.Height == right.Height
}

func candidateMatchesSnapshot(candidate trackedCandidate, snapshot objectSnapshot) bool {
	if int64(len(snapshot.Bytes)) != candidate.EncodedSize {
		return false
	}
	return sha256.Sum256(snapshot.Bytes) == candidate.SHA256
}

func candidateMismatchFailure(candidate trackedCandidate, snapshot objectSnapshot) validationFailure {
	observedSize := int64(len(snapshot.Bytes))
	if snapshot.ContentLength > observedSize {
		observedSize = snapshot.ContentLength
	}
	evidence := map[string]any{
		"policy_version":  1,
		"expected_size":   candidate.EncodedSize,
		"observed_size":   observedSize,
		"expected_sha256": hex.EncodeToString(candidate.SHA256[:]),
	}
	if snapshot.Bytes != nil {
		observedDigest := sha256.Sum256(snapshot.Bytes)
		evidence["observed_sha256"] = hex.EncodeToString(observedDigest[:])
	}
	return validationFailure{
		Class:    "internal_invariant",
		Reason:   "candidate_integrity_mismatch",
		Phase:    "candidate_verify",
		Evidence: evidence,
	}
}

func transitionFinalizeReady(
	ctx context.Context,
	pool *pgxpool.Pool,
	claim finalizeClaim,
	selected trackedCandidate,
) (bool, error) {
	committed, outcomeUnknown, err := retryUploadTransaction(
		ctx,
		pool,
		func(databaseContext context.Context, tx pgx.Tx) (bool, error) {
			parent, active, err := lockFinalizeParent(databaseContext, tx, claim)
			if err != nil || !active {
				return false, err
			}
			candidates, err := lockAllCandidates(databaseContext, tx, claim.UploadID)
			if err != nil {
				return false, err
			}
			found := false
			for _, candidate := range candidates {
				if sameCandidate(candidate, selected) {
					found = true
				}
			}
			if !found {
				return false, errors.New("selected candidate is not tracked")
			}
			var selectedTombstoned bool
			if err := tx.QueryRow(databaseContext, `
				SELECT EXISTS (
					SELECT 1
					FROM cleanup_tombstones
					WHERE object_key = $1::text
				)`,
				selected.ObjectKey,
			).Scan(&selectedTombstoned); err != nil {
				return false, err
			}
			if selectedTombstoned {
				return false, errors.New("selected candidate is tombstoned")
			}
			targets := terminalTombstones(parent, candidates, selected.ObjectKey)
			if err := ensureTombstones(databaseContext, tx, claim.UploadID, targets); err != nil {
				return false, err
			}
			result, err := tx.Exec(databaseContext, `
				UPDATE uploads
				SET state = 'ready',
				    reconcile_after = NULL,
				    claim_token = NULL,
				    claim_expires_at = NULL,
				    terminal_at = $3::timestamptz,
				    final_key = $4::text
				WHERE upload_id = $1::uuid
				  AND state = 'finalizing'
				  AND claim_token = $2::uuid
				  AND claim_expires_at > $3::timestamptz`,
				claim.UploadID,
				claim.Token,
				parent.DatabaseNow,
				selected.ObjectKey,
			)
			return err == nil && result.RowsAffected() == 1, err
		},
	)
	if outcomeUnknown {
		confirmed, confirmErr := confirmReady(ctx, pool, claim.UploadID, selected.ObjectKey)
		return resolveTerminalCommit(err, confirmed, confirmErr)
	}
	return committed, err
}

func transitionFinalizeRejected(
	ctx context.Context,
	pool *pgxpool.Pool,
	claim finalizeClaim,
	failure validationFailure,
) (bool, error) {
	evidence, err := json.Marshal(failure.Evidence)
	if err != nil || len(failure.Evidence) == 0 {
		return false, errors.New("invalid rejection evidence")
	}
	committed, outcomeUnknown, err := retryUploadTransaction(
		ctx,
		pool,
		func(databaseContext context.Context, tx pgx.Tx) (bool, error) {
			parent, active, err := lockFinalizeParent(databaseContext, tx, claim)
			if err != nil || !active {
				return false, err
			}
			candidates, err := lockAllCandidates(databaseContext, tx, claim.UploadID)
			if err != nil {
				return false, err
			}
			targets := terminalTombstones(parent, candidates, "")
			if err := ensureTombstones(databaseContext, tx, claim.UploadID, targets); err != nil {
				return false, err
			}
			result, err := tx.Exec(databaseContext, `
				UPDATE uploads
				SET state = 'rejected',
				    reconcile_after = NULL,
				    claim_token = NULL,
				    claim_expires_at = NULL,
				    terminal_at = $3::timestamptz,
				    rejection_class = $4::text,
				    rejection_reason = $5::text,
				    rejection_policy_version = 1,
				    rejection_phase = $6::text,
				    rejection_evidence = $7::jsonb
				WHERE upload_id = $1::uuid
				  AND state = 'finalizing'
				  AND claim_token = $2::uuid
				  AND claim_expires_at > $3::timestamptz`,
				claim.UploadID,
				claim.Token,
				parent.DatabaseNow,
				failure.Class,
				failure.Reason,
				failure.Phase,
				string(evidence),
			)
			return err == nil && result.RowsAffected() == 1, err
		},
	)
	if outcomeUnknown {
		confirmed, confirmErr := confirmRejected(ctx, pool, claim.UploadID, failure.Reason)
		return resolveTerminalCommit(err, confirmed, confirmErr)
	}
	return committed, err
}

func resolveTerminalCommit(commitErr error, confirmed bool, confirmErr error) (bool, error) {
	if confirmErr != nil {
		return false, confirmErr
	}
	if confirmed {
		return true, nil
	}
	return false, commitErr
}

type lockedFinalizeUpload struct {
	DatabaseNow       time.Time
	UploadDeadline    time.Time
	MaxWriteExpiresAt time.Time
	StagingKey        string
	FailureStreak     int
}

func lockFinalizeParent(
	ctx context.Context,
	tx pgx.Tx,
	claim finalizeClaim,
) (lockedFinalizeUpload, bool, error) {
	var parent lockedFinalizeUpload
	var claimExpiresAt time.Time
	err := tx.QueryRow(ctx, `
		SELECT
			staging_key,
			upload_deadline,
			max_write_expires_at,
			reconcile_failure_streak,
			claim_expires_at
		FROM uploads
		WHERE upload_id = $1::uuid
		  AND state = 'finalizing'
		  AND claim_token = $2::uuid
		FOR UPDATE`,
		claim.UploadID,
		claim.Token,
	).Scan(
		&parent.StagingKey,
		&parent.UploadDeadline,
		&parent.MaxWriteExpiresAt,
		&parent.FailureStreak,
		&claimExpiresAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return lockedFinalizeUpload{}, false, nil
	}
	if err != nil {
		return lockedFinalizeUpload{}, false, err
	}
	if err := tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&parent.DatabaseNow); err != nil {
		return lockedFinalizeUpload{}, false, err
	}
	if !parent.DatabaseNow.Before(claimExpiresAt) {
		return lockedFinalizeUpload{}, false, nil
	}
	parent.DatabaseNow = parent.DatabaseNow.UTC()
	parent.UploadDeadline = parent.UploadDeadline.UTC()
	parent.MaxWriteExpiresAt = parent.MaxWriteExpiresAt.UTC()
	return parent, true, nil
}

func lockAllCandidates(ctx context.Context, tx pgx.Tx, uploadID string) ([]trackedCandidate, error) {
	rows, err := tx.Query(ctx, `
		SELECT
			sha256,
			object_key,
			encoded_size,
			validation_policy_version,
			image_format,
			width,
			height
		FROM upload_candidates
		WHERE upload_id = $1::uuid
		ORDER BY object_key
		FOR UPDATE`,
		uploadID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var candidates []trackedCandidate
	for rows.Next() {
		candidate, err := scanTrackedCandidate(rows)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, candidate)
	}
	return candidates, rows.Err()
}

func terminalTombstones(
	parent lockedFinalizeUpload,
	candidates []trackedCandidate,
	selectedKey string,
) []tombstoneTarget {
	targets := []tombstoneTarget{{
		ObjectKey:       parent.StagingKey,
		DeleteNotBefore: parent.MaxWriteExpiresAt,
	}}
	for _, candidate := range candidates {
		if candidate.ObjectKey == selectedKey {
			continue
		}
		digest := append([]byte(nil), candidate.SHA256[:]...)
		targets = append(targets, tombstoneTarget{
			ObjectKey:       candidate.ObjectKey,
			CandidateSHA256: digest,
			DeleteNotBefore: parent.DatabaseNow,
		})
	}
	sort.Slice(targets, func(left, right int) bool {
		return targets[left].ObjectKey < targets[right].ObjectKey
	})
	return targets
}

func ensureTombstones(
	ctx context.Context,
	tx pgx.Tx,
	uploadID string,
	targets []tombstoneTarget,
) error {
	for _, target := range targets {
		_, err := tx.Exec(ctx, `
			INSERT INTO cleanup_tombstones (
				object_key,
				upload_id,
				candidate_sha256,
				created_at,
				delete_not_before,
				next_attempt_at
			)
			VALUES (
				$1::text,
				$2::uuid,
				$3::bytea,
				clock_timestamp(),
				$4::timestamptz,
				$4::timestamptz
			)
			ON CONFLICT (object_key) DO NOTHING`,
			target.ObjectKey,
			uploadID,
			target.CandidateSHA256,
			target.DeleteNotBefore,
		)
		if err != nil {
			return err
		}
		var storedUploadID string
		var storedDigest []byte
		var storedDeleteNotBefore time.Time
		if err := tx.QueryRow(ctx, `
			SELECT upload_id::text, candidate_sha256, delete_not_before
			FROM cleanup_tombstones
			WHERE object_key = $1::text
			FOR UPDATE`,
			target.ObjectKey,
		).Scan(&storedUploadID, &storedDigest, &storedDeleteNotBefore); err != nil {
			return err
		}
		if storedUploadID != uploadID ||
			!bytes.Equal(storedDigest, target.CandidateSHA256) ||
			storedDeleteNotBefore.Before(target.DeleteNotBefore) {
			return errors.New("conflicting cleanup tombstone")
		}
	}
	return nil
}

func scheduleFinalizeRetry(
	ctx context.Context,
	pool *pgxpool.Pool,
	claim finalizeClaim,
	phase, errorClass string,
) (retryUpdate, bool, error) {
	tx, databaseContext, cancelDatabase, err := beginUploadTransaction(ctx, pool)
	if err != nil {
		return retryUpdate{}, false, err
	}
	defer func() {
		_ = tx.Rollback(databaseContext)
		cancelDatabase()
	}()
	parent, active, err := lockFinalizeParent(databaseContext, tx, claim)
	if err != nil || !active {
		return retryUpdate{}, false, err
	}
	streak := parent.FailureStreak + 1
	delay := finalizerRetryDelay(streak, errorClass)
	nextAttempt := parent.DatabaseNow.Add(delay)
	result, err := tx.Exec(databaseContext, `
		UPDATE uploads
		SET reconcile_after = $3::timestamptz,
		    reconcile_failure_streak = $4::integer,
		    reconcile_last_failure_class = $5::text,
		    reconcile_last_failure_at = $6::timestamptz,
		    claim_token = NULL,
		    claim_expires_at = NULL
		WHERE upload_id = $1::uuid
		  AND state = 'finalizing'
		  AND claim_token = $2::uuid`,
		claim.UploadID,
		claim.Token,
		nextAttempt,
		streak,
		errorClass,
		parent.DatabaseNow,
	)
	if err != nil || result.RowsAffected() != 1 {
		return retryUpdate{}, false, err
	}
	update := retryUpdate{
		FailureStreak: streak,
		Delay:         delay,
		NextAttemptAt: nextAttempt.UTC(),
	}
	if err := tx.Commit(databaseContext); err != nil {
		cancelDatabase()
		confirmed, confirmErr := confirmRetry(ctx, pool, claim.UploadID, update, errorClass)
		return update, confirmed, confirmErr
	}
	cancelDatabase()
	return update, true, nil
}

func finalizerRetryDelay(streak int, errorClass string) time.Duration {
	if errorClass == "auth" || errorClass == "configuration" || errorClass == "other_deterministic" {
		return time.Minute
	}
	if streak < 1 {
		streak = 1
	}
	delay := time.Second << min(streak-1, 6)
	if delay > time.Minute {
		return time.Minute
	}
	return delay
}

func confirmRetry(
	ctx context.Context,
	pool *pgxpool.Pool,
	uploadID string,
	expected retryUpdate,
	errorClass string,
) (bool, error) {
	databaseContext, cancelDatabase := context.WithTimeout(ctx, uploadDatabaseTimeout)
	defer cancelDatabase()
	var streak int
	var storedClass string
	var nextAttempt time.Time
	var claimReleased bool
	err := pool.QueryRow(databaseContext, `
		SELECT
			reconcile_failure_streak,
			reconcile_last_failure_class,
			reconcile_after,
			claim_token IS NULL
		FROM uploads
		WHERE upload_id = $1::uuid
		  AND state = 'finalizing'`,
		uploadID,
	).Scan(&streak, &storedClass, &nextAttempt, &claimReleased)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return streak == expected.FailureStreak &&
		storedClass == errorClass &&
		nextAttempt.Equal(expected.NextAttemptAt) &&
		claimReleased, nil
}

func confirmReady(
	ctx context.Context,
	pool *pgxpool.Pool,
	uploadID, finalKey string,
) (bool, error) {
	databaseContext, cancelDatabase := context.WithTimeout(ctx, uploadDatabaseTimeout)
	defer cancelDatabase()
	var storedKey string
	err := pool.QueryRow(databaseContext, `
		SELECT final_key
		FROM uploads
		WHERE upload_id = $1::uuid
		  AND state = 'ready'`,
		uploadID,
	).Scan(&storedKey)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return storedKey == finalKey, err
}

func confirmRejected(
	ctx context.Context,
	pool *pgxpool.Pool,
	uploadID, reason string,
) (bool, error) {
	databaseContext, cancelDatabase := context.WithTimeout(ctx, uploadDatabaseTimeout)
	defer cancelDatabase()
	var storedReason string
	err := pool.QueryRow(databaseContext, `
		SELECT rejection_reason
		FROM uploads
		WHERE upload_id = $1::uuid
		  AND state = 'rejected'`,
		uploadID,
	).Scan(&storedReason)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return storedReason == reason, err
}

func storageObjectMissing(err error) bool {
	var apiError interface{ ErrorCode() string }
	if errors.As(err, &apiError) {
		return apiError.ErrorCode() == "NoSuchKey" || apiError.ErrorCode() == "NotFound"
	}
	var responseError interface{ HTTPStatusCode() int }
	return errors.As(err, &responseError) && responseError.HTTPStatusCode() == 404
}

func classifyStorageError(err error, write bool) string {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		if write {
			return "ambiguous"
		}
		return "transient"
	}
	if errors.Is(err, errObjectLengthMismatch) {
		return "transient"
	}
	var networkError net.Error
	if errors.As(err, &networkError) {
		if write {
			return "ambiguous"
		}
		return "transient"
	}
	var apiError interface{ ErrorCode() string }
	if errors.As(err, &apiError) {
		switch apiError.ErrorCode() {
		case "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch", "ExpiredToken":
			return "auth"
		case "NoSuchBucket", "InvalidBucketName", "AuthorizationHeaderMalformed":
			return "configuration"
		case "InternalError", "RequestTimeout", "ServiceUnavailable", "SlowDown", "Throttling":
			return "transient"
		}
	}
	var responseError interface{ HTTPStatusCode() int }
	if errors.As(err, &responseError) {
		status := responseError.HTTPStatusCode()
		switch {
		case status == 401 || status == 403:
			return "auth"
		case status == 408 || status == 425 || status == 429 || status >= 500:
			return "transient"
		}
	}
	if write {
		return "ambiguous"
	}
	return "other_deterministic"
}

func classifyDatabaseError(err error, mutation bool) string {
	if errors.Is(err, errCandidateRegistrationOutcomeUnknown) {
		return "ambiguous"
	}
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) {
		switch postgresError.Code {
		case "40003":
			return "ambiguous"
		case "40001", "40P01", "55P03", "57014", "57P01", "53300":
			return "transient"
		case "28000", "28P01":
			return "auth"
		case "3D000", "42P01", "42703":
			return "configuration"
		default:
			return "other_deterministic"
		}
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		if mutation {
			return "ambiguous"
		}
		return "transient"
	}
	var networkError net.Error
	if errors.As(err, &networkError) {
		if mutation {
			return "ambiguous"
		}
		return "transient"
	}
	if mutation {
		return "ambiguous"
	}
	return "other_deterministic"
}
