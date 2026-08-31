package main

import (
	"context"
	"errors"
	"expvar"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	expiryBatchSize     = 100
	expirySweepInterval = time.Minute
)

func runExpiry(ctx context.Context, logger *slog.Logger, pool *pgxpool.Pool) {
	runExpiryLoop(ctx, func(ctx context.Context) {
		sweepPendingExpiries(ctx, logger, pool)
	})
}

func runExpiryLoop(ctx context.Context, sweep func(context.Context)) {
	for ctx.Err() == nil {
		sweep(ctx)
		if ctx.Err() != nil {
			return
		}
		timer := time.NewTimer(expirySweepInterval)
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

func sweepPendingExpiries(ctx context.Context, logger *slog.Logger, pool *pgxpool.Pool) (int, int) {
	started := time.Now()
	processed := 0
	errorCount := 0
	for processed < expiryBatchSize {
		uploadID, expired, err := expireNextPendingUpload(ctx, pool)
		if err != nil {
			errorCount++
			break
		}
		if !expired {
			break
		}
		processed++
		serviceMetrics.Get("upload_transitions_total").(*expvar.Map).
			Get("pending_to_expired").(*expvar.Int).Add(1)
		logger.Info(
			"upload.transition",
			"upload_id", uploadID,
			"state_from", "pending",
			"state_to", "expired",
			"trigger", "expiry",
			"reason_code", "upload_deadline_elapsed",
		)
	}
	logger.Info(
		"maintenance.sweep_finished",
		"phase", "expiry",
		"processed", processed,
		"errors", errorCount,
		"batch_full", processed == expiryBatchSize,
		"duration_ms", time.Since(started).Milliseconds(),
	)
	return processed, errorCount
}

func expireNextPendingUpload(
	ctx context.Context,
	pool *pgxpool.Pool,
) (string, bool, error) {
	tx, databaseContext, cancelDatabase, err := beginUploadTransaction(ctx, pool)
	if err != nil {
		return "", false, err
	}
	defer func() {
		_ = tx.Rollback(databaseContext)
		cancelDatabase()
	}()

	locked, err := scanStoredUpload(tx.QueryRow(databaseContext, `
		SELECT `+uploadRecordColumns+`, created_at
		FROM uploads
		WHERE state = 'pending'
		  AND upload_deadline <= clock_timestamp()
		ORDER BY upload_deadline, upload_id
		FOR UPDATE SKIP LOCKED
		LIMIT 1`))
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if err := tx.QueryRow(databaseContext, "SELECT clock_timestamp()").Scan(&locked.DatabaseNow); err != nil {
		return "", false, err
	}
	locked.DatabaseNow = locked.DatabaseNow.UTC()
	applied, err := transitionPendingExpiredLocked(databaseContext, tx, locked)
	if err != nil || !applied {
		return "", false, err
	}
	if err := tx.Commit(databaseContext); err != nil {
		cancelDatabase()
		confirmed, confirmErr := confirmPendingExpired(ctx, pool, locked.Upload.UploadID, locked.MaxWriteExpiresAt)
		if confirmErr != nil {
			return "", false, confirmErr
		}
		if !confirmed {
			return "", false, err
		}
	}
	cancelDatabase()
	return locked.Upload.UploadID, true, nil
}

func transitionPendingExpiredLocked(
	ctx context.Context,
	tx pgx.Tx,
	locked storedUpload,
) (bool, error) {
	if locked.Upload.State != "pending" || locked.DatabaseNow.Before(locked.Upload.UploadDeadline) {
		return false, nil
	}
	result, err := tx.Exec(ctx, `
		UPDATE uploads
		SET state = 'expired',
		    terminal_at = $2::timestamptz,
		    expiry_reason = 'upload_deadline_elapsed'
		WHERE upload_id = $1::uuid
		  AND state = 'pending'
		  AND upload_deadline <= $2::timestamptz`,
		locked.Upload.UploadID,
		locked.DatabaseNow,
	)
	if err != nil || result.RowsAffected() != 1 {
		return false, err
	}
	if err := ensureTombstones(ctx, tx, locked.Upload.UploadID, []tombstoneTarget{{
		ObjectKey:       locked.StagingKey,
		DeleteNotBefore: locked.MaxWriteExpiresAt,
	}}); err != nil {
		return false, err
	}
	return true, nil
}

func confirmPendingExpired(
	ctx context.Context,
	pool *pgxpool.Pool,
	uploadID string,
	expectedHorizon time.Time,
) (bool, error) {
	databaseContext, cancelDatabase := context.WithTimeout(ctx, uploadDatabaseTimeout)
	defer cancelDatabase()
	var confirmed bool
	err := pool.QueryRow(databaseContext, `
		SELECT
			state = 'expired'
			AND expiry_reason = 'upload_deadline_elapsed'
			AND completion_requested_at IS NULL
			AND max_write_expires_at = $2::timestamptz
			AND EXISTS (
				SELECT 1
				FROM cleanup_tombstones
				WHERE upload_id = uploads.upload_id
				  AND object_key = uploads.staging_key
				  AND candidate_sha256 IS NULL
				  AND delete_not_before = $2::timestamptz
			)
		FROM uploads
		WHERE upload_id = $1::uuid`,
		uploadID,
		expectedHorizon,
	).Scan(&confirmed)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return confirmed, err
}

func prepareMissingObservation(
	ctx context.Context,
	pool *pgxpool.Pool,
	claim finalizeClaim,
) (time.Time, bool, bool, error) {
	tx, databaseContext, cancelDatabase, err := beginUploadTransaction(ctx, pool)
	if err != nil {
		return time.Time{}, false, false, err
	}
	defer func() {
		_ = tx.Rollback(databaseContext)
		cancelDatabase()
	}()
	parent, active, err := lockFinalizeParent(databaseContext, tx, claim)
	if err != nil || !active {
		return time.Time{}, false, active, err
	}
	due := !parent.DatabaseNow.Before(parent.UploadDeadline) &&
		!parent.DatabaseNow.Before(parent.MaxWriteExpiresAt)
	return parent.MaxWriteExpiresAt, due, true, nil
}

func transitionFinalizeExpiredMissing(
	ctx context.Context,
	pool *pgxpool.Pool,
	claim finalizeClaim,
	observedHorizon time.Time,
	observedCandidates []trackedCandidate,
) (bool, error) {
	tx, databaseContext, cancelDatabase, err := beginUploadTransaction(ctx, pool)
	if err != nil {
		return false, err
	}
	defer func() {
		_ = tx.Rollback(databaseContext)
		cancelDatabase()
	}()
	parent, active, err := lockFinalizeParent(databaseContext, tx, claim)
	if err != nil || !active {
		return false, err
	}
	if !parent.MaxWriteExpiresAt.Equal(observedHorizon) ||
		parent.DatabaseNow.Before(parent.UploadDeadline) ||
		parent.DatabaseNow.Before(parent.MaxWriteExpiresAt) {
		return false, nil
	}
	candidates, err := lockAllCandidates(databaseContext, tx, claim.UploadID)
	if err != nil {
		return false, err
	}
	if !sameCandidateList(candidates, observedCandidates) {
		return false, nil
	}
	if err := ensureTombstones(
		databaseContext,
		tx,
		claim.UploadID,
		terminalTombstones(parent, candidates, ""),
	); err != nil {
		return false, err
	}
	result, err := tx.Exec(databaseContext, `
		UPDATE uploads
		SET state = 'expired',
		    reconcile_after = NULL,
		    claim_token = NULL,
		    claim_expires_at = NULL,
		    terminal_at = $3::timestamptz,
		    expiry_reason = 'staging_missing_after_write_window'
		WHERE upload_id = $1::uuid
		  AND state = 'finalizing'
		  AND claim_token = $2::uuid
		  AND claim_expires_at > $3::timestamptz
		  AND max_write_expires_at = $4::timestamptz`,
		claim.UploadID,
		claim.Token,
		parent.DatabaseNow,
		observedHorizon,
	)
	if err != nil || result.RowsAffected() != 1 {
		return false, err
	}
	if err := tx.Commit(databaseContext); err != nil {
		cancelDatabase()
		return confirmFinalizeExpiredMissing(
			ctx,
			pool,
			claim.UploadID,
			observedHorizon,
			len(observedCandidates),
		)
	}
	cancelDatabase()
	return true, nil
}

func sameCandidateList(left, right []trackedCandidate) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !sameCandidate(left[index], right[index]) {
			return false
		}
	}
	return true
}

func confirmFinalizeExpiredMissing(
	ctx context.Context,
	pool *pgxpool.Pool,
	uploadID string,
	expectedHorizon time.Time,
	expectedCandidates int,
) (bool, error) {
	databaseContext, cancelDatabase := context.WithTimeout(ctx, uploadDatabaseTimeout)
	defer cancelDatabase()
	var confirmed bool
	err := pool.QueryRow(databaseContext, `
		SELECT
			state = 'expired'
			AND expiry_reason = 'staging_missing_after_write_window'
			AND completion_requested_at IS NOT NULL
			AND max_write_expires_at = $2::timestamptz
			AND terminal_at >= GREATEST(upload_deadline, max_write_expires_at)
			AND (SELECT count(*) FROM upload_candidates WHERE upload_id = uploads.upload_id) = $3::integer
			AND EXISTS (
				SELECT 1
				FROM cleanup_tombstones
				WHERE upload_id = uploads.upload_id
				  AND object_key = uploads.staging_key
				  AND candidate_sha256 IS NULL
				  AND delete_not_before = $2::timestamptz
			)
			AND NOT EXISTS (
				SELECT 1
				FROM upload_candidates candidate
				WHERE candidate.upload_id = uploads.upload_id
				  AND NOT EXISTS (
					SELECT 1
					FROM cleanup_tombstones tombstone
					WHERE tombstone.object_key = candidate.object_key
					  AND tombstone.upload_id = candidate.upload_id
					  AND tombstone.candidate_sha256 = candidate.sha256
				)
			)
		FROM uploads
		WHERE upload_id = $1::uuid`,
		uploadID,
		expectedHorizon,
		expectedCandidates,
	).Scan(&confirmed)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return confirmed, err
}

func (f *finalizer) expireMissing(
	ctx context.Context,
	claim finalizeClaim,
	observedHorizon time.Time,
	observedCandidates []trackedCandidate,
) error {
	finish := f.phase(claim, "terminal_commit")
	committed, err := transitionFinalizeExpiredMissing(
		ctx,
		f.pool,
		claim,
		observedHorizon,
		observedCandidates,
	)
	if err != nil {
		finish("retry")
		return f.retry(ctx, claim, "terminal_commit", classifyDatabaseError(err, true))
	}
	if !committed {
		finish("stale")
		return f.retry(ctx, claim, "staging_observe", "transient")
	}
	finish("expired")
	serviceMetrics.Get("upload_transitions_total").(*expvar.Map).
		Get("finalizing_to_expired").(*expvar.Int).Add(1)
	f.logger.Info(
		"upload.transition",
		"upload_id", claim.UploadID,
		"state_from", "finalizing",
		"state_to", "expired",
		"trigger", "finalizer",
		"reason_code", "staging_missing_after_write_window",
	)
	return nil
}
