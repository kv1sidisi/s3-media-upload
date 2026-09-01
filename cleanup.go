package main

import (
	"context"
	"errors"
	"expvar"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	cleanupBatchSize          = 10
	cleanupReservationLease   = 30 * time.Second
	cleanupProtectiveInterval = 24 * time.Hour
)

type cleanupWorker struct {
	logger  *slog.Logger
	pool    *pgxpool.Pool
	storage *s3.Client
	bucket  string
}

type cleanupReservation struct {
	UploadID      string
	ObjectKey     string
	TargetKind    string
	Token         string
	ExpiresAt     time.Time
	FailureStreak int
}

type cleanupUpdate struct {
	FailureStreak int
	Delay         time.Duration
	LastAttemptAt time.Time
	NextAttemptAt time.Time
	Result        string
}

type cleanupSnapshot struct {
	Due              int64
	OldestDueAge     int64
	SnapshotUnixTime int64
}

func runMaintenance(
	ctx context.Context,
	logger *slog.Logger,
	pool *pgxpool.Pool,
	storage *s3.Client,
	bucket string,
) {
	worker := &cleanupWorker{logger: logger, pool: pool, storage: storage, bucket: bucket}
	runMaintenanceLoop(ctx, func(ctx context.Context) {
		sweepPendingExpiries(ctx, logger, pool)
		if ctx.Err() == nil {
			sweepCleanup(ctx, worker)
		}
	})
}

func sweepCleanup(ctx context.Context, worker *cleanupWorker) (int, int) {
	started := time.Now()
	processed := 0
	errorCount := 0
	for processed < cleanupBatchSize {
		attempted, failed, err := worker.processNext(ctx)
		if attempted {
			processed++
		}
		if failed {
			errorCount++
		}
		if err != nil {
			if !failed {
				errorCount++
			}
			break
		}
		if !attempted {
			break
		}
	}

	oldestDueAge := serviceMetrics.Get("cleanup_oldest_due_age_seconds").(*expvar.Int).Value()
	if snapshot, err := refreshCleanupGauges(ctx, worker.pool); err != nil {
		errorCount++
	} else {
		oldestDueAge = snapshot.OldestDueAge
	}
	worker.logger.Info(
		"maintenance.sweep_finished",
		"phase", "cleanup",
		"processed", processed,
		"errors", errorCount,
		"batch_full", processed == cleanupBatchSize,
		"duration_ms", time.Since(started).Milliseconds(),
		"oldest_due_age_seconds", oldestDueAge,
	)
	return processed, errorCount
}

func (worker *cleanupWorker) processNext(ctx context.Context) (bool, bool, error) {
	reservation, reserved, err := reserveNextCleanup(ctx, worker.pool)
	if err != nil || !reserved {
		return false, false, err
	}

	serviceMetrics.Get("cleanup_attempts_total").(*expvar.Int).Add(1)
	worker.logger.Info(
		"cleanup.delete",
		"upload_id", reservation.UploadID,
		"target_kind", reservation.TargetKind,
		"edge", "start",
		"failure_streak", reservation.FailureStreak,
	)

	deleteErr := deleteS3Object(ctx, worker.storage, worker.bucket, reservation.ObjectKey)
	result, logOutcome, errorClass := classifyCleanupDelete(ctx, deleteErr)
	update, applied, applyErr := applyCleanupResult(
		ctx,
		worker.pool,
		reservation,
		result,
		errorClass,
	)
	finishStreak := reservation.FailureStreak
	if applied {
		finishStreak = update.FailureStreak
	}
	attributes := []any{
		"upload_id", reservation.UploadID,
		"target_kind", reservation.TargetKind,
		"edge", "finish",
		"outcome", logOutcome,
		"failure_streak", finishStreak,
		"result_applied", applied,
	}
	if applied {
		attributes = append(attributes, "next_attempt_at", update.NextAttemptAt)
	}
	worker.logger.Info("cleanup.delete", attributes...)
	if applyErr != nil || !applied {
		return true, errorClass != "", applyErr
	}

	serviceMetrics.Get("cleanup_outcomes_total").(*expvar.Map).
		Get(result).(*expvar.Int).Add(1)
	if errorClass != "" {
		serviceMetrics.Get("retries_scheduled_total").(*expvar.Map).
			Get("cleanup_" + errorClass).(*expvar.Int).Add(1)
		worker.logger.Info(
			"retry.scheduled",
			"upload_id", reservation.UploadID,
			"retry_owner", "cleanup",
			"phase", "delete",
			"error_class", errorClass,
			"failure_streak", update.FailureStreak,
			"delay_ms", update.Delay.Milliseconds(),
			"next_attempt_at", update.NextAttemptAt,
		)
	}
	return true, errorClass != "", nil
}

func reserveNextCleanup(
	ctx context.Context,
	pool *pgxpool.Pool,
) (cleanupReservation, bool, error) {
	token, err := newUploadUUID()
	if err != nil {
		return cleanupReservation{}, false, err
	}
	tx, databaseContext, cancelDatabase, err := beginUploadTransaction(ctx, pool)
	if err != nil {
		return cleanupReservation{}, false, err
	}
	defer func() {
		_ = tx.Rollback(databaseContext)
		cancelDatabase()
	}()

	var reservation cleanupReservation
	var staging bool
	err = tx.QueryRow(databaseContext, `
		SELECT
			tombstone.upload_id::text,
			tombstone.object_key,
			tombstone.candidate_sha256 IS NULL,
			tombstone.failure_streak
		FROM cleanup_tombstones tombstone
		WHERE tombstone.delete_not_before <= clock_timestamp()
		  AND tombstone.next_attempt_at <= clock_timestamp()
		  AND (
			tombstone.reservation_token IS NULL
			OR tombstone.reservation_expires_at <= clock_timestamp()
		  )
		  AND NOT EXISTS (
			SELECT 1
			FROM uploads
			WHERE uploads.upload_id = tombstone.upload_id
			  AND uploads.final_key = tombstone.object_key
		  )
		ORDER BY tombstone.next_attempt_at, tombstone.object_key
		FOR UPDATE OF tombstone SKIP LOCKED
		LIMIT 1`).Scan(
		&reservation.UploadID,
		&reservation.ObjectKey,
		&staging,
		&reservation.FailureStreak,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return cleanupReservation{}, false, nil
	}
	if err != nil {
		return cleanupReservation{}, false, err
	}
	var databaseNow time.Time
	if err := tx.QueryRow(databaseContext, "SELECT clock_timestamp()").Scan(&databaseNow); err != nil {
		return cleanupReservation{}, false, err
	}
	reservation.Token = token
	reservation.ExpiresAt = databaseNow.Add(cleanupReservationLease)
	reservation.TargetKind = "candidate"
	if staging {
		reservation.TargetKind = "staging"
	}
	result, err := tx.Exec(databaseContext, `
		UPDATE cleanup_tombstones tombstone
		SET reservation_token = $2::uuid,
		    reservation_expires_at = $3::timestamptz
		WHERE tombstone.object_key = $1::text
		  AND tombstone.delete_not_before <= $4::timestamptz
		  AND tombstone.next_attempt_at <= $4::timestamptz
		  AND (
			tombstone.reservation_token IS NULL
			OR tombstone.reservation_expires_at <= $4::timestamptz
		  )
		  AND NOT EXISTS (
			SELECT 1
			FROM uploads
			WHERE uploads.upload_id = tombstone.upload_id
			  AND uploads.final_key = tombstone.object_key
		  )`,
		reservation.ObjectKey,
		reservation.Token,
		reservation.ExpiresAt,
		databaseNow,
	)
	if err != nil || result.RowsAffected() != 1 {
		return cleanupReservation{}, false, err
	}
	if err := tx.Commit(databaseContext); err != nil {
		cancelDatabase()
		return confirmCleanupReservation(ctx, pool, reservation)
	}
	cancelDatabase()
	reservation.ExpiresAt = reservation.ExpiresAt.UTC()
	return reservation, true, nil
}

func confirmCleanupReservation(
	ctx context.Context,
	pool *pgxpool.Pool,
	expected cleanupReservation,
) (cleanupReservation, bool, error) {
	databaseContext, cancelDatabase := context.WithTimeout(ctx, uploadDatabaseTimeout)
	defer cancelDatabase()
	var reservation cleanupReservation
	var staging bool
	err := pool.QueryRow(databaseContext, `
		SELECT
			upload_id::text,
			object_key,
			candidate_sha256 IS NULL,
			reservation_token::text,
			reservation_expires_at,
			failure_streak
		FROM cleanup_tombstones tombstone
		WHERE object_key = $1::text
		  AND reservation_token = $2::uuid
		  AND reservation_expires_at > clock_timestamp()
		  AND NOT EXISTS (
			SELECT 1
			FROM uploads
			WHERE uploads.upload_id = tombstone.upload_id
			  AND uploads.final_key = tombstone.object_key
		  )`,
		expected.ObjectKey,
		expected.Token,
	).Scan(
		&reservation.UploadID,
		&reservation.ObjectKey,
		&staging,
		&reservation.Token,
		&reservation.ExpiresAt,
		&reservation.FailureStreak,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return cleanupReservation{}, false, nil
	}
	if err != nil {
		return cleanupReservation{}, false, err
	}
	reservation.TargetKind = "candidate"
	if staging {
		reservation.TargetKind = "staging"
	}
	reservation.ExpiresAt = reservation.ExpiresAt.UTC()
	return reservation, true, nil
}

func deleteS3Object(ctx context.Context, client *s3.Client, bucket, key string) error {
	operationContext, cancel := context.WithTimeout(ctx, s3OperationTimeout)
	defer cancel()
	_, err := client.DeleteObject(operationContext, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	return err
}

func classifyCleanupDelete(ctx context.Context, err error) (string, string, string) {
	if err == nil {
		return "delete_succeeded", "deleted", ""
	}
	if storageObjectMissing(err) {
		return "confirmed_absent", "absent", ""
	}
	errorClass := classifyStorageError(err, true)
	logOutcome := errorClass + "_error"
	if errors.Is(err, context.Canceled) && ctx.Err() != nil {
		logOutcome = "canceled"
	}
	return errorClass + "_error", logOutcome, errorClass
}

func applyCleanupResult(
	ctx context.Context,
	pool *pgxpool.Pool,
	reservation cleanupReservation,
	result, errorClass string,
) (cleanupUpdate, bool, error) {
	tx, databaseContext, cancelDatabase, err := beginUploadTransaction(ctx, pool)
	if err != nil {
		return cleanupUpdate{}, false, err
	}
	defer func() {
		_ = tx.Rollback(databaseContext)
		cancelDatabase()
	}()

	var previousStreak int
	err = tx.QueryRow(databaseContext, `
		SELECT failure_streak
		FROM cleanup_tombstones
		WHERE object_key = $1::text
		  AND reservation_token = $2::uuid
		FOR UPDATE`,
		reservation.ObjectKey,
		reservation.Token,
	).Scan(&previousStreak)
	if errors.Is(err, pgx.ErrNoRows) {
		return cleanupUpdate{}, false, nil
	}
	if err != nil {
		return cleanupUpdate{}, false, err
	}
	var databaseNow time.Time
	if err := tx.QueryRow(databaseContext, "SELECT clock_timestamp()").Scan(&databaseNow); err != nil {
		return cleanupUpdate{}, false, err
	}
	update := cleanupUpdate{
		FailureStreak: 0,
		Delay:         cleanupProtectiveInterval,
		LastAttemptAt: databaseNow.UTC(),
		Result:        result,
	}
	if errorClass != "" {
		update.FailureStreak = previousStreak + 1
		update.Delay = cleanupRetryDelay(update.FailureStreak, errorClass)
	}
	update.NextAttemptAt = update.LastAttemptAt.Add(update.Delay)
	updateResult, err := tx.Exec(databaseContext, `
		UPDATE cleanup_tombstones
		SET next_attempt_at = $3::timestamptz,
		    reservation_token = NULL,
		    reservation_expires_at = NULL,
		    failure_streak = $4::integer,
		    last_attempt_at = $5::timestamptz,
		    last_result = $6::text
		WHERE object_key = $1::text
		  AND reservation_token = $2::uuid`,
		reservation.ObjectKey,
		reservation.Token,
		update.NextAttemptAt,
		update.FailureStreak,
		update.LastAttemptAt,
		update.Result,
	)
	if err != nil || updateResult.RowsAffected() != 1 {
		return cleanupUpdate{}, false, err
	}
	if err := tx.Commit(databaseContext); err != nil {
		cancelDatabase()
		confirmed, confirmErr := confirmCleanupResult(ctx, pool, reservation.ObjectKey, update)
		return update, confirmed, confirmErr
	}
	cancelDatabase()
	return update, true, nil
}

func cleanupRetryDelay(streak int, errorClass string) time.Duration {
	if errorClass == "auth" || errorClass == "configuration" || errorClass == "other_deterministic" {
		return time.Hour
	}
	if streak < 1 {
		streak = 1
	}
	delay := time.Minute << min(streak-1, 6)
	if delay > time.Hour {
		return time.Hour
	}
	return delay
}

func confirmCleanupResult(
	ctx context.Context,
	pool *pgxpool.Pool,
	objectKey string,
	expected cleanupUpdate,
) (bool, error) {
	databaseContext, cancelDatabase := context.WithTimeout(ctx, uploadDatabaseTimeout)
	defer cancelDatabase()
	var stored cleanupUpdate
	var reservationReleased bool
	err := pool.QueryRow(databaseContext, `
		SELECT
			failure_streak,
			last_attempt_at,
			next_attempt_at,
			last_result,
			reservation_token IS NULL AND reservation_expires_at IS NULL
		FROM cleanup_tombstones
		WHERE object_key = $1::text`,
		objectKey,
	).Scan(
		&stored.FailureStreak,
		&stored.LastAttemptAt,
		&stored.NextAttemptAt,
		&stored.Result,
		&reservationReleased,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return stored.FailureStreak == expected.FailureStreak &&
		stored.LastAttemptAt.Equal(expected.LastAttemptAt) &&
		stored.NextAttemptAt.Equal(expected.NextAttemptAt) &&
		stored.Result == expected.Result &&
		reservationReleased, nil
}

func refreshCleanupGauges(ctx context.Context, pool *pgxpool.Pool) (cleanupSnapshot, error) {
	databaseContext, cancelDatabase := context.WithTimeout(ctx, uploadDatabaseTimeout)
	defer cancelDatabase()
	var snapshot cleanupSnapshot
	err := pool.QueryRow(databaseContext, `
		SELECT
			count(*),
			COALESCE(
				FLOOR(EXTRACT(EPOCH FROM clock_timestamp() - min(next_attempt_at)))::bigint,
				0
			),
			FLOOR(EXTRACT(EPOCH FROM clock_timestamp()))::bigint
		FROM cleanup_tombstones tombstone
		WHERE delete_not_before <= clock_timestamp()
		  AND next_attempt_at <= clock_timestamp()
		  AND (
			reservation_token IS NULL
			OR reservation_expires_at <= clock_timestamp()
		  )
		  AND NOT EXISTS (
			SELECT 1
			FROM uploads
			WHERE uploads.upload_id = tombstone.upload_id
			  AND uploads.final_key = tombstone.object_key
		  )`).Scan(
		&snapshot.Due,
		&snapshot.OldestDueAge,
		&snapshot.SnapshotUnixTime,
	)
	if err != nil {
		return cleanupSnapshot{}, err
	}
	serviceMetrics.Get("cleanup_due").(*expvar.Int).Set(snapshot.Due)
	serviceMetrics.Get("cleanup_oldest_due_age_seconds").(*expvar.Int).Set(snapshot.OldestDueAge)
	serviceMetrics.Get("cleanup_snapshot_unix_seconds").(*expvar.Int).Set(snapshot.SnapshotUnixTime)
	return snapshot, nil
}
