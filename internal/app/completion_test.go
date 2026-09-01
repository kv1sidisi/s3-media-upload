package app

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

type completeCallOutcome struct {
	result completeUploadResult
	err    error
}

func TestParentFirstCompletionExpiryRace(t *testing.T) {
	if testing.Short() {
		t.Skip("requires migrated PostgreSQL")
	}
	cfg := integrationConfig(t)

	t.Run("concurrent completion has one lifecycle transition", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		firstPool := newSingleConnectionPool(t, ctx, cfg.DatabaseURL)
		defer firstPool.Close()
		secondPool := newSingleConnectionPool(t, ctx, cfg.DatabaseURL)
		defer secondPool.Close()
		holder := connectUploadTestPostgres(t, ctx, cfg.DatabaseURL)
		defer holder.Close(context.Background())
		observer := connectUploadTestPostgres(t, ctx, cfg.DatabaseURL)
		defer observer.Close(context.Background())

		uploadID, idempotencyKey := testUUID(t), testUUID(t)
		defer cleanupUploadByIdempotencyKey(t, firstPool, idempotencyKey)
		seed, err := holder.Begin(ctx)
		if err != nil {
			t.Fatal("begin completion seed transaction")
		}
		defer seed.Rollback(context.Background())
		if err := insertPendingUpload(ctx, seed, uploadID, idempotencyKey, 37, "image/png"); err != nil {
			t.Fatal("insert completion seed")
		}
		if err := seed.Commit(ctx); err != nil {
			t.Fatal("commit completion seed")
		}

		holderTx, err := holder.Begin(ctx)
		if err != nil {
			t.Fatal("begin completion race holder")
		}
		defer holderTx.Rollback(context.Background())
		if _, err := holderTx.Exec(ctx, `SELECT 1 FROM uploads WHERE upload_id = $1::uuid FOR UPDATE`, uploadID); err != nil {
			t.Fatal("lock completion parent")
		}

		firstPID := singlePoolPID(t, ctx, firstPool)
		secondPID := singlePoolPID(t, ctx, secondPool)
		firstResult := make(chan completeCallOutcome, 1)
		secondResult := make(chan completeCallOutcome, 1)
		go func() {
			result, err := completeUploadByID(ctx, firstPool, uploadID)
			firstResult <- completeCallOutcome{result: result, err: err}
		}()
		waitForPostgresBlock(t, ctx, observer, firstPID, holder.PgConn().PID())
		go func() {
			result, err := completeUploadByID(ctx, secondPool, uploadID)
			secondResult <- completeCallOutcome{result: result, err: err}
		}()
		waitForAnyPostgresBlock(t, ctx, observer, secondPID)
		if err := holderTx.Commit(ctx); err != nil {
			t.Fatal("release completion race holder")
		}

		outcomes := []completeCallOutcome{
			receiveCompleteOutcome(t, ctx, firstResult),
			receiveCompleteOutcome(t, ctx, secondResult),
		}
		transitions := 0
		for _, outcome := range outcomes {
			if outcome.err != nil || outcome.result.Upload.UploadID != uploadID || outcome.result.Upload.State != "finalizing" {
				t.Fatalf("concurrent completion outcome=%#v err=%v", outcome.result, outcome.err)
			}
			switch outcome.result.Transition {
			case "pending_to_finalizing":
				transitions++
			case "":
			default:
				t.Fatalf("unexpected concurrent transition %q", outcome.result.Transition)
			}
		}
		if transitions != 1 {
			t.Fatalf("concurrent completions reported %d lifecycle transitions", transitions)
		}

		var requestedBefore, dueBefore, deadline time.Time
		var due bool
		var tombstones int
		if err := observer.QueryRow(ctx, `
			SELECT completion_requested_at, reconcile_after, upload_deadline,
			       reconcile_after <= clock_timestamp(),
			       (SELECT count(*) FROM cleanup_tombstones WHERE upload_id = uploads.upload_id)
			FROM uploads
			WHERE upload_id = $1::uuid`, uploadID).Scan(
			&requestedBefore,
			&dueBefore,
			&deadline,
			&due,
			&tombstones,
		); err != nil {
			t.Fatal("read concurrent completion facts")
		}
		if !requestedBefore.Before(deadline) || !due || tombstones != 0 {
			t.Fatalf("invalid finalizing facts: requested=%s deadline=%s due=%t tombstones=%d", requestedBefore, deadline, due, tombstones)
		}

		replay, err := completeUploadByID(ctx, firstPool, uploadID)
		if err != nil || replay.Transition != "" || replay.Upload.State != "finalizing" {
			t.Fatalf("completion replay=%#v err=%v", replay, err)
		}
		var requestedAfter, dueAfter time.Time
		if err := observer.QueryRow(ctx, `
			SELECT completion_requested_at, reconcile_after
			FROM uploads
			WHERE upload_id = $1::uuid`, uploadID).Scan(&requestedAfter, &dueAfter); err != nil {
			t.Fatal("read replayed completion facts")
		}
		if !requestedAfter.Equal(requestedBefore) || dueAfter.After(dueBefore) {
			t.Fatalf("replay changed lifecycle facts: requested %s -> %s, due %s -> %s", requestedBefore, requestedAfter, dueBefore, dueAfter)
		}
	})

	for _, test := range []struct {
		name           string
		commitExpiry   bool
		wantTransition string
	}{
		{name: "committed expiry wins", commitExpiry: true},
		{name: "rolled back expiry lets completion win", wantTransition: "pending_to_expired"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			pool := newSingleConnectionPool(t, ctx, cfg.DatabaseURL)
			defer pool.Close()
			holder := connectUploadTestPostgres(t, ctx, cfg.DatabaseURL)
			defer holder.Close(context.Background())
			observer := connectUploadTestPostgres(t, ctx, cfg.DatabaseURL)
			defer observer.Close(context.Background())

			uploadID, idempotencyKey := testUUID(t), testUUID(t)
			defer cleanupUploadByIdempotencyKey(t, pool, idempotencyKey)
			seed, err := holder.Begin(ctx)
			if err != nil {
				t.Fatal("begin expired upload seed")
			}
			defer seed.Rollback(context.Background())
			insertLatePendingUpload(t, ctx, seed, uploadID, idempotencyKey)
			if err := seed.Commit(ctx); err != nil {
				t.Fatal("commit expired upload seed")
			}

			expiry, err := holder.Begin(ctx)
			if err != nil {
				t.Fatal("begin competing expiry")
			}
			defer expiry.Rollback(context.Background())
			expirePendingUpload(t, ctx, expiry, uploadID)

			workerPID := singlePoolPID(t, ctx, pool)
			result := make(chan completeCallOutcome, 1)
			go func() {
				completed, err := completeUploadByID(ctx, pool, uploadID)
				result <- completeCallOutcome{result: completed, err: err}
			}()
			waitForPostgresBlock(t, ctx, observer, workerPID, holder.PgConn().PID())
			if test.commitExpiry {
				if err := expiry.Commit(ctx); err != nil {
					t.Fatal("commit competing expiry")
				}
			} else if err := expiry.Rollback(ctx); err != nil {
				t.Fatal("rollback competing expiry")
			}

			outcome := receiveCompleteOutcome(t, ctx, result)
			if outcome.err != nil ||
				outcome.result.Upload.UploadID != uploadID ||
				outcome.result.Upload.State != "expired" ||
				outcome.result.Upload.Failure == nil ||
				outcome.result.Upload.Failure.Code != "upload_expired" ||
				outcome.result.Transition != test.wantTransition {
				t.Fatalf("completion/expiry outcome=%#v err=%v", outcome.result, outcome.err)
			}

			var state, reason string
			var completionAbsent, cutoffExact bool
			var tombstones int
			if err := observer.QueryRow(ctx, `
				SELECT state,
				       completion_requested_at IS NULL,
				       expiry_reason,
				       (SELECT count(*) FROM cleanup_tombstones WHERE upload_id = uploads.upload_id),
				       EXISTS (
				           SELECT 1
				           FROM cleanup_tombstones
				           WHERE upload_id = uploads.upload_id
				             AND object_key = uploads.staging_key
				             AND delete_not_before = uploads.max_write_expires_at
				             AND next_attempt_at = uploads.max_write_expires_at
				       )
				FROM uploads
				WHERE upload_id = $1::uuid`, uploadID).Scan(
				&state,
				&completionAbsent,
				&reason,
				&tombstones,
				&cutoffExact,
			); err != nil {
				t.Fatal("read completion/expiry winner")
			}
			if state != "expired" || !completionAbsent || reason != "upload_deadline_elapsed" || tombstones != 1 || !cutoffExact {
				t.Fatalf("invalid expiry winner: state=%q completion_absent=%t reason=%q tombstones=%d cutoff_exact=%t", state, completionAbsent, reason, tombstones, cutoffExact)
			}
			status, err := getUploadByID(ctx, pool, uploadID)
			if err != nil || status.State != "expired" || status.Failure == nil || status.Failure.Code != "upload_expired" {
				t.Fatalf("expired status=%#v err=%v", status, err)
			}
		})
	}
}

func TestCompletionDurableBoundaryRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("requires migrated PostgreSQL")
	}
	cfg := integrationConfig(t)

	t.Run("fresh actor recovers discarded committed result", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		holder := connectUploadTestPostgres(t, ctx, cfg.DatabaseURL)
		defer holder.Close(context.Background())
		uploadID, idempotencyKey := testUUID(t), testUUID(t)
		seed, err := holder.Begin(ctx)
		if err != nil {
			t.Fatal("begin recovery seed")
		}
		defer seed.Rollback(context.Background())
		if err := insertPendingUpload(ctx, seed, uploadID, idempotencyKey, 41, "image/jpeg"); err != nil {
			t.Fatal("insert recovery seed")
		}
		if err := seed.Commit(ctx); err != nil {
			t.Fatal("commit recovery seed")
		}

		firstPool := newSingleConnectionPool(t, ctx, cfg.DatabaseURL)
		if _, err := completeUploadByID(ctx, firstPool, uploadID); err != nil {
			firstPool.Close()
			t.Fatal("commit completion before discarding result")
		}
		firstPool.Close()

		freshPool := newSingleConnectionPool(t, ctx, cfg.DatabaseURL)
		defer freshPool.Close()
		defer cleanupUploadByIdempotencyKey(t, freshPool, idempotencyKey)
		fresh, err := recoverCompletionOutcome(ctx, freshPool, uploadID, completionRecovery{})
		if err != nil || fresh.Upload.UploadID != uploadID || fresh.Upload.State != "finalizing" || fresh.Transition != "" {
			t.Fatalf("fresh completion recovery=%#v err=%v", fresh, err)
		}

		var state string
		var requestedBeforeDeadline, due bool
		var tombstones int
		if err := freshPool.QueryRow(ctx, `
			SELECT state,
			       completion_requested_at < upload_deadline,
			       reconcile_after <= clock_timestamp(),
			       (SELECT count(*) FROM cleanup_tombstones WHERE upload_id = uploads.upload_id)
			FROM uploads
			WHERE upload_id = $1::uuid`, uploadID).Scan(
			&state,
			&requestedBeforeDeadline,
			&due,
			&tombstones,
		); err != nil {
			t.Fatal("read recovered completion")
		}
		if state != "finalizing" || !requestedBeforeDeadline || !due || tombstones != 0 {
			t.Fatalf("invalid recovered completion: state=%q requested_before_deadline=%t due=%t tombstones=%d", state, requestedBeforeDeadline, due, tombstones)
		}
	})

	t.Run("rollback permits one guarded retry", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		pool := newSingleConnectionPool(t, ctx, cfg.DatabaseURL)
		defer pool.Close()
		holder := connectUploadTestPostgres(t, ctx, cfg.DatabaseURL)
		defer holder.Close(context.Background())
		observer := connectUploadTestPostgres(t, ctx, cfg.DatabaseURL)
		defer observer.Close(context.Background())

		uploadID, idempotencyKey := testUUID(t), testUUID(t)
		defer cleanupUploadByIdempotencyKey(t, pool, idempotencyKey)
		seed, err := holder.Begin(ctx)
		if err != nil {
			t.Fatal("begin rollback seed")
		}
		defer seed.Rollback(context.Background())
		if err := insertPendingUpload(ctx, seed, uploadID, idempotencyKey, 43, "image/png"); err != nil {
			t.Fatal("insert rollback seed")
		}
		if err := seed.Commit(ctx); err != nil {
			t.Fatal("commit rollback seed")
		}

		attempt, err := holder.Begin(ctx)
		if err != nil {
			t.Fatal("begin discarded completion attempt")
		}
		defer attempt.Rollback(context.Background())
		var deadline, databaseNow time.Time
		if err := attempt.QueryRow(ctx, `
			SELECT upload_deadline
			FROM uploads
			WHERE upload_id = $1::uuid
			FOR UPDATE`, uploadID).Scan(&deadline); err != nil {
			t.Fatal("lock rollback completion parent")
		}
		if err := attempt.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&databaseNow); err != nil {
			t.Fatal("sample rollback completion time")
		}
		if !databaseNow.Before(deadline) {
			t.Fatal("rollback completion fixture unexpectedly reached its deadline")
		}
		result, err := attempt.Exec(ctx, `
			UPDATE uploads
			SET state = 'finalizing',
			    completion_requested_at = $2::timestamptz,
			    reconcile_after = $2::timestamptz
			WHERE upload_id = $1::uuid
			  AND state = 'pending'`, uploadID, databaseNow)
		if err != nil || result.RowsAffected() != 1 {
			t.Fatal("apply rollback completion attempt")
		}
		if err := attempt.Rollback(ctx); err != nil {
			t.Fatal("rollback discarded completion attempt")
		}

		var stillPending, lifecycleFactsAbsent bool
		if err := observer.QueryRow(ctx, `
			SELECT state = 'pending',
			       completion_requested_at IS NULL AND reconcile_after IS NULL
			FROM uploads
			WHERE upload_id = $1::uuid`, uploadID).Scan(&stillPending, &lifecycleFactsAbsent); err != nil {
			t.Fatal("read rolled back completion")
		}
		if !stillPending || !lifecycleFactsAbsent {
			t.Fatal("rolled back completion leaked lifecycle facts")
		}

		retry, err := recoverCompletionOutcome(ctx, pool, uploadID, completionRecovery{})
		if err != nil || retry.Upload.UploadID != uploadID || retry.Upload.State != "finalizing" || retry.Transition != "pending_to_finalizing" {
			t.Fatalf("guarded completion retry=%#v err=%v", retry, err)
		}
		var state string
		var requestedBeforeDeadline, due bool
		if err := observer.QueryRow(ctx, `
			SELECT state,
			       completion_requested_at < upload_deadline,
			       reconcile_after <= clock_timestamp()
			FROM uploads
			WHERE upload_id = $1::uuid`, uploadID).Scan(&state, &requestedBeforeDeadline, &due); err != nil {
			t.Fatal("read retried completion")
		}
		if state != "finalizing" || !requestedBeforeDeadline || !due {
			t.Fatalf("invalid retried completion: state=%q requested_before_deadline=%t due=%t", state, requestedBeforeDeadline, due)
		}
	})
}

func receiveCompleteOutcome(t *testing.T, ctx context.Context, result <-chan completeCallOutcome) completeCallOutcome {
	t.Helper()
	select {
	case outcome := <-result:
		return outcome
	case <-ctx.Done():
		t.Fatal("completion call did not finish")
		return completeCallOutcome{}
	}
}

func waitForAnyPostgresBlock(t *testing.T, ctx context.Context, observer *pgx.Conn, waiterPID uint32) {
	t.Helper()
	for ctx.Err() == nil {
		var blocked bool
		if err := observer.QueryRow(ctx, `SELECT cardinality(pg_blocking_pids($1::integer)) > 0`, int32(waiterPID)).Scan(&blocked); err != nil {
			t.Fatal("observe PostgreSQL waiter")
		}
		if blocked {
			return
		}
	}
	t.Fatal("completion waiter was not observed blocked")
}

func insertLatePendingUpload(t *testing.T, ctx context.Context, tx pgx.Tx, uploadID, idempotencyKey string) {
	t.Helper()
	if _, err := tx.Exec(ctx, `
		WITH sampled AS MATERIALIZED (
			SELECT clock_timestamp() - interval '25 hours' AS created_at
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
			'staging/' || $1::text,
			47,
			'image/png',
			'pending',
			created_at,
			created_at + interval '24 hours',
			created_at + interval '15 minutes'
		FROM sampled`, uploadID, idempotencyKey); err != nil {
		t.Fatal("insert late pending upload")
	}
}

func expirePendingUpload(t *testing.T, ctx context.Context, tx pgx.Tx, uploadID string) {
	t.Helper()
	var stagingKey string
	var deadline, horizon, databaseNow time.Time
	if err := tx.QueryRow(ctx, `
		SELECT staging_key, upload_deadline, max_write_expires_at
		FROM uploads
		WHERE upload_id = $1::uuid
		FOR UPDATE`, uploadID).Scan(&stagingKey, &deadline, &horizon); err != nil {
		t.Fatal("lock expiry parent")
	}
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&databaseNow); err != nil {
		t.Fatal("sample expiry time")
	}
	if databaseNow.Before(deadline) {
		t.Fatal("expiry fixture has not reached its deadline")
	}
	result, err := tx.Exec(ctx, `
		UPDATE uploads
		SET state = 'expired',
		    terminal_at = $2::timestamptz,
		    expiry_reason = 'upload_deadline_elapsed'
		WHERE upload_id = $1::uuid
		  AND state = 'pending'`, uploadID, databaseNow)
	if err != nil || result.RowsAffected() != 1 {
		t.Fatal("apply competing expiry")
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO cleanup_tombstones (
			object_key,
			upload_id,
			created_at,
			delete_not_before,
			next_attempt_at
		)
		VALUES ($1::text, $2::uuid, $3::timestamptz, $4::timestamptz, $4::timestamptz)`,
		stagingKey,
		uploadID,
		databaseNow,
		horizon,
	); err != nil {
		t.Fatal("insert competing expiry tombstone")
	}
}
