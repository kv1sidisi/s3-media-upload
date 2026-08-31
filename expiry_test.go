package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"expvar"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func testPendingExpiryQueueAndBatch(t *testing.T) {
	if testing.Short() {
		t.Skip("requires migrated PostgreSQL")
	}
	cfg := integrationConfig(t)

	t.Run("locked oldest is skipped without changing business truth", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		pool := newSingleConnectionPool(t, ctx, cfg.DatabaseURL)
		defer pool.Close()
		holder := connectUploadTestPostgres(t, ctx, cfg.DatabaseURL)
		defer holder.Close(context.Background())

		firstID, firstKey := testUUID(t), testUUID(t)
		secondID, secondKey := testUUID(t), testUUID(t)
		seed, err := holder.Begin(ctx)
		if err != nil {
			t.Fatal("begin pending expiry seed")
		}
		defer seed.Rollback(context.Background())
		insertLatePendingUpload(t, ctx, seed, firstID, firstKey)
		insertLatePendingUpload(t, ctx, seed, secondID, secondKey)
		if err := seed.Commit(ctx); err != nil {
			t.Fatal("commit pending expiry seed")
		}
		defer cleanupUploadByIdempotencyKey(t, pool, firstKey)
		defer cleanupUploadByIdempotencyKey(t, pool, secondKey)

		locked, err := holder.Begin(ctx)
		if err != nil {
			t.Fatal("begin pending expiry lock")
		}
		defer locked.Rollback(context.Background())
		var lockedID string
		if err := locked.QueryRow(ctx, `
			SELECT upload_id::text
			FROM uploads
			WHERE upload_id IN ($1::uuid, $2::uuid)
			ORDER BY upload_deadline, upload_id
			FOR UPDATE
			LIMIT 1`, firstID, secondID).Scan(&lockedID); err != nil {
			t.Fatal("lock oldest pending upload")
		}
		otherID := firstID
		if lockedID == firstID {
			otherID = secondID
		}

		uploadID, expired, err := expireNextPendingUpload(ctx, pool)
		if err != nil || !expired || uploadID != otherID {
			t.Fatalf("skipped expiry upload=%q expired=%t error=%v", uploadID, expired, err)
		}
		var lockedState, otherState string
		if err := locked.QueryRow(ctx, `SELECT state FROM uploads WHERE upload_id = $1::uuid`, lockedID).Scan(&lockedState); err != nil {
			t.Fatal("read locked pending upload")
		}
		if err := locked.QueryRow(ctx, `SELECT state FROM uploads WHERE upload_id = $1::uuid`, otherID).Scan(&otherState); err != nil {
			t.Fatal("read skipped expiry winner")
		}
		if lockedState != "pending" || otherState != "expired" {
			t.Fatalf("locked state=%q skipped winner state=%q", lockedState, otherState)
		}
	})

	t.Run("one pass expires at most one hundred oldest due rows", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		pool, err := openPostgres(ctx, cfg.DatabaseURL)
		if err != nil {
			t.Fatal("open migrated PostgreSQL")
		}
		defer pool.Close()

		seed, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal("begin pending batch seed")
		}
		defer seed.Rollback(context.Background())
		uploadIDs := make([]string, 0, expiryBatchSize+1)
		for range expiryBatchSize + 1 {
			uploadID, idempotencyKey := testUUID(t), testUUID(t)
			insertLatePendingUpload(t, ctx, seed, uploadID, idempotencyKey)
			uploadIDs = append(uploadIDs, uploadID)
		}
		if _, err := seed.Exec(ctx, `
			WITH ordered AS (
				SELECT upload_id::uuid, ordinal
				FROM unnest($1::text[]) WITH ORDINALITY AS seeded(upload_id, ordinal)
			), sampled AS MATERIALIZED (
				SELECT clock_timestamp() - interval '26 hours' AS base
			)
			UPDATE uploads
			SET created_at = sampled.base + ordered.ordinal * interval '1 second',
			    upload_deadline = sampled.base + ordered.ordinal * interval '1 second' + interval '24 hours',
			    max_write_expires_at = sampled.base + ordered.ordinal * interval '1 second' + interval '15 minutes'
			FROM ordered, sampled
			WHERE uploads.upload_id = ordered.upload_id`, uploadIDs); err != nil {
			t.Fatal("order pending expiry batch")
		}
		if err := seed.Commit(ctx); err != nil {
			t.Fatal("commit pending batch seed")
		}
		defer cleanupPendingExpiryBatch(t, pool, uploadIDs)

		var logs bytes.Buffer
		counter := serviceMetrics.Get("upload_transitions_total").(*expvar.Map).Get("pending_to_expired").(*expvar.Int)
		before := counter.Value()
		processed, errors := sweepPendingExpiries(
			ctx,
			slog.New(slog.NewJSONHandler(&logs, nil)),
			pool,
		)
		if processed != expiryBatchSize || errors != 0 {
			t.Fatalf("expiry sweep processed=%d errors=%d", processed, errors)
		}
		if counter.Value()-before != expiryBatchSize ||
			strings.Count(logs.String(), `"msg":"upload.transition"`) != expiryBatchSize ||
			strings.Count(logs.String(), `"msg":"maintenance.sweep_finished"`) != 1 ||
			!strings.Contains(logs.String(), `"phase":"expiry","processed":100,"errors":0,"batch_full":true`) {
			t.Fatalf("expiry sweep counter delta=%d logs=%s", counter.Value()-before, logs.String())
		}
		var expired, pending, exactTombstones int
		var remainingID string
		if err := pool.QueryRow(ctx, `
			SELECT
				count(*) FILTER (WHERE state = 'expired'),
				count(*) FILTER (WHERE state = 'pending'),
				count(*) FILTER (
					WHERE state = 'expired'
					  AND EXISTS (
						SELECT 1
						FROM cleanup_tombstones
						WHERE upload_id = uploads.upload_id
						  AND object_key = uploads.staging_key
						  AND candidate_sha256 IS NULL
						  AND delete_not_before = uploads.max_write_expires_at
						  AND next_attempt_at = uploads.max_write_expires_at
					)
				),
				COALESCE(min(upload_id::text) FILTER (WHERE state = 'pending'), '')
			FROM uploads
			WHERE upload_id::text = ANY($1::text[])`,
			uploadIDs,
		).Scan(&expired, &pending, &exactTombstones, &remainingID); err != nil {
			t.Fatal("read pending expiry batch")
		}
		if expired != expiryBatchSize || pending != 1 || exactTombstones != expiryBatchSize ||
			remainingID != uploadIDs[len(uploadIDs)-1] {
			t.Fatalf("expired=%d pending=%d exact_tombstones=%d newest_remaining=%t", expired, pending, exactTombstones, remainingID == uploadIDs[len(uploadIDs)-1])
		}
	})
}

func TestFinalizingMissingExpiry(t *testing.T) {
	if testing.Short() {
		t.Skip("requires migrated PostgreSQL and Garage")
	}
	cfg := integrationConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool, err := openPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		t.Fatal("open migrated PostgreSQL")
	}
	defer pool.Close()
	storage, err := newS3Client(ctx, cfg)
	if err != nil {
		t.Fatal("create Garage client")
	}
	worker := &finalizer{
		logger:     slog.New(slog.NewJSONHandler(io.Discard, nil)),
		pool:       pool,
		storage:    storage,
		bucket:     cfg.S3Bucket,
		claimLease: cfg.FinalizeClaimLease,
	}

	t.Run("missing before either horizon stays retriable", func(t *testing.T) {
		for _, test := range []struct {
			name            string
			deadlineReached bool
		}{
			{"write horizon reached first", false},
			{"upload deadline reached first", true},
		} {
			t.Run(test.name, func(t *testing.T) {
				uploadID, idempotencyKey := seedFinalizingUpload(t, ctx, pool, 1, "image/png")
				defer cleanupUploadByIdempotencyKey(t, pool, idempotencyKey)
				makeFinalizingBetweenHorizons(t, ctx, pool, uploadID, test.deadlineReached)
				claim, ok, err := claimNextFinalizing(ctx, pool, cfg.FinalizeClaimLease)
				if err != nil || !ok || claim.UploadID != uploadID {
					t.Fatalf("before-horizon claim=%#v ok=%t error=%v", claim, ok, err)
				}
				observedHorizon, due, active, err := prepareMissingObservation(ctx, pool, claim)
				if err != nil || !active || due {
					t.Fatalf("before-horizon observation due=%t active=%t error=%v", due, active, err)
				}
				committed, err := transitionFinalizeExpiredMissing(ctx, pool, claim, observedHorizon, nil)
				if err != nil || committed {
					t.Fatalf("before-horizon expiry committed=%t error=%v", committed, err)
				}
				expireFinalizeClaim(t, ctx, pool, claim)

				processed, err := worker.processNext(ctx)
				if err != nil || !processed {
					t.Fatalf("before-horizon processed=%t error=%v", processed, err)
				}
				var state string
				var streak, tombstones int
				var terminalAbsent, claimReleased bool
				if err := pool.QueryRow(ctx, `
					SELECT
						state,
						reconcile_failure_streak,
						terminal_at IS NULL AND expiry_reason IS NULL,
						claim_token IS NULL AND claim_expires_at IS NULL,
						(SELECT count(*) FROM cleanup_tombstones WHERE upload_id = uploads.upload_id)
					FROM uploads
					WHERE upload_id = $1::uuid`, uploadID).Scan(
					&state,
					&streak,
					&terminalAbsent,
					&claimReleased,
					&tombstones,
				); err != nil {
					t.Fatal("read before-horizon missing outcome")
				}
				if state != "finalizing" || streak != 1 || !terminalAbsent || !claimReleased || tombstones != 0 {
					t.Fatalf("state=%q streak=%d terminal_absent=%t claim_released=%t tombstones=%d", state, streak, terminalAbsent, claimReleased, tombstones)
				}
			})
		}
	})

	t.Run("missing after both horizons expires staging and tracked candidates", func(t *testing.T) {
		uploadID, idempotencyKey := seedFinalizingUpload(t, ctx, pool, 1, "image/png")
		defer cleanupUploadByIdempotencyKey(t, pool, idempotencyKey)
		makeFinalizingPastHorizon(t, ctx, pool, uploadID)
		claim, ok, err := claimNextFinalizing(ctx, pool, cfg.FinalizeClaimLease)
		if err != nil || !ok || claim.UploadID != uploadID {
			t.Fatalf("missing setup claim=%#v ok=%t error=%v", claim, ok, err)
		}
		candidate := missingTrackedCandidate(uploadID)
		active, mismatch, err := registerCandidate(ctx, pool, claim, candidate)
		if err != nil || !active || mismatch {
			t.Fatalf("missing candidate active=%t mismatch=%t error=%v", active, mismatch, err)
		}
		expireFinalizeClaim(t, ctx, pool, claim)

		var logs bytes.Buffer
		expiryWorker := *worker
		expiryWorker.logger = slog.New(slog.NewJSONHandler(&logs, nil))
		counter := serviceMetrics.Get("upload_transitions_total").(*expvar.Map).Get("finalizing_to_expired").(*expvar.Int)
		before := counter.Value()
		processed, err := expiryWorker.processNext(ctx)
		if err != nil || !processed {
			t.Fatalf("after-horizon processed=%t error=%v", processed, err)
		}
		if counter.Value()-before != 1 || strings.Count(logs.String(), `"msg":"upload.transition"`) != 1 ||
			!strings.Contains(logs.String(), `"reason_code":"staging_missing_after_write_window"`) {
			t.Fatalf("missing transition counter delta=%d logs=%s", counter.Value()-before, logs.String())
		}
		confirmed, err := confirmFinalizeExpiredMissing(ctx, pool, uploadID, claim.MaxWriteExpiresAt, 1)
		if err != nil || !confirmed {
			t.Fatalf("missing expiry confirmed=%t error=%v", confirmed, err)
		}
		var state, reason string
		var claimReleased, stagingTombstone, candidateTombstone bool
		if err := pool.QueryRow(ctx, `
			SELECT
				state,
				expiry_reason,
				claim_token IS NULL AND claim_expires_at IS NULL AND reconcile_after IS NULL,
				EXISTS (
					SELECT 1 FROM cleanup_tombstones
					WHERE object_key = uploads.staging_key
					  AND delete_not_before = uploads.max_write_expires_at
				),
				EXISTS (
					SELECT 1 FROM cleanup_tombstones
					WHERE object_key = $2::text
					  AND candidate_sha256 = $3::bytea
				)
			FROM uploads
			WHERE upload_id = $1::uuid`,
			uploadID,
			candidate.ObjectKey,
			candidate.SHA256[:],
		).Scan(&state, &reason, &claimReleased, &stagingTombstone, &candidateTombstone); err != nil {
			t.Fatal("read missing expiry")
		}
		if state != "expired" || reason != "staging_missing_after_write_window" ||
			!claimReleased || !stagingTombstone || !candidateTombstone {
			t.Fatalf("state=%q reason=%q claim_released=%t staging=%t candidate=%t", state, reason, claimReleased, stagingTombstone, candidateTombstone)
		}
	})

	t.Run("publishable tracked candidate wins after the horizons", func(t *testing.T) {
		raw := encodeValidationImage(t, "png")
		uploadID, idempotencyKey := seedFinalizingUpload(t, ctx, pool, int64(len(raw)), "image/png")
		defer cleanupUploadByIdempotencyKey(t, pool, idempotencyKey)
		makeFinalizingPastHorizon(t, ctx, pool, uploadID)
		claim, ok, err := claimNextFinalizing(ctx, pool, cfg.FinalizeClaimLease)
		if err != nil || !ok || claim.UploadID != uploadID {
			t.Fatalf("candidate setup claim=%#v ok=%t error=%v", claim, ok, err)
		}
		candidate := validatedTrackedCandidate(t, uploadID, raw)
		active, mismatch, err := registerCandidate(ctx, pool, claim, candidate)
		if err != nil || !active || mismatch {
			t.Fatalf("publishable candidate active=%t mismatch=%t error=%v", active, mismatch, err)
		}
		keys := []string{"staging/" + uploadID, candidate.ObjectKey}
		defer deleteFinalizerObjects(t, storage, cfg.S3Bucket, &keys)
		if err := putCandidateObject(ctx, storage, cfg.S3Bucket, candidate.ObjectKey, raw, "image/png"); err != nil {
			t.Fatal("put publishable candidate")
		}
		expireFinalizeClaim(t, ctx, pool, claim)

		processed, err := worker.processNext(ctx)
		if err != nil || !processed {
			t.Fatalf("candidate-first processed=%t error=%v", processed, err)
		}
		var state, finalKey string
		if err := pool.QueryRow(ctx, `
			SELECT state, final_key
			FROM uploads
			WHERE upload_id = $1::uuid`, uploadID).Scan(&state, &finalKey); err != nil {
			t.Fatal("read candidate-first winner")
		}
		if state != "ready" || finalKey != candidate.ObjectKey {
			t.Fatalf("candidate-first state=%q final_key_match=%t", state, finalKey == candidate.ObjectKey)
		}
	})
}

func testMissingExpiryObservationFencing(t *testing.T) {
	if testing.Short() {
		t.Skip("requires migrated PostgreSQL")
	}
	cfg := integrationConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := openPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		t.Fatal("open migrated PostgreSQL")
	}
	defer pool.Close()

	t.Run("changed write horizon invalidates an otherwise due observation", func(t *testing.T) {
		uploadID, idempotencyKey := seedFinalizingUpload(t, ctx, pool, 1, "image/png")
		defer cleanupUploadByIdempotencyKey(t, pool, idempotencyKey)
		makeFinalizingPastHorizon(t, ctx, pool, uploadID)
		claim, ok, err := claimNextFinalizing(ctx, pool, cfg.FinalizeClaimLease)
		if err != nil || !ok || claim.UploadID != uploadID {
			t.Fatalf("horizon setup claim=%#v ok=%t error=%v", claim, ok, err)
		}
		observedHorizon, due, active, err := prepareMissingObservation(ctx, pool, claim)
		if err != nil || !active || !due {
			t.Fatalf("horizon observation due=%t active=%t error=%v", due, active, err)
		}
		if _, err := pool.Exec(ctx, `
			UPDATE uploads
			SET max_write_expires_at = upload_deadline + interval '15 minutes'
			WHERE upload_id = $1::uuid`, uploadID); err != nil {
			t.Fatal("extend observed write horizon")
		}
		committed, err := transitionFinalizeExpiredMissing(ctx, pool, claim, observedHorizon, nil)
		if err != nil || committed {
			t.Fatalf("changed-horizon expiry committed=%t error=%v", committed, err)
		}
		assertFinalizingWithoutTombstones(t, ctx, pool, uploadID)
	})

	t.Run("new candidate invalidates an already started observation", func(t *testing.T) {
		uploadID, idempotencyKey := seedFinalizingUpload(t, ctx, pool, 1, "image/png")
		defer cleanupUploadByIdempotencyKey(t, pool, idempotencyKey)
		makeFinalizingPastHorizon(t, ctx, pool, uploadID)
		claim, ok, err := claimNextFinalizing(ctx, pool, cfg.FinalizeClaimLease)
		if err != nil || !ok || claim.UploadID != uploadID {
			t.Fatalf("candidate guard claim=%#v ok=%t error=%v", claim, ok, err)
		}
		observedHorizon, due, active, err := prepareMissingObservation(ctx, pool, claim)
		if err != nil || !active || !due {
			t.Fatalf("candidate observation due=%t active=%t error=%v", due, active, err)
		}
		candidate := missingTrackedCandidate(uploadID)
		active, mismatch, err := registerCandidate(ctx, pool, claim, candidate)
		if err != nil || !active || mismatch {
			t.Fatalf("late candidate active=%t mismatch=%t error=%v", active, mismatch, err)
		}
		committed, err := transitionFinalizeExpiredMissing(ctx, pool, claim, observedHorizon, nil)
		if err != nil || committed {
			t.Fatalf("unreconciled-candidate expiry committed=%t error=%v", committed, err)
		}
		assertFinalizingWithoutTombstones(t, ctx, pool, uploadID)
	})

	t.Run("stale claim cannot commit missing expiry", func(t *testing.T) {
		uploadID, idempotencyKey := seedFinalizingUpload(t, ctx, pool, 1, "image/png")
		defer cleanupUploadByIdempotencyKey(t, pool, idempotencyKey)
		makeFinalizingPastHorizon(t, ctx, pool, uploadID)
		claim, ok, err := claimNextFinalizing(ctx, pool, cfg.FinalizeClaimLease)
		if err != nil || !ok || claim.UploadID != uploadID {
			t.Fatalf("stale setup claim=%#v ok=%t error=%v", claim, ok, err)
		}
		observedHorizon, due, active, err := prepareMissingObservation(ctx, pool, claim)
		if err != nil || !active || !due {
			t.Fatalf("stale observation due=%t active=%t error=%v", due, active, err)
		}
		expireFinalizeClaim(t, ctx, pool, claim)
		committed, err := transitionFinalizeExpiredMissing(ctx, pool, claim, observedHorizon, nil)
		if err != nil || committed {
			t.Fatalf("stale expiry committed=%t error=%v", committed, err)
		}
		assertFinalizingWithoutTombstones(t, ctx, pool, uploadID)
	})
}

func testExpiryDurableBoundaryRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("requires migrated PostgreSQL")
	}
	cfg := integrationConfig(t)

	t.Run("pending expiry rollback and fresh confirmation", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		pool, err := openPostgres(ctx, cfg.DatabaseURL)
		if err != nil {
			t.Fatal("open migrated PostgreSQL")
		}
		defer pool.Close()
		uploadID, idempotencyKey := testUUID(t), testUUID(t)
		seed, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal("begin pending recovery seed")
		}
		defer seed.Rollback(context.Background())
		insertLatePendingUpload(t, ctx, seed, uploadID, idempotencyKey)
		if err := seed.Commit(ctx); err != nil {
			t.Fatal("commit pending recovery seed")
		}
		defer cleanupUploadByIdempotencyKey(t, pool, idempotencyKey)

		rolled, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal("begin discarded pending expiry")
		}
		locked, err := scanStoredUpload(rolled.QueryRow(ctx, `
			SELECT `+uploadRecordColumns+`, created_at
			FROM uploads
			WHERE upload_id = $1::uuid
			FOR UPDATE`, uploadID))
		if err != nil {
			_ = rolled.Rollback(context.Background())
			t.Fatal("lock discarded pending expiry")
		}
		if err := rolled.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&locked.DatabaseNow); err != nil {
			_ = rolled.Rollback(context.Background())
			t.Fatal("sample discarded pending expiry")
		}
		locked.DatabaseNow = locked.DatabaseNow.UTC()
		applied, err := transitionPendingExpiredLocked(ctx, rolled, locked)
		if err != nil || !applied {
			_ = rolled.Rollback(context.Background())
			t.Fatalf("discarded pending expiry applied=%t error=%v", applied, err)
		}
		if err := rolled.Rollback(ctx); err != nil {
			t.Fatal("rollback pending expiry")
		}
		confirmed, err := confirmPendingExpired(ctx, pool, uploadID, locked.MaxWriteExpiresAt)
		if err != nil || confirmed {
			t.Fatalf("rolled pending expiry confirmed=%t error=%v", confirmed, err)
		}

		freshID, expired, err := expireNextPendingUpload(ctx, pool)
		if err != nil || !expired || freshID != uploadID {
			t.Fatalf("fresh pending expiry id=%q expired=%t error=%v", freshID, expired, err)
		}
		freshPool := newSingleConnectionPool(t, ctx, cfg.DatabaseURL)
		defer freshPool.Close()
		confirmed, err = confirmPendingExpired(ctx, freshPool, uploadID, locked.MaxWriteExpiresAt)
		if err != nil || !confirmed {
			t.Fatalf("committed pending expiry confirmed=%t error=%v", confirmed, err)
		}
	})

	t.Run("missing expiry rollback and fresh confirmation", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		pool, err := openPostgres(ctx, cfg.DatabaseURL)
		if err != nil {
			t.Fatal("open migrated PostgreSQL")
		}
		defer pool.Close()
		uploadID, idempotencyKey := seedFinalizingUpload(t, ctx, pool, 1, "image/png")
		defer cleanupUploadByIdempotencyKey(t, pool, idempotencyKey)
		makeFinalizingPastHorizon(t, ctx, pool, uploadID)
		claim, ok, err := claimNextFinalizing(ctx, pool, cfg.FinalizeClaimLease)
		if err != nil || !ok || claim.UploadID != uploadID {
			t.Fatalf("missing recovery claim=%#v ok=%t error=%v", claim, ok, err)
		}
		observedHorizon, due, active, err := prepareMissingObservation(ctx, pool, claim)
		if err != nil || !active || !due {
			t.Fatalf("missing recovery observation due=%t active=%t error=%v", due, active, err)
		}

		rolled, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal("begin discarded missing expiry")
		}
		parent, active, err := lockFinalizeParent(ctx, rolled, claim)
		if err != nil || !active {
			_ = rolled.Rollback(context.Background())
			t.Fatalf("lock discarded missing expiry active=%t error=%v", active, err)
		}
		if err := ensureTombstones(ctx, rolled, uploadID, terminalTombstones(parent, nil, "")); err != nil {
			_ = rolled.Rollback(context.Background())
			t.Fatal("tombstone discarded missing expiry")
		}
		result, err := rolled.Exec(ctx, `
			UPDATE uploads
			SET state = 'expired',
			    reconcile_after = NULL,
			    claim_token = NULL,
			    claim_expires_at = NULL,
			    terminal_at = $3::timestamptz,
			    expiry_reason = 'staging_missing_after_write_window'
			WHERE upload_id = $1::uuid
			  AND claim_token = $2::uuid`, uploadID, claim.Token, parent.DatabaseNow)
		if err != nil || result.RowsAffected() != 1 {
			_ = rolled.Rollback(context.Background())
			t.Fatalf("apply discarded missing expiry rows=%d error=%v", result.RowsAffected(), err)
		}
		if err := rolled.Rollback(ctx); err != nil {
			t.Fatal("rollback missing expiry")
		}
		confirmed, err := confirmFinalizeExpiredMissing(ctx, pool, uploadID, observedHorizon, 0)
		if err != nil || confirmed {
			t.Fatalf("rolled missing expiry confirmed=%t error=%v", confirmed, err)
		}
		assertFinalizingWithoutTombstones(t, ctx, pool, uploadID)

		committed, err := transitionFinalizeExpiredMissing(ctx, pool, claim, observedHorizon, nil)
		if err != nil || !committed {
			t.Fatalf("fresh missing expiry committed=%t error=%v", committed, err)
		}
		freshPool := newSingleConnectionPool(t, ctx, cfg.DatabaseURL)
		defer freshPool.Close()
		confirmed, err = confirmFinalizeExpiredMissing(ctx, freshPool, uploadID, observedHorizon, 0)
		if err != nil || !confirmed {
			t.Fatalf("committed missing expiry confirmed=%t error=%v", confirmed, err)
		}
	})
}

func makeFinalizingBetweenHorizons(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	uploadID string,
	deadlineReached bool,
) {
	t.Helper()
	result, err := pool.Exec(ctx, `
		WITH sampled AS MATERIALIZED (
			SELECT
				CASE
					WHEN $2::boolean THEN clock_timestamp() - interval '24 hours 1 minute'
					ELSE clock_timestamp() - interval '1 hour'
				END AS created_at
		)
		UPDATE uploads
		SET created_at = sampled.created_at,
		    upload_deadline = sampled.created_at + interval '24 hours',
		    max_write_expires_at = CASE
		        WHEN $2::boolean THEN sampled.created_at + interval '24 hours 10 minutes'
		        ELSE sampled.created_at + interval '15 minutes'
		    END,
		    completion_requested_at = sampled.created_at + interval '30 minutes',
		    reconcile_after = clock_timestamp() - interval '1 second',
		    claim_token = NULL,
		    claim_expires_at = NULL
		FROM sampled
		WHERE upload_id = $1::uuid
		  AND state = 'finalizing'`, uploadID, deadlineReached)
	if err != nil || result.RowsAffected() != 1 {
		t.Fatalf("move finalizing upload between horizons: rows=%d error=%v", result.RowsAffected(), err)
	}
}

func makeFinalizingPastHorizon(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	uploadID string,
) {
	t.Helper()
	result, err := pool.Exec(ctx, `
		WITH sampled AS MATERIALIZED (
			SELECT clock_timestamp() - interval '25 hours' AS created_at
		)
		UPDATE uploads
		SET created_at = sampled.created_at,
		    upload_deadline = sampled.created_at + interval '24 hours',
		    max_write_expires_at = sampled.created_at + interval '15 minutes',
		    completion_requested_at = sampled.created_at + interval '23 hours',
		    reconcile_after = clock_timestamp() - interval '1 second',
		    claim_token = NULL,
		    claim_expires_at = NULL
		FROM sampled
		WHERE upload_id = $1::uuid
		  AND state = 'finalizing'`, uploadID)
	if err != nil || result.RowsAffected() != 1 {
		t.Fatalf("move finalizing upload past horizons: rows=%d error=%v", result.RowsAffected(), err)
	}
}

func missingTrackedCandidate(uploadID string) trackedCandidate {
	raw := []byte("missing")
	digest := sha256.Sum256(raw)
	return trackedCandidate{
		SHA256:                  digest,
		ObjectKey:               "media/" + uploadID + "/" + hex.EncodeToString(digest[:]),
		EncodedSize:             int64(len(raw)),
		ValidationPolicyVersion: validationPolicyVersion,
		Format:                  "png",
		Width:                   1,
		Height:                  1,
	}
}

func validatedTrackedCandidate(t *testing.T, uploadID string, raw []byte) trackedCandidate {
	t.Helper()
	validated, failure, err := validateImageBytes(raw, int64(len(raw)), int64(len(raw)), "image/png")
	if err != nil || failure != nil {
		t.Fatalf("validate tracked candidate failure=%#v error=%v", failure, err)
	}
	return trackedCandidate{
		SHA256:                  validated.SHA256,
		ObjectKey:               "media/" + uploadID + "/" + hex.EncodeToString(validated.SHA256[:]),
		EncodedSize:             validated.SizeBytes,
		ValidationPolicyVersion: validationPolicyVersion,
		Format:                  validated.Format,
		Width:                   validated.Width,
		Height:                  validated.Height,
	}
}

func expireFinalizeClaim(t *testing.T, ctx context.Context, pool *pgxpool.Pool, claim finalizeClaim) {
	t.Helper()
	result, err := pool.Exec(ctx, `
		UPDATE uploads
		SET claim_expires_at = clock_timestamp() - interval '1 second'
		WHERE upload_id = $1::uuid
		  AND claim_token = $2::uuid`, claim.UploadID, claim.Token)
	if err != nil || result.RowsAffected() != 1 {
		t.Fatalf("expire finalize claim: rows=%d error=%v", result.RowsAffected(), err)
	}
}

func assertFinalizingWithoutTombstones(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	uploadID string,
) {
	t.Helper()
	var state string
	var terminalAbsent bool
	var tombstones int
	if err := pool.QueryRow(ctx, `
		SELECT
			state,
			terminal_at IS NULL AND expiry_reason IS NULL,
			(SELECT count(*) FROM cleanup_tombstones WHERE upload_id = uploads.upload_id)
		FROM uploads
		WHERE upload_id = $1::uuid`, uploadID).Scan(&state, &terminalAbsent, &tombstones); err != nil {
		t.Fatal("read guarded missing outcome")
	}
	if state != "finalizing" || !terminalAbsent || tombstones != 0 {
		t.Fatalf("guarded state=%q terminal_absent=%t tombstones=%d", state, terminalAbsent, tombstones)
	}
}

func cleanupPendingExpiryBatch(t *testing.T, pool *pgxpool.Pool, uploadIDs []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Error("begin pending expiry cleanup")
		return
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, `
		DELETE FROM cleanup_tombstones
		WHERE upload_id::text = ANY($1::text[])`, uploadIDs); err != nil {
		t.Error("delete pending expiry tombstones")
		return
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM uploads
		WHERE upload_id::text = ANY($1::text[])`, uploadIDs); err != nil {
		t.Error("delete pending expiry uploads")
		return
	}
	if err := tx.Commit(ctx); err != nil {
		t.Error("commit pending expiry cleanup")
	}
}
