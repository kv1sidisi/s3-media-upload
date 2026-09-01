package main

import (
	"bytes"
	"context"
	"image"
	"image/color"
	"image/png"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgxpool"
)

type cleanupSuccessHTTPClient struct {
	hits atomic.Int64
}

func (client *cleanupSuccessHTTPClient) Do(request *http.Request) (*http.Response, error) {
	client.hits.Add(1)
	return &http.Response{
		StatusCode: http.StatusNoContent,
		Status:     "204 No Content",
		Header:     make(http.Header),
		Body:       http.NoBody,
		Request:    request,
	}, nil
}

type cleanupMissingHTTPClient struct {
	delegate    s3.HTTPClient
	escapedPath string
}

func (client *cleanupMissingHTTPClient) Do(request *http.Request) (*http.Response, error) {
	if request.Method == http.MethodDelete && request.URL.EscapedPath() == client.escapedPath {
		return nil, finalizerStorageError{code: "NoSuchKey", status: http.StatusNotFound}
	}
	return client.delegate.Do(request)
}

type cleanupGateHTTPClient struct {
	delegate    s3.HTTPClient
	method      string
	escapedPath string
	started     chan struct{}
	release     chan struct{}
	once        sync.Once
}

func (client *cleanupGateHTTPClient) Do(request *http.Request) (*http.Response, error) {
	if request.Method != client.method || request.URL.EscapedPath() != client.escapedPath {
		return client.delegate.Do(request)
	}
	client.once.Do(func() { close(client.started) })
	select {
	case <-client.release:
		return client.delegate.Do(request)
	case <-request.Context().Done():
		return nil, request.Context().Err()
	}
}

func testCleanupDueQueueAndTokenFencing(t *testing.T) {
	if testing.Short() {
		t.Skip("requires migrated PostgreSQL")
	}
	cfg := integrationConfig(t)

	t.Run("queue, DB time, fresh token, backoff and protective sweep", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		pool, err := openPostgres(ctx, cfg.DatabaseURL)
		if err != nil {
			t.Fatal("open migrated PostgreSQL")
		}
		defer pool.Close()
		holder := connectUploadTestPostgres(t, ctx, cfg.DatabaseURL)
		defer holder.Close(context.Background())

		firstID, firstKey, firstIdentity := seedDueCleanupTombstone(t, ctx, pool)
		secondID, secondKey, secondIdentity := seedDueCleanupTombstone(t, ctx, pool)
		defer cleanupUploadByIdempotencyKey(t, pool, firstIdentity)
		defer cleanupUploadByIdempotencyKey(t, pool, secondIdentity)
		if _, err := pool.Exec(ctx, `
			UPDATE cleanup_tombstones
			SET next_attempt_at = CASE object_key
				WHEN $1::text THEN clock_timestamp() - interval '2 hours'
				ELSE clock_timestamp() - interval '1 hour'
			END
			WHERE object_key IN ($1::text, $2::text)`, firstKey, secondKey); err != nil {
			t.Fatal("order cleanup queue")
		}

		locked, err := holder.Begin(ctx)
		if err != nil {
			t.Fatal("begin cleanup queue lock")
		}
		defer locked.Rollback(context.Background())
		if _, err := locked.Exec(ctx, `
			SELECT 1
			FROM cleanup_tombstones
			WHERE object_key = $1::text
			FOR UPDATE`, firstKey); err != nil {
			t.Fatal("lock oldest cleanup tombstone")
		}

		skipped, ok, err := reserveNextCleanup(ctx, pool)
		if err != nil || !ok || skipped.UploadID != secondID || skipped.ObjectKey != secondKey {
			t.Fatalf("skipped reservation=%#v ok=%t error=%v", skipped, ok, err)
		}
		if err := locked.Rollback(ctx); err != nil {
			t.Fatal("release oldest cleanup tombstone")
		}
		first, ok, err := reserveNextCleanup(ctx, pool)
		if err != nil || !ok || first.UploadID != firstID || first.ObjectKey != firstKey || first.Token == skipped.Token {
			t.Fatalf("first reservation=%#v ok=%t error=%v", first, ok, err)
		}
		if _, err := pool.Exec(ctx, `
			UPDATE cleanup_tombstones
			SET reservation_expires_at = clock_timestamp() - interval '1 second'
			WHERE object_key = $1::text
			  AND reservation_token = $2::uuid`, firstKey, first.Token); err != nil {
			t.Fatal("expire cleanup reservation")
		}
		reclaimed, ok, err := reserveNextCleanup(ctx, pool)
		if err != nil || !ok || reclaimed.ObjectKey != firstKey || reclaimed.Token == first.Token {
			t.Fatalf("reclaimed reservation=%#v ok=%t error=%v", reclaimed, ok, err)
		}
		if _, applied, err := applyCleanupResult(
			ctx,
			pool,
			first,
			"transient_error",
			"transient",
		); err != nil || applied {
			t.Fatalf("stale cleanup result applied=%t error=%v", applied, err)
		}
		update, applied, err := applyCleanupResult(
			ctx,
			pool,
			reclaimed,
			"transient_error",
			"transient",
		)
		if err != nil || !applied || update.FailureStreak != 1 || update.Delay != time.Minute ||
			!update.NextAttemptAt.Equal(update.LastAttemptAt.Add(time.Minute)) {
			t.Fatalf("transient update=%#v applied=%t error=%v", update, applied, err)
		}

		if _, err := pool.Exec(ctx, `
			UPDATE cleanup_tombstones
			SET next_attempt_at = clock_timestamp() - interval '1 second'
			WHERE object_key = $1::text`, firstKey); err != nil {
			t.Fatal("force protective cleanup retry due")
		}
		protective, ok, err := reserveNextCleanup(ctx, pool)
		if err != nil || !ok || protective.ObjectKey != firstKey {
			t.Fatalf("protective reservation=%#v ok=%t error=%v", protective, ok, err)
		}
		update, applied, err = applyCleanupResult(ctx, pool, protective, "delete_succeeded", "")
		if err != nil || !applied || update.FailureStreak != 0 || update.Delay != cleanupProtectiveInterval ||
			!update.NextAttemptAt.Equal(update.LastAttemptAt.Add(cleanupProtectiveInterval)) {
			t.Fatalf("protective update=%#v applied=%t error=%v", update, applied, err)
		}
		var permanent bool
		if err := pool.QueryRow(ctx, `
			SELECT
				last_result = 'delete_succeeded'
				AND failure_streak = 0
				AND next_attempt_at - last_attempt_at = interval '24 hours'
			FROM cleanup_tombstones
			WHERE object_key = $1::text`, firstKey).Scan(&permanent); err != nil || !permanent {
			t.Fatalf("permanent protective tombstone=%t error=%v", permanent, err)
		}

		_, futureKey, futureIdentity := seedDueCleanupTombstone(t, ctx, pool)
		defer cleanupUploadByIdempotencyKey(t, pool, futureIdentity)
		if _, err := pool.Exec(ctx, `
			UPDATE cleanup_tombstones
			SET next_attempt_at = clock_timestamp() + interval '1 hour'
			WHERE object_key = $1::text`, futureKey); err != nil {
			t.Fatal("move cleanup tombstone into the future")
		}
		if reservation, ok, err := reserveNextCleanup(ctx, pool); err != nil || ok {
			t.Fatalf("future cleanup reservation=%#v ok=%t error=%v", reservation, ok, err)
		}
		snapshot, err := refreshCleanupGauges(ctx, pool)
		if err != nil || snapshot.Due != 0 || snapshot.OldestDueAge != 0 || snapshot.SnapshotUnixTime <= 0 {
			t.Fatalf("cleanup snapshot=%#v error=%v", snapshot, err)
		}
	})

	t.Run("one pass processes ten tombstones", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		pool, err := openPostgres(ctx, cfg.DatabaseURL)
		if err != nil {
			t.Fatal("open migrated PostgreSQL")
		}
		defer pool.Close()
		identities := make([]string, 0, cleanupBatchSize+1)
		for range cleanupBatchSize + 1 {
			_, _, identity := seedDueCleanupTombstone(t, ctx, pool)
			identities = append(identities, identity)
		}
		defer func() {
			for _, identity := range identities {
				cleanupUploadByIdempotencyKey(t, pool, identity)
			}
		}()

		base, err := newS3Client(ctx, cfg)
		if err != nil {
			t.Fatal("create cleanup test S3 client")
		}
		transport := &cleanupSuccessHTTPClient{}
		options := base.Options()
		options.HTTPClient = transport
		options.Retryer = aws.NopRetryer{}
		var logs bytes.Buffer
		worker := &cleanupWorker{
			logger:  slog.New(slog.NewJSONHandler(&logs, nil)),
			pool:    pool,
			storage: s3.New(options),
			bucket:  cfg.S3Bucket,
		}
		processed, failures := sweepCleanup(ctx, worker)
		if processed != cleanupBatchSize || failures != 0 || transport.hits.Load() != cleanupBatchSize {
			t.Fatalf("cleanup processed=%d failures=%d deletes=%d", processed, failures, transport.hits.Load())
		}
		var due, completed int
		if err := pool.QueryRow(ctx, `
			SELECT
				count(*) FILTER (
					WHERE next_attempt_at <= clock_timestamp()
					  AND (reservation_token IS NULL OR reservation_expires_at <= clock_timestamp())
				),
				count(*) FILTER (WHERE last_result = 'delete_succeeded')
			FROM cleanup_tombstones
			WHERE upload_id IN (
				SELECT upload_id
				FROM uploads
				WHERE idempotency_key::text = ANY($1::text[])
			)`, identities).Scan(&due, &completed); err != nil {
			t.Fatal("read cleanup batch outcome")
		}
		if due != 1 || completed != cleanupBatchSize || !strings.Contains(logs.String(), `"batch_full":true`) {
			t.Fatalf("cleanup due=%d completed=%d batch log=%t", due, completed, strings.Contains(logs.String(), `"batch_full":true`))
		}
	})
}

func testCleanupDurableBoundaryRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("requires migrated PostgreSQL")
	}
	cfg := integrationConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := openPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		t.Fatal("open migrated PostgreSQL")
	}
	defer pool.Close()
	_, objectKey, identity := seedDueCleanupTombstone(t, ctx, pool)
	defer cleanupUploadByIdempotencyKey(t, pool, identity)

	rolledToken := testUUID(t)
	rolled, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal("begin discarded cleanup reservation")
	}
	result, err := rolled.Exec(ctx, `
		UPDATE cleanup_tombstones
		SET reservation_token = $2::uuid,
		    reservation_expires_at = clock_timestamp() + interval '30 seconds'
		WHERE object_key = $1::text`, objectKey, rolledToken)
	if err != nil || result.RowsAffected() != 1 {
		_ = rolled.Rollback(context.Background())
		t.Fatal("apply discarded cleanup reservation")
	}
	if err := rolled.Rollback(ctx); err != nil {
		t.Fatal("rollback discarded cleanup reservation")
	}
	if reservation, confirmed, err := confirmCleanupReservation(ctx, pool, cleanupReservation{
		ObjectKey: objectKey,
		Token:     rolledToken,
	}); err != nil || confirmed {
		t.Fatalf("discarded reservation=%#v confirmed=%t error=%v", reservation, confirmed, err)
	}

	reservation, ok, err := reserveNextCleanup(ctx, pool)
	if err != nil || !ok || reservation.ObjectKey != objectKey {
		t.Fatalf("cleanup reservation=%#v ok=%t error=%v", reservation, ok, err)
	}
	freshPool, err := openPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		t.Fatal("open fresh cleanup actor")
	}
	defer freshPool.Close()
	confirmedReservation, confirmed, err := confirmCleanupReservation(ctx, freshPool, reservation)
	if err != nil || !confirmed || confirmedReservation.Token != reservation.Token {
		t.Fatalf("confirmed reservation=%#v confirmed=%t error=%v", confirmedReservation, confirmed, err)
	}
	update, applied, err := applyCleanupResult(ctx, freshPool, reservation, "delete_succeeded", "")
	if err != nil || !applied {
		t.Fatalf("cleanup result update=%#v applied=%t error=%v", update, applied, err)
	}
	confirmed, err = confirmCleanupResult(ctx, pool, objectKey, update)
	if err != nil || !confirmed {
		t.Fatalf("discarded cleanup result confirmed=%t error=%v", confirmed, err)
	}
}

func seedDueCleanupTombstone(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
) (string, string, string) {
	t.Helper()
	uploadID, identity := testUUID(t), testUUID(t)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal("begin cleanup seed")
	}
	defer tx.Rollback(context.Background())
	insertLatePendingUpload(t, ctx, tx, uploadID, identity)
	expirePendingUpload(t, ctx, tx, uploadID)
	if err := tx.Commit(ctx); err != nil {
		t.Fatal("commit cleanup seed")
	}
	return uploadID, "staging/" + uploadID, identity
}

func forceCleanupDue(t *testing.T, ctx context.Context, pool *pgxpool.Pool, objectKey string) {
	t.Helper()
	result, err := pool.Exec(ctx, `
		UPDATE cleanup_tombstones
		SET next_attempt_at = GREATEST(delete_not_before, clock_timestamp()),
		    reservation_token = NULL,
		    reservation_expires_at = NULL
		WHERE object_key = $1::text`, objectKey)
	if err != nil || result.RowsAffected() != 1 {
		t.Fatalf("force cleanup due rows=%d error=%v", result.RowsAffected(), err)
	}
}

func cleanupS3Client(base *s3.Client, client s3.HTTPClient) *s3.Client {
	options := base.Options()
	options.HTTPClient = client
	options.Retryer = aws.NopRetryer{}
	return s3.New(options)
}

func cleanupObjectPath(bucket, key string) string {
	return (&url.URL{Path: "/" + bucket + "/" + key}).EscapedPath()
}

func cleanupPNG(t *testing.T, value byte) []byte {
	t.Helper()
	encoded := new(bytes.Buffer)
	picture := image.NewRGBA(image.Rect(0, 0, 1, 1))
	picture.Set(0, 0, color.RGBA{R: value, G: value ^ 0xff, B: value / 2, A: 0xff})
	if err := png.Encode(encoded, picture); err != nil {
		t.Fatal("encode cleanup image")
	}
	return encoded.Bytes()
}

func assertCleanupObjectMissing(
	t *testing.T,
	ctx context.Context,
	storage *s3.Client,
	bucket, key string,
) {
	t.Helper()
	if _, err := captureS3Object(ctx, storage, bucket, key, maxUploadSizeBytes); err == nil || !storageObjectMissing(err) {
		t.Fatalf("cleanup object %q absence error=%v", key, err)
	}
}

func TestE2ECleanupRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("requires migrated PostgreSQL and Garage")
	}
	cfg := integrationConfig(t)
	if cfg.S3Endpoint == "" {
		t.Fatal("explicit S3_ENDPOINT is required for cleanup recovery")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
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
	realHTTP := &http.Client{
		Timeout: 10 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	worker := func(client *s3.Client, logger *slog.Logger) *cleanupWorker {
		return &cleanupWorker{logger: logger, pool: pool, storage: client, bucket: cfg.S3Bucket}
	}
	discardLogger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	t.Run("reservation crash", func(t *testing.T) {
		uploadID, objectKey, identity := seedDueCleanupTombstone(t, ctx, pool)
		t.Cleanup(func() { cleanupUploadByIdempotencyKey(t, pool, identity) })
		keys := []string{objectKey}
		t.Cleanup(func() { deleteFinalizerObjects(t, storage, cfg.S3Bucket, &keys) })
		raw := cleanupPNG(t, 0x10)
		if err := putCandidateObject(ctx, storage, cfg.S3Bucket, objectKey, raw, "image/png"); err != nil {
			t.Fatal("put cleanup crash object")
		}
		reservation, ok, err := reserveNextCleanup(ctx, pool)
		if err != nil || !ok || reservation.UploadID != uploadID || reservation.ObjectKey != objectKey {
			t.Fatalf("abandoned reservation=%#v ok=%t error=%v", reservation, ok, err)
		}
		result, err := pool.Exec(ctx, `
			UPDATE cleanup_tombstones
			SET reservation_expires_at = clock_timestamp() - interval '1 second'
			WHERE object_key = $1::text
			  AND reservation_token = $2::uuid`, objectKey, reservation.Token)
		if err != nil || result.RowsAffected() != 1 {
			t.Fatalf("expire abandoned reservation rows=%d error=%v", result.RowsAffected(), err)
		}
		attempted, failed, err := worker(storage, discardLogger).processNext(ctx)
		if err != nil || !attempted || failed {
			t.Fatalf("recovered cleanup attempted=%t failed=%t error=%v", attempted, failed, err)
		}
		assertCleanupObjectMissing(t, ctx, storage, cfg.S3Bucket, objectKey)
		var durable bool
		if err := pool.QueryRow(ctx, `
			SELECT
				last_result = 'delete_succeeded'
				AND failure_streak = 0
				AND reservation_token IS NULL
				AND next_attempt_at - last_attempt_at = interval '24 hours'
			FROM cleanup_tombstones
			WHERE object_key = $1::text`, objectKey).Scan(&durable); err != nil || !durable {
			t.Fatalf("recovered cleanup durable=%t error=%v", durable, err)
		}
	})

	t.Run("delegate then fail", func(t *testing.T) {
		_, objectKey, identity := seedDueCleanupTombstone(t, ctx, pool)
		t.Cleanup(func() { cleanupUploadByIdempotencyKey(t, pool, identity) })
		keys := []string{objectKey}
		t.Cleanup(func() { deleteFinalizerObjects(t, storage, cfg.S3Bucket, &keys) })
		raw := cleanupPNG(t, 0x20)
		if err := putCandidateObject(ctx, storage, cfg.S3Bucket, objectKey, raw, "image/png"); err != nil {
			t.Fatal("put hidden-delete object")
		}
		fault := &recoveryS3HTTPClient{
			delegate:    realHTTP,
			method:      http.MethodDelete,
			escapedPath: cleanupObjectPath(cfg.S3Bucket, objectKey),
			action:      recoveryDelegateThenFail,
		}
		attempted, failed, err := worker(cleanupS3Client(storage, fault), discardLogger).processNext(ctx)
		if err != nil || !attempted || !failed || fault.hits != 1 || !fault.delegated {
			t.Fatalf(
				"hidden delete attempted=%t failed=%t hits=%d delegated=%t error=%v",
				attempted,
				failed,
				fault.hits,
				fault.delegated,
				err,
			)
		}
		assertCleanupObjectMissing(t, ctx, storage, cfg.S3Bucket, objectKey)
		var ambiguous bool
		if err := pool.QueryRow(ctx, `
			SELECT
				last_result = 'ambiguous_error'
				AND failure_streak = 1
				AND next_attempt_at - last_attempt_at = interval '1 minute'
			FROM cleanup_tombstones
			WHERE object_key = $1::text`, objectKey).Scan(&ambiguous); err != nil || !ambiguous {
			t.Fatalf("hidden delete durable ambiguity=%t error=%v", ambiguous, err)
		}
		forceCleanupDue(t, ctx, pool, objectKey)
		attempted, failed, err = worker(storage, discardLogger).processNext(ctx)
		if err != nil || !attempted || failed {
			t.Fatalf("hidden delete recovery attempted=%t failed=%t error=%v", attempted, failed, err)
		}
	})

	t.Run("late loser PUT", func(t *testing.T) {
		selectedRaw := cleanupPNG(t, 0x30)
		loserRaw := cleanupPNG(t, 0x40)
		uploadID, identity := seedFinalizingUpload(t, ctx, pool, int64(len(selectedRaw)), "image/png")
		t.Cleanup(func() { cleanupUploadByIdempotencyKey(t, pool, identity) })
		selected := validatedTrackedCandidate(t, uploadID, selectedRaw)
		loser := validatedTrackedCandidate(t, uploadID, loserRaw)
		keys := []string{"staging/" + uploadID, selected.ObjectKey, loser.ObjectKey}
		t.Cleanup(func() { deleteFinalizerObjects(t, storage, cfg.S3Bucket, &keys) })
		claim, ok, err := claimNextFinalizing(ctx, pool, cfg.FinalizeClaimLease)
		if err != nil || !ok || claim.UploadID != uploadID {
			t.Fatalf("late-write claim=%#v ok=%t error=%v", claim, ok, err)
		}
		for _, candidate := range []trackedCandidate{selected, loser} {
			active, mismatch, err := registerCandidate(ctx, pool, claim, candidate)
			if err != nil || !active || mismatch {
				t.Fatalf("register candidate active=%t mismatch=%t error=%v", active, mismatch, err)
			}
		}
		if err := putCandidateObject(ctx, storage, cfg.S3Bucket, selected.ObjectKey, selectedRaw, "image/png"); err != nil {
			t.Fatal("put selected cleanup candidate")
		}

		gate := &cleanupGateHTTPClient{
			delegate:    realHTTP,
			method:      http.MethodPut,
			escapedPath: cleanupObjectPath(cfg.S3Bucket, loser.ObjectKey),
			started:     make(chan struct{}),
			release:     make(chan struct{}),
		}
		defer func() {
			select {
			case <-gate.release:
			default:
				close(gate.release)
			}
		}()
		putResult := make(chan error, 1)
		go func() {
			putResult <- putCandidateObject(
				ctx,
				cleanupS3Client(storage, gate),
				cfg.S3Bucket,
				loser.ObjectKey,
				loserRaw,
				"image/png",
			)
		}()
		select {
		case <-gate.started:
		case <-ctx.Done():
			t.Fatal("late loser PUT did not reach its storage gate")
		}
		committed, err := transitionFinalizeReady(ctx, pool, claim, selected)
		if err != nil || !committed {
			t.Fatalf("select cleanup winner committed=%t error=%v", committed, err)
		}

		missing := &cleanupMissingHTTPClient{
			delegate:    realHTTP,
			escapedPath: cleanupObjectPath(cfg.S3Bucket, loser.ObjectKey),
		}
		var logs bytes.Buffer
		attempted, failed, err := worker(
			cleanupS3Client(storage, missing),
			slog.New(slog.NewJSONHandler(&logs, nil)),
		).processNext(ctx)
		if err != nil || !attempted || failed {
			t.Fatalf("absent loser cleanup attempted=%t failed=%t error=%v", attempted, failed, err)
		}
		var absent bool
		if err := pool.QueryRow(ctx, `
			SELECT
				last_result = 'confirmed_absent'
				AND next_attempt_at - last_attempt_at = interval '24 hours'
			FROM cleanup_tombstones
			WHERE object_key = $1::text`, loser.ObjectKey).Scan(&absent); err != nil || !absent {
			t.Fatalf("loser absence durable=%t error=%v", absent, err)
		}
		if !strings.Contains(logs.String(), `"outcome":"absent"`) ||
			!strings.Contains(logs.String(), `"result_applied":true`) {
			t.Fatal("cleanup absence logs omitted the durable outcome")
		}
		loserDigest := loser.ObjectKey[strings.LastIndex(loser.ObjectKey, "/")+1:]
		if strings.Contains(logs.String(), loserDigest) {
			t.Fatal("cleanup logs exposed an object digest")
		}

		close(gate.release)
		if err := <-putResult; err != nil {
			t.Fatal("release late loser PUT")
		}
		assertGarageObject(t, ctx, storage, cfg.S3Bucket, loser.ObjectKey, loserRaw, "image/png")
		forceCleanupDue(t, ctx, pool, loser.ObjectKey)
		attempted, failed, err = worker(storage, discardLogger).processNext(ctx)
		if err != nil || !attempted || failed {
			t.Fatalf("late loser convergence attempted=%t failed=%t error=%v", attempted, failed, err)
		}
		assertCleanupObjectMissing(t, ctx, storage, cfg.S3Bucket, loser.ObjectKey)
		assertGarageObject(t, ctx, storage, cfg.S3Bucket, selected.ObjectKey, selectedRaw, "image/png")

		var state, finalKey string
		var selectedTombstoned, loserTombstoned bool
		if err := pool.QueryRow(ctx, `
			SELECT
				state,
				final_key,
				EXISTS (SELECT 1 FROM cleanup_tombstones WHERE object_key = uploads.final_key),
				EXISTS (SELECT 1 FROM cleanup_tombstones WHERE object_key = $2::text)
			FROM uploads
			WHERE upload_id = $1::uuid`, uploadID, loser.ObjectKey).Scan(
			&state,
			&finalKey,
			&selectedTombstoned,
			&loserTombstoned,
		); err != nil {
			t.Fatal("read late-write cleanup outcome")
		}
		if state != "ready" || finalKey != selected.ObjectKey || selectedTombstoned || !loserTombstoned {
			t.Fatalf(
				"state=%q final_key_match=%t selected_tombstoned=%t loser_tombstoned=%t",
				state,
				finalKey == selected.ObjectKey,
				selectedTombstoned,
				loserTombstoned,
			)
		}
	})
}

func TestE2EGracefulShutdown(t *testing.T) {
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
	_, _, cleanupIdentity := seedDueCleanupTombstone(t, ctx, pool)
	defer cleanupUploadByIdempotencyKey(t, pool, cleanupIdentity)

	pendingID, pendingIdentity := testUUID(t), testUUID(t)
	seed, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal("begin shutdown request seed")
	}
	if err := insertPendingUpload(ctx, seed, pendingID, pendingIdentity, 1, "image/png"); err != nil {
		_ = seed.Rollback(context.Background())
		t.Fatal("insert shutdown request seed")
	}
	if err := seed.Commit(ctx); err != nil {
		t.Fatal("commit shutdown request seed")
	}
	defer cleanupUploadByIdempotencyKey(t, pool, pendingIdentity)

	deleteStarted := make(chan struct{})
	deleteCanceled := make(chan struct{})
	var startedOnce sync.Once
	var canceledOnce sync.Once
	storageServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodDelete {
			writer.WriteHeader(http.StatusOK)
			return
		}
		startedOnce.Do(func() { close(deleteStarted) })
		<-request.Context().Done()
		canceledOnce.Do(func() { close(deleteCanceled) })
	}))
	defer storageServer.Close()

	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("reserve shutdown listener")
	}
	address := probe.Addr().String()
	probe.Close()
	t.Setenv("HTTP_ADDR", address)
	t.Setenv("DATABASE_URL", cfg.DatabaseURL)
	t.Setenv("S3_BUCKET", cfg.S3Bucket)
	t.Setenv("AWS_REGION", cfg.AWSRegion)
	t.Setenv("S3_ENDPOINT", storageServer.URL)
	t.Setenv("AWS_ACCESS_KEY_ID", "shutdown-test-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "shutdown-test-secret")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

	serviceContext, stopService := context.WithCancel(context.Background())
	defer stopService()
	var logs bytes.Buffer
	serviceResult := make(chan error, 1)
	go func() {
		serviceResult <- run(serviceContext, slog.New(slog.NewJSONHandler(&logs, nil)))
	}()
	select {
	case <-deleteStarted:
	case err := <-serviceResult:
		t.Fatalf("service stopped before blocked cleanup: %v", err)
	case <-ctx.Done():
		t.Fatal("cleanup delete did not block")
	}

	client := &http.Client{Timeout: 15 * time.Second}
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+"/livez", nil)
		if err != nil {
			t.Fatal("build shutdown liveness request")
		}
		response, err := client.Do(request)
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		if ctx.Err() != nil {
			t.Fatal("service did not start before shutdown")
		}
	}

	holder := connectUploadTestPostgres(t, ctx, cfg.DatabaseURL)
	defer holder.Close(context.Background())
	holderTx, err := holder.Begin(ctx)
	if err != nil {
		t.Fatal("begin shutdown request blocker")
	}
	defer holderTx.Rollback(context.Background())
	if _, err := holderTx.Exec(ctx, `
		SELECT 1
		FROM uploads
		WHERE upload_id = $1::uuid
		FOR UPDATE`, pendingID); err != nil {
		t.Fatal("lock shutdown request upload")
	}

	type responseResult struct {
		status int
		err    error
	}
	requestResult := make(chan responseResult, 1)
	go func() {
		request, err := http.NewRequestWithContext(
			ctx,
			http.MethodPost,
			"http://"+address+"/uploads/"+pendingID+"/complete",
			nil,
		)
		if err != nil {
			requestResult <- responseResult{err: err}
			return
		}
		response, err := client.Do(request)
		if err != nil {
			requestResult <- responseResult{err: err}
			return
		}
		_, _ = io.Copy(io.Discard, response.Body)
		response.Body.Close()
		requestResult <- responseResult{status: response.StatusCode}
	}()
	waitForServicePostgresBlock(t, ctx, pool, holder.PgConn().PID())

	stopService()
	select {
	case <-deleteCanceled:
	case <-ctx.Done():
		t.Fatal("shutdown did not cancel blocked cleanup S3")
	}
	select {
	case result := <-requestResult:
		t.Fatalf("shutdown did not drain active HTTP request: %#v", result)
	default:
	}
	select {
	case err := <-serviceResult:
		t.Fatalf("service exited before active HTTP request drained: %v", err)
	default:
	}

	if err := holderTx.Rollback(ctx); err != nil {
		t.Fatal("release shutdown request blocker")
	}
	select {
	case result := <-requestResult:
		if result.err != nil || result.status != http.StatusAccepted {
			t.Fatalf("drained request status=%d error=%v", result.status, result.err)
		}
	case <-ctx.Done():
		t.Fatal("active HTTP request did not drain")
	}
	select {
	case err := <-serviceResult:
		if err != nil {
			t.Fatalf("graceful service shutdown: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("service did not finish graceful shutdown")
	}

	var state string
	if err := pool.QueryRow(ctx, `SELECT state FROM uploads WHERE upload_id = $1::uuid`, pendingID).Scan(&state); err != nil {
		t.Fatal("read drained completion outcome")
	}
	if state != "finalizing" {
		t.Fatalf("drained completion state=%q", state)
	}
	stopping := strings.Index(logs.String(), `"msg":"service.stopping"`)
	stopped := strings.Index(logs.String(), `"msg":"service.stopped"`)
	if stopping < 0 || stopped <= stopping {
		t.Fatal("service stop events are missing or out of order")
	}
}

func waitForServicePostgresBlock(
	t *testing.T,
	ctx context.Context,
	observer *pgxpool.Pool,
	blockerPID uint32,
) {
	t.Helper()
	for ctx.Err() == nil {
		var blocked bool
		if err := observer.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_stat_activity
				WHERE application_name = 's3-media-upload'
				  AND $1::integer = ANY(pg_blocking_pids(pid))
			)`, int32(blockerPID)).Scan(&blocked); err != nil {
			t.Fatal("observe shutdown request blocker")
		}
		if blocked {
			return
		}
	}
	t.Fatal("service request was not observed behind its PostgreSQL blocker")
}
