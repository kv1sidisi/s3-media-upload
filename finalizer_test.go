package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"image"
	"image/jpeg"
	"image/png"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgxpool"
)

type finalizerStorageError struct {
	code   string
	status int
}

func (err finalizerStorageError) Error() string       { return "storage error" }
func (err finalizerStorageError) ErrorCode() string   { return err.code }
func (err finalizerStorageError) HTTPStatusCode() int { return err.status }

func TestConfigAndRetryPolicy(t *testing.T) {
	tests := []struct {
		name       string
		streak     int
		errorClass string
		want       time.Duration
	}{
		{"first transient", 1, "transient", time.Second},
		{"second ambiguous", 2, "ambiguous", 2 * time.Second},
		{"bounded transient", 20, "transient", time.Minute},
		{"auth cadence", 1, "auth", time.Minute},
		{"configuration cadence", 9, "configuration", time.Minute},
		{"deterministic cadence", 3, "other_deterministic", time.Minute},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := finalizerRetryDelay(test.streak, test.errorClass); got != test.want {
				t.Fatalf("delay=%s, want %s", got, test.want)
			}
		})
	}
	if classifyStorageError(errObjectLengthMismatch, false) != "transient" ||
		classifyStorageError(context.DeadlineExceeded, false) != "transient" ||
		classifyStorageError(context.DeadlineExceeded, true) != "ambiguous" {
		t.Fatal("bounded read and timeout errors must remain retriable")
	}
	if storageObjectMissing(finalizerStorageError{code: "NoSuchBucket", status: http.StatusNotFound}) ||
		!storageObjectMissing(finalizerStorageError{code: "NoSuchKey", status: http.StatusNotFound}) {
		t.Fatal("explicit S3 error code must take precedence over HTTP 404")
	}
	for _, test := range []struct {
		class, reason, want string
	}{
		{"invalid_input", "object_too_large", "image_too_large"},
		{"invalid_input", "pixel_limit_exceeded", "image_too_large"},
		{"invalid_input", "malformed_image", "invalid_image"},
		{"internal_invariant", "candidate_integrity_mismatch", "upload_processing_failed"},
	} {
		if got := publicFailureCode(test.class, test.reason); got != test.want {
			t.Fatalf("publicFailureCode(%q, %q)=%q, want %q", test.class, test.reason, got, test.want)
		}
	}
}

func TestReadyCandidateInvariant(t *testing.T) {
	parent := lockedFinalizeUpload{
		DatabaseNow:       time.Date(2026, time.August, 31, 20, 0, 0, 0, time.UTC),
		MaxWriteExpiresAt: time.Date(2026, time.August, 31, 20, 15, 0, 0, time.UTC),
		StagingKey:        "staging/upload",
	}
	selected := trackedCandidate{ObjectKey: "media/upload/b"}
	loser := trackedCandidate{ObjectKey: "media/upload/a"}
	loser.SHA256[0] = 1
	targets := terminalTombstones(parent, []trackedCandidate{selected, loser}, selected.ObjectKey)
	if len(targets) != 2 || targets[0].ObjectKey != loser.ObjectKey ||
		targets[1].ObjectKey != parent.StagingKey ||
		targets[0].DeleteNotBefore != parent.DatabaseNow ||
		targets[1].DeleteNotBefore != parent.MaxWriteExpiresAt {
		t.Fatalf("terminal tombstones=%#v", targets)
	}
	for _, target := range targets {
		if target.ObjectKey == selected.ObjectKey {
			t.Fatal("selected candidate received a tombstone")
		}
	}
}

func TestWorkerSchedulingAndCancellation(t *testing.T) {
	if finalizerIdleWait != time.Second {
		t.Fatalf("finalizer idle wait=%s, want 1s", finalizerIdleWait)
	}
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		calls := make(chan time.Time, 3)
		done := make(chan struct{})
		go func() {
			defer close(done)
			runFinalizerLoop(ctx, func(context.Context) (bool, error) {
				calls <- time.Now()
				return false, nil
			})
		}()

		synctest.Wait()
		first := <-calls
		time.Sleep(finalizerIdleWait - time.Nanosecond)
		synctest.Wait()
		if len(calls) != 0 {
			t.Fatal("finalizer retried before the one-second idle wait")
		}
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		second := <-calls
		if second.Sub(first) != finalizerIdleWait {
			t.Fatalf("finalizer retry interval=%s, want %s", second.Sub(first), finalizerIdleWait)
		}

		cancel()
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Fatal("canceled finalizer did not stop")
		}
	})
}

func TestValidationMemoryHardCap(t *testing.T) {
	t.Run("8192x1024 JPEG", func(t *testing.T) {
		var encoded bytes.Buffer
		if err := jpeg.Encode(&encoded, image.NewRGBA(image.Rect(0, 0, 8_192, 1_024)), nil); err != nil {
			t.Fatal("encode JPEG memory boundary")
		}
		assertValidationBoundary(t, encoded.Bytes(), "image/jpeg", 8_192, 1_024)
	})
	t.Run("8192x1024 16-bit RGBA PNG", func(t *testing.T) {
		var encoded bytes.Buffer
		if err := png.Encode(&encoded, image.NewNRGBA64(image.Rect(0, 0, 8_192, 1_024))); err != nil {
			t.Fatal("encode PNG memory boundary")
		}
		assertValidationBoundary(t, encoded.Bytes(), "image/png", 8_192, 1_024)
	})
	t.Run("over-limit dimensions stop before full decode", func(t *testing.T) {
		encoded := validationPNGConfig(maxImageAxis+1, 1)
		_, failure, err := validateImage(bytes.NewReader(encoded), int64(len(encoded)), int64(len(encoded)), "image/png")
		if err != nil || failure == nil || failure.Reason != "dimensions_limit_exceeded" || failure.Phase != "decode_config" {
			t.Fatalf("failure=%#v error=%v", failure, err)
		}
	})
}

func assertValidationBoundary(t *testing.T, encoded []byte, contentType string, width, height int) {
	t.Helper()
	validated, failure, err := validateImage(bytes.NewReader(encoded), int64(len(encoded)), int64(len(encoded)), contentType)
	if err != nil || failure != nil || validated.Width != width || validated.Height != height {
		t.Fatalf(
			"boundary width=%d height=%d failure_reason=%q has_error=%t",
			validated.Width,
			validated.Height,
			validationFailureReason(failure),
			err != nil,
		)
	}
}

func validationFailureReason(failure *validationFailure) string {
	if failure == nil {
		return ""
	}
	return failure.Reason
}

func TestDueQueueAndTokenFencing(t *testing.T) {
	if testing.Short() {
		t.Skip("requires migrated PostgreSQL")
	}
	cfg := integrationConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := openPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		t.Fatal("open migrated PostgreSQL")
	}
	defer pool.Close()

	firstID, firstKey := seedFinalizingUpload(t, ctx, pool, 1, "image/png")
	defer cleanupUploadByIdempotencyKey(t, pool, firstKey)
	secondID, secondKey := seedFinalizingUpload(t, ctx, pool, 1, "image/png")
	defer cleanupUploadByIdempotencyKey(t, pool, secondKey)
	if _, err := pool.Exec(ctx, `
		UPDATE uploads
		SET reconcile_after = CASE upload_id
			WHEN $1::uuid THEN clock_timestamp() - interval '200 years'
			ELSE clock_timestamp() - interval '100 years'
		END
		WHERE upload_id IN ($1::uuid, $2::uuid)`, firstID, secondID); err != nil {
		t.Fatal("order finalizer queue")
	}

	firstClaim, ok, err := claimNextFinalizing(ctx, pool, cfg.FinalizeClaimLease)
	if err != nil || !ok || firstClaim.UploadID != firstID {
		t.Fatalf("first claim=%#v ok=%t error=%v", firstClaim, ok, err)
	}
	secondClaim, ok, err := claimNextFinalizing(ctx, pool, cfg.FinalizeClaimLease)
	if err != nil || !ok || secondClaim.UploadID != secondID || secondClaim.Token == firstClaim.Token {
		t.Fatalf("second claim=%#v ok=%t error=%v", secondClaim, ok, err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE uploads
		SET claim_expires_at = clock_timestamp() - interval '1 second'
		WHERE upload_id = $1::uuid
		  AND claim_token = $2::uuid`, firstID, firstClaim.Token); err != nil {
		t.Fatal("expire first claim")
	}
	reclaimed, ok, err := claimNextFinalizing(ctx, pool, cfg.FinalizeClaimLease)
	if err != nil || !ok || reclaimed.UploadID != firstID || reclaimed.Token == firstClaim.Token {
		t.Fatalf("reclaim=%#v ok=%t error=%v", reclaimed, ok, err)
	}
	_, applied, err := scheduleFinalizeRetry(ctx, pool, firstClaim, "staging_capture", "transient")
	if err != nil || applied {
		t.Fatalf("stale retry applied=%t error=%v", applied, err)
	}
	var currentToken string
	if err := pool.QueryRow(ctx, `
		SELECT claim_token::text
		FROM uploads
		WHERE upload_id = $1::uuid`, firstID).Scan(&currentToken); err != nil {
		t.Fatal("read current claim token")
	}
	if currentToken != reclaimed.Token {
		t.Fatal("stale actor changed the fresh claim")
	}
}

func TestDurableBoundaryRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("requires migrated PostgreSQL")
	}
	cfg := integrationConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := openPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		t.Fatal("open migrated PostgreSQL")
	}
	defer pool.Close()
	uploadID, idempotencyKey := seedFinalizingUpload(t, ctx, pool, 1, "image/png")
	defer cleanupUploadByIdempotencyKey(t, pool, idempotencyKey)
	rolledClaimToken := testUUID(t)
	rolledClaim, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal("begin discarded claim transaction")
	}
	rolledClaimResult, err := rolledClaim.Exec(ctx, `
		UPDATE uploads
		SET claim_token = $2::uuid,
		    claim_expires_at = clock_timestamp() + interval '30 seconds'
		WHERE upload_id = $1::uuid
		  AND state = 'finalizing'`, uploadID, rolledClaimToken)
	if err != nil || rolledClaimResult.RowsAffected() != 1 {
		_ = rolledClaim.Rollback(context.Background())
		t.Fatal("apply discarded claim transaction")
	}
	if err := rolledClaim.Rollback(ctx); err != nil {
		t.Fatal("rollback discarded claim transaction")
	}
	var claimAbsent bool
	if err := pool.QueryRow(ctx, `
		SELECT claim_token IS NULL
		FROM uploads
		WHERE upload_id = $1::uuid`, uploadID).Scan(&claimAbsent); err != nil || !claimAbsent {
		t.Fatalf("discarded claim leaked: absent=%t error=%v", claimAbsent, err)
	}

	claim, ok, err := claimNextFinalizing(ctx, pool, cfg.FinalizeClaimLease)
	if err != nil || !ok || claim.UploadID != uploadID {
		t.Fatalf("claim=%#v ok=%t error=%v", claim, ok, err)
	}
	confirmedClaim, confirmed, err := confirmFinalizeClaim(ctx, pool, claim)
	if err != nil || !confirmed || confirmedClaim.Token != claim.Token {
		t.Fatalf("confirmed claim=%#v confirmed=%t error=%v", confirmedClaim, confirmed, err)
	}
	digest := sha256.Sum256([]byte("x"))
	candidate := trackedCandidate{
		SHA256:                  digest,
		ObjectKey:               "media/" + uploadID + "/" + hex.EncodeToString(digest[:]),
		EncodedSize:             1,
		ValidationPolicyVersion: validationPolicyVersion,
		Format:                  "png",
		Width:                   1,
		Height:                  1,
	}
	rolledCandidate, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal("begin discarded candidate transaction")
	}
	rolledCandidateResult, err := rolledCandidate.Exec(ctx, `
		INSERT INTO upload_candidates (
			upload_id, sha256, object_key, encoded_size,
			validation_policy_version, image_format, width, height, registered_at
		)
		VALUES ($1::uuid, $2::bytea, $3::text, $4::bigint, $5::smallint, $6::text, $7::integer, $8::integer, clock_timestamp())`,
		uploadID,
		candidate.SHA256[:],
		candidate.ObjectKey,
		candidate.EncodedSize,
		candidate.ValidationPolicyVersion,
		candidate.Format,
		candidate.Width,
		candidate.Height,
	)
	if err != nil || rolledCandidateResult.RowsAffected() != 1 {
		_ = rolledCandidate.Rollback(context.Background())
		t.Fatal("apply discarded candidate transaction")
	}
	if err := rolledCandidate.Rollback(ctx); err != nil {
		t.Fatal("rollback discarded candidate transaction")
	}
	tracked, err := candidateTracked(ctx, pool, uploadID, candidate)
	if err != nil || tracked {
		t.Fatalf("discarded candidate tracked=%t error=%v", tracked, err)
	}
	active, mismatch, err := registerCandidate(ctx, pool, claim, candidate)
	if err != nil || !active || mismatch {
		t.Fatalf("register active=%t mismatch=%t error=%v", active, mismatch, err)
	}
	tracked, err = candidateTracked(ctx, pool, uploadID, candidate)
	if err != nil || !tracked {
		t.Fatalf("discarded candidate result tracked=%t error=%v", tracked, err)
	}

	rollback, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal("begin discarded terminal transaction")
	}
	result, err := rollback.Exec(ctx, `
		UPDATE uploads
		SET state = 'ready',
		    reconcile_after = NULL,
		    claim_token = NULL,
		    claim_expires_at = NULL,
		    terminal_at = clock_timestamp(),
		    final_key = $3::text
		WHERE upload_id = $1::uuid
		  AND state = 'finalizing'
		  AND claim_token = $2::uuid`, uploadID, claim.Token, candidate.ObjectKey)
	if err != nil || result.RowsAffected() != 1 {
		_ = rollback.Rollback(context.Background())
		t.Fatal("apply discarded terminal transaction")
	}
	if err := rollback.Rollback(ctx); err != nil {
		t.Fatal("rollback discarded terminal transaction")
	}
	var state, token string
	if err := pool.QueryRow(ctx, `
		SELECT state, claim_token::text
		FROM uploads
		WHERE upload_id = $1::uuid`, uploadID).Scan(&state, &token); err != nil {
		t.Fatal("read rolled back terminal transaction")
	}
	if state != "finalizing" || token != claim.Token {
		t.Fatalf("rollback leaked terminal state=%q token_match=%t", state, token == claim.Token)
	}

	committed, err := transitionFinalizeReady(ctx, pool, claim, candidate)
	if err != nil || !committed {
		t.Fatalf("terminal commit=%t error=%v", committed, err)
	}
	confirmed, err = confirmReady(ctx, pool, uploadID, candidate.ObjectKey)
	if err != nil || !confirmed {
		t.Fatalf("discarded terminal result confirmed=%t error=%v", confirmed, err)
	}
	_, staleApplied, err := scheduleFinalizeRetry(ctx, pool, claim, "terminal_commit", "ambiguous")
	if err != nil || staleApplied {
		t.Fatalf("stale retry applied=%t error=%v", staleApplied, err)
	}
}

func TestParentFirstCandidateTerminalRace(t *testing.T) {
	if testing.Short() {
		t.Skip("requires migrated PostgreSQL")
	}
	cfg := integrationConfig(t)
	for _, order := range []struct {
		name           string
		candidateFirst bool
	}{
		{"terminal first", false},
		{"candidate first", true},
	} {
		t.Run(order.name, func(t *testing.T) {
			testParentFirstCandidateTerminalRace(t, cfg, order.candidateFirst)
		})
	}
}

func TestE2ERejectedImages(t *testing.T) {
	if testing.Short() {
		t.Skip("requires migrated PostgreSQL and Garage")
	}
	cfg := integrationConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
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
	app := &application{
		logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		getUpload: func(ctx context.Context, uploadID string) (uploadRepresentation, error) {
			return getUploadByID(ctx, pool, uploadID)
		},
		getContent: func(ctx context.Context, uploadID string) (contentReadResult, error) {
			return authorizeContentRead(ctx, pool, func(context.Context, string) (string, time.Time, error) {
				return "", time.Time{}, errContentSigningInvalid
			}, uploadID)
		},
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("start rejection test listener")
	}
	server := newHTTPServer(listener.Addr().String(), app)
	serveResult := make(chan error, 1)
	go func() { serveResult <- server.Serve(listener) }()
	defer func() {
		_ = server.Close()
		<-serveResult
	}()
	httpClient := &http.Client{Timeout: 10 * time.Second}
	baseURL := "http://" + listener.Addr().String()

	for _, test := range []struct {
		name         string
		raw          []byte
		declaredSize int64
		wantReason   string
		wantCode     string
		wantPhase    string
		oversized    bool
	}{
		{
			name:         "malformed",
			raw:          []byte("not a PNG"),
			declaredSize: int64(len("not a PNG")),
			wantReason:   "invalid_image_encoding",
			wantCode:     "invalid_image",
			wantPhase:    "decode_config",
		},
		{
			name:         "direct-S3 oversized",
			raw:          make([]byte, maxUploadSizeBytes+1),
			declaredSize: maxUploadSizeBytes,
			wantReason:   "object_too_large",
			wantCode:     "image_too_large",
			wantPhase:    "staging_read",
			oversized:    true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			uploadID, idempotencyKey := seedFinalizingUpload(t, ctx, pool, test.declaredSize, "image/png")
			defer cleanupUploadByIdempotencyKey(t, pool, idempotencyKey)
			stagingKey := "staging/" + uploadID
			keys := []string{stagingKey}
			defer deleteFinalizerObjects(t, storage, cfg.S3Bucket, &keys)
			if err := putCandidateObject(ctx, storage, cfg.S3Bucket, stagingKey, test.raw, "image/png"); err != nil {
				t.Fatal("put rejected staging object")
			}

			processed, err := worker.processNext(ctx)
			if err != nil || !processed {
				t.Fatalf("finalizer processed=%t error=%v", processed, err)
			}
			statusResponse, err := httpClient.Get(baseURL + "/uploads/" + uploadID)
			if err != nil {
				t.Fatal("GET rejected upload")
			}
			statusBody, readErr := io.ReadAll(io.LimitReader(statusResponse.Body, 32<<10))
			statusResponse.Body.Close()
			var status uploadRepresentation
			if readErr != nil || statusResponse.StatusCode != http.StatusOK ||
				statusResponse.Header.Get("Content-Type") != "application/json" ||
				statusResponse.Header.Get("Cache-Control") != "no-store" ||
				json.Unmarshal(statusBody, &status) != nil || status.State != "rejected" || status.Failure == nil ||
				status.Failure.Code != test.wantCode || status.Image != nil || status.UploadRequest != nil {
				t.Fatalf("rejected HTTP representation status=%d read_error=%v", statusResponse.StatusCode, readErr)
			}
			contentResponse, err := httpClient.Get(baseURL + "/uploads/" + uploadID + "/content")
			if err != nil {
				t.Fatal("GET rejected content")
			}
			contentBody, readErr := io.ReadAll(io.LimitReader(contentResponse.Body, 32<<10))
			contentResponse.Body.Close()
			if readErr != nil || contentResponse.StatusCode != http.StatusUnprocessableEntity ||
				contentResponse.Header.Get("Cache-Control") != "no-store" ||
				contentResponse.Header.Get("Location") != "" || responseErrorCode(t, contentBody) != test.wantCode {
				t.Fatalf("rejected content response status=%d read_error=%v", contentResponse.StatusCode, readErr)
			}

			var state, class, reason, phase string
			var finalKeyAbsent, evidenceValid, stagingTombstoneValid bool
			var policyVersion, candidateCount, tombstoneCount int
			if err := pool.QueryRow(ctx, `
				SELECT
					state,
					final_key IS NULL,
					rejection_class,
					rejection_reason,
					rejection_phase,
					rejection_policy_version,
					(rejection_evidence->>'policy_version' = '1'
					 AND (rejection_evidence->>'observed_size')::bigint = $2::bigint
					 AND CASE WHEN $3::boolean
					     THEN (rejection_evidence->>'limit_bytes')::bigint = $4::bigint
					     ELSE rejection_evidence ? 'observed_sha256'
					 END),
					(SELECT count(*) FROM upload_candidates WHERE upload_id = uploads.upload_id),
					(SELECT count(*) FROM cleanup_tombstones WHERE upload_id = uploads.upload_id),
					EXISTS (
						SELECT 1
						FROM cleanup_tombstones
						WHERE upload_id = uploads.upload_id
						  AND object_key = uploads.staging_key
						  AND candidate_sha256 IS NULL
						  AND delete_not_before = uploads.max_write_expires_at
					)
				FROM uploads
				WHERE upload_id = $1::uuid`,
				uploadID,
				len(test.raw),
				test.oversized,
				maxUploadSizeBytes,
			).Scan(
				&state,
				&finalKeyAbsent,
				&class,
				&reason,
				&phase,
				&policyVersion,
				&evidenceValid,
				&candidateCount,
				&tombstoneCount,
				&stagingTombstoneValid,
			); err != nil {
				t.Fatal("read durable rejection")
			}
			if state != "rejected" || !finalKeyAbsent || class != "invalid_input" ||
				reason != test.wantReason || phase != test.wantPhase || policyVersion != validationPolicyVersion ||
				!evidenceValid || candidateCount != 0 || tombstoneCount != 1 || !stagingTombstoneValid {
				t.Fatalf(
					"state=%q final_key_absent=%t class=%q reason=%q phase=%q policy=%d evidence=%t candidates=%d tombstones=%d staging_tombstone=%t",
					state,
					finalKeyAbsent,
					class,
					reason,
					phase,
					policyVersion,
					evidenceValid,
					candidateCount,
					tombstoneCount,
					stagingTombstoneValid,
				)
			}
		})
	}
}

func testParentFirstCandidateTerminalRace(t *testing.T, cfg config, candidateFirst bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	readyPool := newSingleConnectionPool(t, ctx, cfg.DatabaseURL)
	defer readyPool.Close()
	registerPool := newSingleConnectionPool(t, ctx, cfg.DatabaseURL)
	defer registerPool.Close()
	holder := connectUploadTestPostgres(t, ctx, cfg.DatabaseURL)
	defer holder.Close(context.Background())
	observer := connectUploadTestPostgres(t, ctx, cfg.DatabaseURL)
	defer observer.Close(context.Background())
	uploadID, idempotencyKey := seedFinalizingUpload(t, ctx, readyPool, 1, "image/png")
	defer cleanupUploadByIdempotencyKey(t, readyPool, idempotencyKey)
	claim, ok, err := claimNextFinalizing(ctx, readyPool, cfg.FinalizeClaimLease)
	if err != nil || !ok || claim.UploadID != uploadID {
		t.Fatalf("claim=%#v ok=%t error=%v", claim, ok, err)
	}
	firstDigest := sha256.Sum256([]byte("a"))
	winner := trackedCandidate{
		SHA256: firstDigest, ObjectKey: "media/" + uploadID + "/" + hex.EncodeToString(firstDigest[:]),
		EncodedSize: 1, ValidationPolicyVersion: validationPolicyVersion, Format: "png", Width: 1, Height: 1,
	}
	if active, mismatch, err := registerCandidate(ctx, readyPool, claim, winner); err != nil || !active || mismatch {
		t.Fatalf("register winner active=%t mismatch=%t error=%v", active, mismatch, err)
	}
	secondDigest := sha256.Sum256([]byte("b"))
	loser := trackedCandidate{
		SHA256: secondDigest, ObjectKey: "media/" + uploadID + "/" + hex.EncodeToString(secondDigest[:]),
		EncodedSize: 1, ValidationPolicyVersion: validationPolicyVersion, Format: "png", Width: 1, Height: 1,
	}

	holderTx, err := holder.Begin(ctx)
	if err != nil {
		t.Fatal("begin parent holder")
	}
	defer holderTx.Rollback(context.Background())
	if _, err := holderTx.Exec(ctx, `SELECT 1 FROM uploads WHERE upload_id = $1::uuid FOR UPDATE`, uploadID); err != nil {
		t.Fatal("lock finalizer parent")
	}
	readyPID := singlePoolPID(t, ctx, readyPool)
	registerPID := singlePoolPID(t, ctx, registerPool)
	readyResult := make(chan struct {
		committed bool
		err       error
	}, 1)
	registerResult := make(chan struct {
		active, mismatch bool
		err              error
	}, 1)
	startReady := func() {
		go func() {
			committed, err := transitionFinalizeReady(ctx, readyPool, claim, winner)
			readyResult <- struct {
				committed bool
				err       error
			}{committed, err}
		}()
	}
	startRegister := func() {
		go func() {
			active, mismatch, err := registerCandidate(ctx, registerPool, claim, loser)
			registerResult <- struct {
				active, mismatch bool
				err              error
			}{active, mismatch, err}
		}()
	}
	if candidateFirst {
		startRegister()
		waitForPostgresBlock(t, ctx, observer, registerPID, holder.PgConn().PID())
		startReady()
		waitForAnyPostgresBlock(t, ctx, observer, readyPID)
	} else {
		startReady()
		waitForPostgresBlock(t, ctx, observer, readyPID, holder.PgConn().PID())
		startRegister()
		waitForAnyPostgresBlock(t, ctx, observer, registerPID)
	}
	if err := holderTx.Commit(ctx); err != nil {
		t.Fatal("release finalizer parent")
	}
	readyOutcome := <-readyResult
	registerOutcome := <-registerResult
	if readyOutcome.err != nil || !readyOutcome.committed {
		t.Fatalf("ready committed=%t error=%v", readyOutcome.committed, readyOutcome.err)
	}
	if registerOutcome.err != nil || registerOutcome.active != candidateFirst || registerOutcome.mismatch {
		t.Fatalf(
			"registration active=%t mismatch=%t error=%v",
			registerOutcome.active,
			registerOutcome.mismatch,
			registerOutcome.err,
		)
	}
	var finalKey string
	var candidates int
	var selectedTombstoned, loserTombstoned, stagingTombstoned bool
	if err := readyPool.QueryRow(ctx, `
		SELECT
			final_key,
			(SELECT count(*) FROM upload_candidates WHERE upload_id = uploads.upload_id),
			EXISTS (SELECT 1 FROM cleanup_tombstones WHERE object_key = uploads.final_key),
			EXISTS (SELECT 1 FROM cleanup_tombstones WHERE object_key = $2::text),
			EXISTS (SELECT 1 FROM cleanup_tombstones WHERE object_key = uploads.staging_key)
		FROM uploads
		WHERE upload_id = $1::uuid`, uploadID, loser.ObjectKey).Scan(
		&finalKey,
		&candidates,
		&selectedTombstoned,
		&loserTombstoned,
		&stagingTombstoned,
	); err != nil {
		t.Fatal("read parent-first race winner")
	}
	wantCandidates := 1
	if candidateFirst {
		wantCandidates = 2
	}
	if finalKey != winner.ObjectKey || candidates != wantCandidates || selectedTombstoned || loserTombstoned != candidateFirst || !stagingTombstoned {
		t.Fatalf(
			"final_key_match=%t candidates=%d selected_tombstoned=%t loser_tombstoned=%t staging_tombstoned=%t",
			finalKey == winner.ObjectKey,
			candidates,
			selectedTombstoned,
			loserTombstoned,
			stagingTombstoned,
		)
	}
}

func TestIntegrationFinalizeVerifiedUpload(t *testing.T) {
	if testing.Short() {
		t.Skip("requires migrated PostgreSQL and Garage")
	}
	cfg := integrationConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
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
	presigner := s3.NewPresignClient(storage)
	worker := &finalizer{
		logger:     slog.New(slog.NewJSONHandler(io.Discard, nil)),
		pool:       pool,
		storage:    storage,
		bucket:     cfg.S3Bucket,
		claimLease: cfg.FinalizeClaimLease,
	}

	t.Run("valid JPEG and PNG become byte-identical ready content", func(t *testing.T) {
		for _, test := range []struct {
			format, contentType string
		}{
			{"jpeg", "image/jpeg"},
			{"png", "image/png"},
		} {
			t.Run(test.format, func(t *testing.T) {
				raw := encodeValidationImage(t, test.format)
				uploadID, idempotencyKey := seedFinalizingUpload(t, ctx, pool, int64(len(raw)), test.contentType)
				defer cleanupUploadByIdempotencyKey(t, pool, idempotencyKey)
				digest := sha256.Sum256(raw)
				expectedCandidateKey := "media/" + uploadID + "/" + hex.EncodeToString(digest[:])
				keys := []string{"staging/" + uploadID, expectedCandidateKey}
				defer deleteFinalizerObjects(t, storage, cfg.S3Bucket, &keys)
				if err := putCandidateObject(ctx, storage, cfg.S3Bucket, keys[0], raw, test.contentType); err != nil {
					t.Fatal("put staging image")
				}

				processed, err := worker.processNext(ctx)
				if err != nil || !processed {
					t.Fatalf("finalizer processed=%t error=%v", processed, err)
				}
				status, err := getUploadByID(ctx, pool, uploadID)
				if err != nil || status.State != "ready" || status.Failure != nil || status.Image == nil ||
					status.Image.SizeBytes != int64(len(raw)) || status.Image.ContentType != test.contentType ||
					status.Image.Width != 2 || status.Image.Height != 3 {
					t.Fatalf("ready representation=%#v error=%v", status, err)
				}

				var finalKey string
				var candidateCount, tombstoneCount int
				var selectedTombstoned, stagingTombstoned bool
				if err := pool.QueryRow(ctx, `
			SELECT
				final_key,
				(SELECT count(*) FROM upload_candidates WHERE upload_id = uploads.upload_id),
				(SELECT count(*) FROM cleanup_tombstones WHERE upload_id = uploads.upload_id),
				EXISTS (SELECT 1 FROM cleanup_tombstones WHERE object_key = uploads.final_key),
				EXISTS (SELECT 1 FROM cleanup_tombstones WHERE object_key = uploads.staging_key)
			FROM uploads
			WHERE upload_id = $1::uuid`, uploadID).Scan(
					&finalKey,
					&candidateCount,
					&tombstoneCount,
					&selectedTombstoned,
					&stagingTombstoned,
				); err != nil {
					t.Fatal("read ready invariants")
				}
				if finalKey != expectedCandidateKey || candidateCount != 1 || tombstoneCount != 1 || selectedTombstoned || !stagingTombstoned {
					t.Fatalf(
						"final_key_match=%t candidate_count=%d tombstones=%d selected_tombstoned=%t staging_tombstoned=%t",
						finalKey == expectedCandidateKey,
						candidateCount,
						tombstoneCount,
						selectedTombstoned,
						stagingTombstoned,
					)
				}

				content, err := authorizeContentRead(ctx, pool, func(ctx context.Context, key string) (string, time.Time, error) {
					return presignContentGET(ctx, presigner, cfg.S3Bucket, key)
				}, uploadID)
				if err != nil || content.State != "ready" || content.URL == "" {
					t.Fatalf("content capability=%#v error=%v", content, err)
				}
				request, err := http.NewRequestWithContext(ctx, http.MethodGet, content.URL, nil)
				if err != nil {
					t.Fatal("build content GET")
				}
				response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
				if err != nil {
					t.Fatal("download ready content")
				}
				downloaded, readErr := io.ReadAll(io.LimitReader(response.Body, maxUploadSizeBytes+1))
				response.Body.Close()
				if readErr != nil || response.StatusCode != http.StatusOK || !bytes.Equal(downloaded, raw) {
					t.Fatalf("download status=%d read_error=%v bytes_equal=%t", response.StatusCode, readErr, bytes.Equal(downloaded, raw))
				}
			})
		}
	})

	t.Run("malformed bytes become immutable invalid_image", func(t *testing.T) {
		raw := []byte("not a PNG")
		uploadID, idempotencyKey := seedFinalizingUpload(t, ctx, pool, int64(len(raw)), "image/png")
		defer cleanupUploadByIdempotencyKey(t, pool, idempotencyKey)
		keys := []string{"staging/" + uploadID}
		defer deleteFinalizerObjects(t, storage, cfg.S3Bucket, &keys)
		if err := putCandidateObject(ctx, storage, cfg.S3Bucket, keys[0], raw, "image/png"); err != nil {
			t.Fatal("put malformed staging object")
		}

		processed, err := worker.processNext(ctx)
		if err != nil || !processed {
			t.Fatalf("finalizer processed=%t error=%v", processed, err)
		}
		status, err := getUploadByID(ctx, pool, uploadID)
		if err != nil || status.State != "rejected" || status.Failure == nil || status.Failure.Code != "invalid_image" || status.Image != nil {
			t.Fatalf("rejected representation=%#v error=%v", status, err)
		}
		var reason, class string
		var candidateCount, tombstoneCount int
		if err := pool.QueryRow(ctx, `
			SELECT
				rejection_reason,
				rejection_class,
				(SELECT count(*) FROM upload_candidates WHERE upload_id = uploads.upload_id),
				(SELECT count(*) FROM cleanup_tombstones WHERE upload_id = uploads.upload_id)
			FROM uploads
			WHERE upload_id = $1::uuid`, uploadID).Scan(
			&reason,
			&class,
			&candidateCount,
			&tombstoneCount,
		); err != nil {
			t.Fatal("read rejection invariants")
		}
		if reason != "invalid_image_encoding" || class != "invalid_input" || candidateCount != 0 || tombstoneCount != 1 {
			t.Fatalf("reason=%q class=%q candidates=%d tombstones=%d", reason, class, candidateCount, tombstoneCount)
		}
		content, err := authorizeContentRead(ctx, pool, func(context.Context, string) (string, time.Time, error) {
			t.Fatal("rejected content minted a capability")
			return "", time.Time{}, nil
		}, uploadID)
		if err != nil || content.FailureCode != "invalid_image" {
			t.Fatalf("rejected content=%#v error=%v", content, err)
		}
	})

	t.Run("candidate mismatch becomes safe internal rejection", func(t *testing.T) {
		raw := encodeValidationImage(t, "png")
		uploadID, idempotencyKey := seedFinalizingUpload(t, ctx, pool, int64(len(raw)), "image/png")
		defer cleanupUploadByIdempotencyKey(t, pool, idempotencyKey)
		digest := sha256.Sum256(raw)
		candidate := trackedCandidate{
			SHA256:                  digest,
			ObjectKey:               "media/" + uploadID + "/" + hex.EncodeToString(digest[:]),
			EncodedSize:             int64(len(raw)),
			ValidationPolicyVersion: validationPolicyVersion,
			Format:                  "png",
			Width:                   2,
			Height:                  3,
		}
		keys := []string{"staging/" + uploadID, candidate.ObjectKey}
		defer deleteFinalizerObjects(t, storage, cfg.S3Bucket, &keys)
		claim, ok, err := claimNextFinalizing(ctx, pool, cfg.FinalizeClaimLease)
		if err != nil || !ok || claim.UploadID != uploadID {
			t.Fatalf("claim=%#v ok=%t error=%v", claim, ok, err)
		}
		active, mismatch, err := registerCandidate(ctx, pool, claim, candidate)
		if err != nil || !active || mismatch {
			t.Fatalf("register active=%t mismatch=%t error=%v", active, mismatch, err)
		}
		corrupted := append([]byte(nil), raw...)
		corrupted[len(corrupted)-1] ^= 0xff
		if err := putCandidateObject(ctx, storage, cfg.S3Bucket, candidate.ObjectKey, corrupted, "image/png"); err != nil {
			t.Fatal("put corrupted candidate")
		}
		if _, err := pool.Exec(ctx, `
			UPDATE uploads
			SET claim_expires_at = clock_timestamp() - interval '1 second'
			WHERE upload_id = $1::uuid
			  AND claim_token = $2::uuid`, uploadID, claim.Token); err != nil {
			t.Fatal("expire mismatch setup claim")
		}

		processed, err := worker.processNext(ctx)
		if err != nil || !processed {
			t.Fatalf("mismatch processed=%t error=%v", processed, err)
		}
		status, err := getUploadByID(ctx, pool, uploadID)
		if err != nil || status.State != "rejected" || status.Failure == nil || status.Failure.Code != "upload_processing_failed" {
			t.Fatalf("mismatch representation=%#v error=%v", status, err)
		}
		var reason string
		var tombstones int
		if err := pool.QueryRow(ctx, `
			SELECT rejection_reason,
			       (SELECT count(*) FROM cleanup_tombstones WHERE upload_id = uploads.upload_id)
			FROM uploads
			WHERE upload_id = $1::uuid`, uploadID).Scan(&reason, &tombstones); err != nil {
			t.Fatal("read mismatch rejection")
		}
		if reason != "candidate_integrity_mismatch" || tombstones != 2 {
			t.Fatalf("reason=%q tombstones=%d", reason, tombstones)
		}
	})

	t.Run("tracked hidden PUT recovers before staging", func(t *testing.T) {
		raw := encodeValidationImage(t, "png")
		uploadID, idempotencyKey := seedFinalizingUpload(t, ctx, pool, int64(len(raw)), "image/png")
		defer cleanupUploadByIdempotencyKey(t, pool, idempotencyKey)
		keys := []string{"staging/" + uploadID}
		defer deleteFinalizerObjects(t, storage, cfg.S3Bucket, &keys)

		claim, ok, err := claimNextFinalizing(ctx, pool, cfg.FinalizeClaimLease)
		if err != nil || !ok || claim.UploadID != uploadID {
			t.Fatalf("claim=%#v ok=%t error=%v", claim, ok, err)
		}
		validated, failure, err := validateImage(bytes.NewReader(raw), int64(len(raw)), int64(len(raw)), "image/png")
		if err != nil || failure != nil {
			t.Fatalf("validate recovery image failure=%#v error=%v", failure, err)
		}
		candidate := trackedCandidate{
			SHA256:                  validated.SHA256,
			ObjectKey:               "media/" + uploadID + "/" + hex.EncodeToString(validated.SHA256[:]),
			EncodedSize:             validated.SizeBytes,
			ValidationPolicyVersion: validationPolicyVersion,
			Format:                  validated.Format,
			Width:                   validated.Width,
			Height:                  validated.Height,
		}
		active, mismatch, err := registerCandidate(ctx, pool, claim, candidate)
		if err != nil || !active || mismatch {
			t.Fatalf("register active=%t mismatch=%t error=%v", active, mismatch, err)
		}
		keys = append(keys, candidate.ObjectKey)
		if err := putCandidateObject(ctx, storage, cfg.S3Bucket, candidate.ObjectKey, raw, "image/png"); err != nil {
			t.Fatal("apply hidden candidate PUT")
		}
		if _, err := pool.Exec(ctx, `
			UPDATE uploads
			SET claim_expires_at = clock_timestamp() - interval '1 second'
			WHERE upload_id = $1::uuid
			  AND claim_token = $2::uuid`, uploadID, claim.Token); err != nil {
			t.Fatal("expire abandoned claim")
		}

		processed, err := worker.processNext(ctx)
		if err != nil || !processed {
			t.Fatalf("recovery processed=%t error=%v", processed, err)
		}
		var state, finalKey string
		var candidates int
		if err := pool.QueryRow(ctx, `
			SELECT state, final_key, (SELECT count(*) FROM upload_candidates WHERE upload_id = uploads.upload_id)
			FROM uploads
			WHERE upload_id = $1::uuid`, uploadID).Scan(&state, &finalKey, &candidates); err != nil {
			t.Fatal("read recovered candidate")
		}
		if state != "ready" || finalKey != candidate.ObjectKey || candidates != 1 {
			t.Fatalf("state=%q final_key_match=%t candidates=%d", state, finalKey == candidate.ObjectKey, candidates)
		}
		committed, err := transitionFinalizeRejected(ctx, pool, claim, validationFailure{
			Class: "internal_invariant", Reason: "candidate_integrity_mismatch", Phase: "candidate_verify",
			Evidence: map[string]any{"policy_version": 1},
		})
		if err != nil || committed {
			t.Fatalf("stale terminal mutation committed=%t error=%v", committed, err)
		}
	})
}

func seedFinalizingUpload(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	size int64,
	contentType string,
) (string, string) {
	t.Helper()
	uploadID, idempotencyKey := testUUID(t), testUUID(t)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal("begin finalizer seed")
	}
	defer tx.Rollback(context.Background())
	if err := insertPendingUpload(ctx, tx, uploadID, idempotencyKey, size, contentType); err != nil {
		t.Fatal("insert finalizer seed")
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal("commit finalizer seed")
	}
	completed, err := completeUploadByID(ctx, pool, uploadID)
	if err != nil || completed.Upload.State != "finalizing" {
		t.Fatalf("completion=%#v error=%v", completed, err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE uploads
		SET reconcile_after = clock_timestamp() - interval '100 years'
		WHERE upload_id = $1::uuid`, uploadID); err != nil {
		t.Fatal("make finalizer seed due")
	}
	return uploadID, idempotencyKey
}

func deleteFinalizerObjects(
	t *testing.T,
	storage *s3.Client,
	bucket string,
	keys *[]string,
) {
	t.Helper()
	cleanupContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, key := range *keys {
		if _, err := storage.DeleteObject(cleanupContext, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		}); err != nil {
			t.Errorf("delete finalizer object %q", key)
		}
	}
}
