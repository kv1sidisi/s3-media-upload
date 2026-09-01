package app

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgxpool"
)

type recoveryS3TransportError struct{}

func (*recoveryS3TransportError) Error() string   { return "injected S3 transport fault" }
func (*recoveryS3TransportError) Timeout() bool   { return false }
func (*recoveryS3TransportError) Temporary() bool { return true }

var errRecoveryS3Fault = &recoveryS3TransportError{}

type recoveryS3Action int

const (
	recoveryFailBefore recoveryS3Action = iota
	recoveryDelegateThenFail
	recoveryTruncateResponseBody
)

type recoveryS3HTTPClient struct {
	delegate    s3.HTTPClient
	method      string
	escapedPath string
	action      recoveryS3Action
	hits        int
	delegated   bool
	bodyFaulted bool
}

func (client *recoveryS3HTTPClient) Do(request *http.Request) (*http.Response, error) {
	if request.Method != client.method || request.URL.EscapedPath() != client.escapedPath {
		return client.delegate.Do(request)
	}

	client.hits++
	if client.hits != 1 {
		return client.delegate.Do(request)
	}
	if client.action == recoveryFailBefore {
		return nil, errRecoveryS3Fault
	}
	response, err := client.delegate.Do(request)
	if err != nil || response.StatusCode < 200 || response.StatusCode >= 300 {
		return response, err
	}
	client.delegated = true
	if client.action == recoveryTruncateResponseBody {
		response.Body = &recoveryPartialErrorBody{ReadCloser: response.Body, faulted: &client.bodyFaulted}
		return response, nil
	}
	_, readErr := io.Copy(io.Discard, response.Body)
	closeErr := response.Body.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return nil, errRecoveryS3Fault
}

type recoveryPartialErrorBody struct {
	io.ReadCloser
	emitted bool
	faulted *bool
}

func (body *recoveryPartialErrorBody) Read(buffer []byte) (int, error) {
	if body.emitted {
		*body.faulted = true
		return 0, errRecoveryS3Fault
	}
	if len(buffer) > 1 {
		buffer = buffer[:1]
	}
	read, err := body.ReadCloser.Read(buffer)
	if read > 0 {
		body.emitted = true
		return read, nil
	}
	return read, err
}

func TestE2EFinalizeRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("requires migrated PostgreSQL and Garage")
	}
	cfg := integrationConfig(t)
	if cfg.S3Endpoint == "" {
		t.Fatal("explicit S3_ENDPOINT is required for Garage recovery")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	pool, err := openPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		t.Fatal("open migrated PostgreSQL")
	}
	defer pool.Close()
	cleanStorage, err := newS3Client(ctx, cfg)
	if err != nil {
		t.Fatal("create clean Garage client")
	}
	raw := encodeValidationImage(t, "png")
	validated, failure, err := validateImageBytes(raw, int64(len(raw)), int64(len(raw)), "image/png")
	if err != nil || failure != nil {
		t.Fatalf("validate recovery image failure=%#v error=%v", failure, err)
	}
	realHTTP := &http.Client{
		Timeout: 10 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	setup := func(t *testing.T, withStaging bool) (string, trackedCandidate) {
		t.Helper()
		uploadID, idempotencyKey := seedFinalizingUpload(t, ctx, pool, int64(len(raw)), "image/png")
		candidate := trackedCandidate{
			SHA256:                  validated.SHA256,
			ObjectKey:               "media/" + uploadID + "/" + hex.EncodeToString(validated.SHA256[:]),
			EncodedSize:             validated.SizeBytes,
			ValidationPolicyVersion: validationPolicyVersion,
			Format:                  validated.Format,
			Width:                   validated.Width,
			Height:                  validated.Height,
		}
		keys := []string{"staging/" + uploadID, candidate.ObjectKey}
		t.Cleanup(func() { cleanupUploadByIdempotencyKey(t, pool, idempotencyKey) })
		t.Cleanup(func() { deleteFinalizerObjects(t, cleanStorage, cfg.S3Bucket, &keys) })
		if withStaging {
			if err := putCandidateObject(ctx, cleanStorage, cfg.S3Bucket, keys[0], raw, "image/png"); err != nil {
				t.Fatal("put recovery staging object")
			}
		}
		return uploadID, candidate
	}
	claimAndTrack := func(t *testing.T, uploadID string, candidate trackedCandidate) finalizeClaim {
		t.Helper()
		claim, ok, err := claimNextFinalizing(ctx, pool, cfg.FinalizeClaimLease)
		if err != nil || !ok || claim.UploadID != uploadID {
			t.Fatalf("claim=%#v ok=%t error=%v", claim, ok, err)
		}
		active, mismatch, err := registerCandidate(ctx, pool, claim, candidate)
		if err != nil || !active || mismatch {
			t.Fatalf("register active=%t mismatch=%t error=%v", active, mismatch, err)
		}
		return claim
	}
	faultStorage := func(method, key string, action recoveryS3Action) (*s3.Client, *recoveryS3HTTPClient) {
		targetURL := &url.URL{Path: "/" + cfg.S3Bucket + "/" + key}
		faultHTTP := &recoveryS3HTTPClient{
			delegate:    realHTTP,
			method:      method,
			escapedPath: targetURL.EscapedPath(),
			action:      action,
		}
		options := cleanStorage.Options()
		options.HTTPClient = faultHTTP
		options.Retryer = aws.NopRetryer{}
		return s3.New(options), faultHTTP
	}
	runWorker := func(t *testing.T, storage *s3.Client) {
		t.Helper()
		worker := &finalizer{
			logger:     slog.New(slog.NewJSONHandler(io.Discard, nil)),
			pool:       pool,
			storage:    storage,
			bucket:     cfg.S3Bucket,
			claimLease: cfg.FinalizeClaimLease,
		}
		processed, err := worker.processNext(ctx)
		if err != nil || !processed {
			t.Fatalf("finalizer processed=%t error=%v", processed, err)
		}
	}
	assertCandidate := func(t *testing.T, candidate trackedCandidate, exists bool) {
		t.Helper()
		snapshot, err := captureS3Object(ctx, cleanStorage, cfg.S3Bucket, candidate.ObjectKey, maxUploadSizeBytes)
		if exists {
			if err != nil || !candidateMatchesSnapshot(candidate, snapshot) {
				t.Fatalf("clean candidate verification error=%v", err)
			}
			return
		}
		if err == nil || !storageObjectMissing(err) {
			t.Fatalf("clean client candidate absence error=%v", err)
		}
	}
	assertFinalizing := func(t *testing.T, uploadID string) {
		t.Helper()
		var state, finalKey string
		var candidates int
		if err := pool.QueryRow(ctx, `
			SELECT state,
			       COALESCE(final_key, ''),
			       (SELECT count(*) FROM upload_candidates WHERE upload_id = uploads.upload_id)
			FROM uploads
			WHERE upload_id = $1::uuid`, uploadID).Scan(&state, &finalKey, &candidates); err != nil {
			t.Fatal("read retriable finalize state")
		}
		if state != "finalizing" || finalKey != "" || candidates != 1 {
			t.Fatalf("false ready: state=%q final_key=%q candidates=%d", state, finalKey, candidates)
		}
	}
	assertReady := func(t *testing.T, database *pgxpool.Pool, uploadID string, candidate trackedCandidate) {
		t.Helper()
		var state, finalKey string
		var candidates int
		var selectedTombstoned, claimReleased bool
		if err := database.QueryRow(ctx, `
			SELECT state,
			       COALESCE(final_key, ''),
			       (SELECT count(*) FROM upload_candidates WHERE upload_id = uploads.upload_id),
			       EXISTS (SELECT 1 FROM cleanup_tombstones WHERE object_key = uploads.final_key),
			       claim_token IS NULL AND claim_expires_at IS NULL
			FROM uploads
			WHERE upload_id = $1::uuid`, uploadID).Scan(
			&state,
			&finalKey,
			&candidates,
			&selectedTombstoned,
			&claimReleased,
		); err != nil {
			t.Fatal("read recovered ready state")
		}
		if state != "ready" || finalKey != candidate.ObjectKey || candidates != 1 || selectedTombstoned || !claimReleased {
			t.Fatalf(
				"recovered state=%q final_key_match=%t candidates=%d selected_tombstoned=%t claim_released=%t",
				state,
				finalKey == candidate.ObjectKey,
				candidates,
				selectedTombstoned,
				claimReleased,
			)
		}
	}
	expireClaim := func(t *testing.T, claim finalizeClaim) {
		t.Helper()
		result, err := pool.Exec(ctx, `
			UPDATE uploads
			SET claim_expires_at = clock_timestamp() - interval '1 second'
			WHERE upload_id = $1::uuid
			  AND claim_token = $2::uuid`, claim.UploadID, claim.Token)
		if err != nil || result.RowsAffected() != 1 {
			t.Fatalf("expire abandoned claim rows=%d error=%v", result.RowsAffected(), err)
		}
	}
	forceDue := func(t *testing.T, uploadID string) {
		t.Helper()
		result, err := pool.Exec(ctx, `
			UPDATE uploads
			SET reconcile_after = clock_timestamp() - interval '1 second'
			WHERE upload_id = $1::uuid
			  AND state = 'finalizing'`, uploadID)
		if err != nil || result.RowsAffected() != 1 {
			t.Fatalf("force finalizer retry rows=%d error=%v", result.RowsAffected(), err)
		}
	}

	t.Run("fail_before", func(t *testing.T) {
		uploadID, candidate := setup(t, true)
		faultedStorage, fault := faultStorage(http.MethodPut, candidate.ObjectKey, recoveryFailBefore)
		runWorker(t, faultedStorage)
		if fault.hits != 1 || fault.delegated {
			t.Fatalf("fail-before hits=%d delegated=%t", fault.hits, fault.delegated)
		}
		assertCandidate(t, candidate, false)
		assertFinalizing(t, uploadID)
		forceDue(t, uploadID)
		runWorker(t, cleanStorage)
		assertCandidate(t, candidate, true)
		assertReady(t, pool, uploadID, candidate)
	})

	t.Run("delegate_then_fail", func(t *testing.T) {
		uploadID, candidate := setup(t, false)
		claim := claimAndTrack(t, uploadID, candidate)
		faultedStorage, fault := faultStorage(http.MethodPut, candidate.ObjectKey, recoveryDelegateThenFail)
		putErr := putCandidateObject(ctx, faultedStorage, cfg.S3Bucket, candidate.ObjectKey, raw, "image/png")
		if !errors.Is(putErr, errRecoveryS3Fault) || fault.hits != 1 || !fault.delegated {
			t.Fatalf("hidden PUT error=%v hits=%d delegated=%t", putErr, fault.hits, fault.delegated)
		}
		assertCandidate(t, candidate, true)
		assertFinalizing(t, uploadID)
		expireClaim(t, claim)
		forceDue(t, uploadID)
		runWorker(t, cleanStorage)
		assertReady(t, pool, uploadID, candidate)
	})

	t.Run("truncate_response_body", func(t *testing.T) {
		uploadID, candidate := setup(t, false)
		claim := claimAndTrack(t, uploadID, candidate)
		if err := putCandidateObject(ctx, cleanStorage, cfg.S3Bucket, candidate.ObjectKey, raw, "image/png"); err != nil {
			t.Fatal("put candidate before truncated GET")
		}
		assertCandidate(t, candidate, true)
		expireClaim(t, claim)
		forceDue(t, uploadID)
		faultedStorage, fault := faultStorage(http.MethodGet, candidate.ObjectKey, recoveryTruncateResponseBody)
		runWorker(t, faultedStorage)
		if fault.hits != 1 || !fault.delegated || !fault.bodyFaulted {
			t.Fatalf("truncated GET hits=%d delegated=%t body_faulted=%t", fault.hits, fault.delegated, fault.bodyFaulted)
		}
		assertFinalizing(t, uploadID)
		assertCandidate(t, candidate, true)
		forceDue(t, uploadID)
		runWorker(t, cleanStorage)
		assertReady(t, pool, uploadID, candidate)
	})

	t.Run("dropped_terminal_result", func(t *testing.T) {
		uploadID, candidate := setup(t, false)
		claim := claimAndTrack(t, uploadID, candidate)
		if err := putCandidateObject(ctx, cleanStorage, cfg.S3Bucket, candidate.ObjectKey, raw, "image/png"); err != nil {
			t.Fatal("put candidate before terminal commit")
		}
		assertCandidate(t, candidate, true)
		_, _ = transitionFinalizeReady(ctx, pool, claim, candidate)
		freshPool, err := openPostgres(ctx, cfg.DatabaseURL)
		if err != nil {
			t.Fatal("open fresh PostgreSQL actor")
		}
		defer freshPool.Close()
		assertReady(t, freshPool, uploadID, candidate)
	})
}
