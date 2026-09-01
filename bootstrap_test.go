package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var secretSentinels = []string{
	"postgres://sentinel-user:sentinel-password@database.invalid/media",
	"AKIA-SENTINEL-CREDENTIAL",
	"https://storage.invalid/object?X-Amz-Signature=sentinel",
	"claim-token-sentinel",
}

func TestOperationalEndpointsAndRedaction(t *testing.T) {
	var logs bytes.Buffer
	var postgresCalls atomic.Int64
	var s3Calls atomic.Int64
	var postgresDown atomic.Bool
	var s3Down atomic.Bool
	var postgresDeadline atomic.Bool
	var s3Deadline atomic.Bool
	app := &application{
		logger: slog.New(slog.NewJSONHandler(&logs, nil)),
		postgresPing: func(ctx context.Context) error {
			postgresCalls.Add(1)
			deadline, ok := ctx.Deadline()
			postgresDeadline.Store(ok && time.Until(deadline) > 0 && time.Until(deadline) <= readinessCheckTimeout)
			if postgresDown.Load() {
				return errors.New(strings.Join(secretSentinels, " "))
			}
			return nil
		},
		s3HeadBucket: func(ctx context.Context) error {
			s3Calls.Add(1)
			deadline, ok := ctx.Deadline()
			s3Deadline.Store(ok && time.Until(deadline) > 0 && time.Until(deadline) <= readinessCheckTimeout)
			if s3Down.Load() {
				return errors.New(strings.Join(secretSentinels, " "))
			}
			return nil
		},
	}
	request := func(method, target string) *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		app.ServeHTTP(recorder, httptest.NewRequest(method, target, nil))
		if recorder.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("%s %s omitted no-store", method, target)
		}
		if !strings.HasPrefix(recorder.Header().Get("Content-Type"), "application/json") {
			t.Errorf("%s %s did not return JSON", method, target)
		}
		return recorder
	}

	live := request(http.MethodGet, "/livez")
	if live.Code != http.StatusOK || postgresCalls.Load() != 0 || s3Calls.Load() != 0 {
		t.Fatal("liveness called a dependency or returned the wrong status")
	}
	debug := request(http.MethodGet, "/debug/vars")
	if debug.Code != http.StatusOK || !strings.Contains(debug.Body.String(), `"media_upload_service"`) {
		t.Fatal("debug vars omitted the fixed service subtree")
	}
	if postgresCalls.Load() != 0 || s3Calls.Load() != 0 {
		t.Fatal("debug vars called a dependency")
	}

	ready := request(http.MethodGet, "/readyz")
	if ready.Code != http.StatusOK || postgresCalls.Load() != 1 || s3Calls.Load() != 1 ||
		!postgresDeadline.Load() || !s3Deadline.Load() {
		t.Fatal("healthy readiness did not check both dependencies")
	}
	postgresDown.Store(true)
	if recorder := request(http.MethodGet, "/readyz"); recorder.Code != http.StatusServiceUnavailable {
		t.Fatal("postgres outage did not fail readiness")
	}
	postgresDown.Store(false)
	s3Down.Store(true)
	if recorder := request(http.MethodGet, "/readyz"); recorder.Code != http.StatusServiceUnavailable {
		t.Fatal("S3 outage did not fail readiness")
	}
	s3Down.Store(false)
	if recorder := request(http.MethodGet, "/readyz"); recorder.Code != http.StatusOK {
		t.Fatal("readiness did not recover without replacing the application")
	}

	wrongMethod := request(http.MethodHead, "/livez")
	if wrongMethod.Code != http.StatusMethodNotAllowed || wrongMethod.Header().Get("Allow") != http.MethodGet {
		t.Fatal("wrong method did not return exact Allow")
	}
	unknown := request(http.MethodGet, "/"+secretSentinels[1]+"?X-Amz-Signature=sentinel")
	if unknown.Code != http.StatusNotFound {
		t.Fatal("unknown route did not return 404")
	}
	if recorder := request(secretSentinels[3], "/livez"); recorder.Code != http.StatusMethodNotAllowed {
		t.Fatal("unknown method did not return 405")
	}
	app.stopping.Store(true)
	if recorder := request(http.MethodGet, "/livez"); recorder.Code != http.StatusServiceUnavailable {
		t.Fatal("liveness stayed successful after shutdown started")
	}

	combined := logs.String() + live.Body.String() + debug.Body.String() + ready.Body.String() + unknown.Body.String()
	for _, sentinel := range append(secretSentinels, "X-Amz-") {
		if strings.Contains(combined, sentinel) {
			t.Fatalf("logs or responses exposed sentinel %d", len(sentinel))
		}
	}
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		if !json.Valid([]byte(line)) {
			t.Fatalf("log line is not JSON: %q", line)
		}
	}
}

func TestE2EHTTPHeaderBoundary(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	app := &application{
		logger:       logger,
		postgresPing: func(context.Context) error { return nil },
		s3HeadBucket: func(context.Context) error { return nil },
	}
	var handlerCalls atomic.Int64
	handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		handlerCalls.Add(1)
		app.ServeHTTP(writer, request)
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := newHTTPServer(listener.Addr().String(), handler)
	if server.ErrorLog == nil || server.ErrorLog.Writer() != io.Discard {
		t.Fatal("HTTP server raw error log is not disabled")
	}
	serveResult := make(chan error, 1)
	go func() { serveResult <- server.Serve(listener) }()
	defer func() {
		_ = server.Close()
		<-serveResult
	}()

	send := func(total int) int {
		prefix := "GET /livez HTTP/1.1\r\nHost: localhost\r\nX-Pad: "
		suffix := "\r\nConnection: close\r\n\r\n"
		padding := total - len(prefix) - len(suffix)
		if padding < 0 {
			t.Fatal("request target length is too small")
		}
		raw := prefix + strings.Repeat("a", padding) + suffix
		if len(raw) != total {
			t.Fatalf("raw request has %d bytes, want %d", len(raw), total)
		}
		connection, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer connection.Close()
		_ = connection.SetDeadline(time.Now().Add(2 * time.Second))
		if _, err := io.WriteString(connection, raw); err != nil {
			t.Fatalf("write raw request: %v", err)
		}
		response, err := http.ReadResponse(bufio.NewReader(connection), &http.Request{Method: http.MethodGet})
		if err != nil {
			t.Fatalf("read raw response: %v", err)
		}
		defer response.Body.Close()
		_, _ = io.Copy(io.Discard, response.Body)
		return response.StatusCode
	}

	if status := send(16384); status != http.StatusOK {
		t.Fatalf("16,384-byte request returned %d", status)
	}
	if handlerCalls.Load() != 1 {
		t.Fatal("16,384-byte request did not reach the handler")
	}
	if status := send(16385); status != http.StatusRequestHeaderFieldsTooLarge {
		t.Fatalf("16,385-byte request returned %d", status)
	}
	if handlerCalls.Load() != 1 {
		t.Fatal("parser rejection reached the handler")
	}
}

func TestMigrationIsExternallyTransactional(t *testing.T) {
	source, err := os.ReadFile("migrations/0001_initial.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	upper := strings.ToUpper(string(source))
	for _, forbidden := range []string{"BEGIN;", "COMMIT;"} {
		if strings.Contains(upper, forbidden) {
			t.Fatalf("migration contains %s", forbidden)
		}
	}
	for _, index := range []string{
		"uploads_pending_due_idx",
		"uploads_finalizing_due_idx",
		"cleanup_tombstones_due_idx",
	} {
		if strings.Count(string(source), index) != 1 {
			t.Fatalf("migration does not define %s exactly once", index)
		}
	}
}

func TestSchemaVersionValidation(t *testing.T) {
	for _, test := range []struct {
		count  int
		latest int64
		valid  bool
	}{
		{1, 1, true},
		{0, 0, false},
		{2, 2, false},
		{2, 1, false},
	} {
		err := validateSchemaV1(test.count, test.latest)
		if (err == nil) != test.valid {
			t.Fatalf("count=%d latest=%d valid=%v", test.count, test.latest, test.valid)
		}
	}
}

func TestRunRejectsInvalidConfig(t *testing.T) {
	t.Setenv("HTTP_ADDR", "0.0.0.0:8080")
	t.Setenv("DATABASE_URL", secretSentinels[0])
	t.Setenv("S3_BUCKET", "media-upload")
	t.Setenv("AWS_REGION", "garage")
	t.Setenv("S3_ENDPOINT", "http://127.0.0.1:3900")
	var logs bytes.Buffer
	err := run(context.Background(), slog.New(slog.NewJSONHandler(&logs, nil)))
	if err == nil {
		t.Fatal("service accepted invalid startup config")
	}
	combined := err.Error() + logs.String()
	for _, sentinel := range secretSentinels {
		if strings.Contains(combined, sentinel) {
			t.Fatalf("startup failure exposed sentinel %d", len(sentinel))
		}
	}
}

func TestIntegrationOperationalReadiness(t *testing.T) {
	if testing.Short() {
		t.Skip("requires migrated PostgreSQL and Garage")
	}
	cfg := integrationConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := openPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		t.Fatal("open migrated PostgreSQL")
	}
	defer pool.Close()
	storage, err := newS3Client(ctx, cfg)
	if err != nil {
		t.Fatal("create S3 client")
	}
	var logs bytes.Buffer
	app := &application{
		logger:       slog.New(slog.NewJSONHandler(&logs, nil)),
		postgresPing: pool.Ping,
		s3HeadBucket: func(ctx context.Context) error {
			return headBucket(ctx, storage, cfg.S3Bucket)
		},
	}
	server := httptest.NewServer(app)
	defer server.Close()
	for _, path := range []string{"/livez", "/readyz", "/debug/vars"} {
		response, err := server.Client().Get(server.URL + path)
		if err != nil {
			t.Fatalf("GET %s failed", path)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("GET %s returned %d: %s", path, response.StatusCode, body)
		}
		if response.Header.Get("Cache-Control") != "no-store" ||
			!strings.HasPrefix(response.Header.Get("Content-Type"), "application/json") ||
			!json.Valid(body) {
			t.Fatalf("GET %s did not return no-store JSON", path)
		}
	}
}

func TestIntegrationServiceLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("requires migrated PostgreSQL and Garage")
	}
	cfg := integrationConfig(t)
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("reserve loopback address")
	}
	address := probe.Addr().String()
	probe.Close()
	t.Setenv("DATABASE_URL", cfg.DatabaseURL)
	t.Setenv("HTTP_ADDR", address)

	ctx, cancel := context.WithCancel(context.Background())
	var logs bytes.Buffer
	result := make(chan error, 1)
	go func() {
		result <- run(ctx, slog.New(slog.NewJSONHandler(&logs, nil)))
	}()
	client := &http.Client{Timeout: 5 * time.Second}
	deadline := time.Now().Add(10 * time.Second)
	for {
		select {
		case err := <-result:
			t.Fatalf("service stopped before readiness: %v", err)
		default:
		}
		response, err := client.Get("http://" + address + "/readyz")
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			cancel()
			<-result
			t.Fatal("service did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal("service shutdown failed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("service shutdown exceeded test budget")
	}
	for _, event := range []string{"service.started", "service.stopping", "service.stopped"} {
		if !strings.Contains(logs.String(), `"msg":"`+event+`"`) {
			t.Fatalf("missing %s log", event)
		}
	}
}

func TestPostgresSchemaV1(t *testing.T) {
	if testing.Short() {
		t.Skip("requires migrated PostgreSQL")
	}
	cfg := integrationConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := openPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		t.Fatal("open migrated PostgreSQL")
	}
	defer pool.Close()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal("begin schema test transaction")
	}
	defer tx.Rollback(context.Background())

	var versions int
	var latest int64
	if err := tx.QueryRow(ctx, "SELECT count(*), max(version) FROM schema_migrations").Scan(&versions, &latest); err != nil {
		t.Fatal("read schema ledger")
	}
	if versions != 1 || latest != 1 {
		t.Fatalf("schema ledger is count=%d latest=%d", versions, latest)
	}

	expectedConstraints := []string{
		"uploads_pkey",
		"uploads_idempotency_key_key",
		"uploads_staging_key_check",
		"uploads_declared_size_check",
		"uploads_declared_content_type_check",
		"uploads_state_check",
		"uploads_upload_deadline_check",
		"uploads_write_horizon_check",
		"uploads_completion_deadline_check",
		"uploads_reconcile_failure_check",
		"uploads_claim_pair_check",
		"uploads_claim_state_check",
		"uploads_final_key_state_check",
		"uploads_rejection_phase_check",
		"uploads_rejection_reason_check",
		"uploads_state_shape_check",
		"uploads_final_candidate_fkey",
		"upload_candidates_pkey",
		"upload_candidates_upload_id_object_key_key",
		"upload_candidates_upload_id_fkey",
		"upload_candidates_sha256_check",
		"upload_candidates_object_key_check",
		"upload_candidates_encoded_size_check",
		"upload_candidates_policy_check",
		"upload_candidates_image_format_check",
		"upload_candidates_dimensions_check",
		"upload_candidates_pixels_check",
		"cleanup_tombstones_pkey",
		"cleanup_tombstones_upload_id_fkey",
		"cleanup_tombstones_candidate_fkey",
		"cleanup_tombstones_object_shape_check",
		"cleanup_tombstones_due_check",
		"cleanup_tombstones_reservation_pair_check",
		"cleanup_tombstones_attempt_pair_check",
		"cleanup_tombstones_attempt_shape_check",
	}
	expectedConstraintDefinitions := map[string][]string{
		"uploads_pkey":                               {"PRIMARYKEY(upload_id)"},
		"uploads_idempotency_key_key":                {"UNIQUE(idempotency_key)"},
		"uploads_staging_key_check":                  {"staging_key", "'staging/'", "upload_id::text"},
		"uploads_declared_size_check":                {"declared_size", "10485760"},
		"uploads_declared_content_type_check":        {"declared_content_type", "'image/jpeg'", "'image/png'"},
		"uploads_state_check":                        {"state", "'pending'", "'finalizing'", "'ready'", "'rejected'", "'expired'"},
		"uploads_upload_deadline_check":              {"upload_deadline", "created_at", "24:00:00"},
		"uploads_write_horizon_check":                {"max_write_expires_at", "created_at", "upload_deadline", "00:15:00"},
		"uploads_completion_deadline_check":          {"completion_requested_at", "upload_deadline"},
		"uploads_reconcile_failure_check":            {"reconcile_failure_streak", "reconcile_last_failure_class", "reconcile_last_failure_at", "'transient'", "'other_deterministic'"},
		"uploads_claim_pair_check":                   {"claim_token", "claim_expires_at"},
		"uploads_claim_state_check":                  {"claim_token", "state", "'finalizing'"},
		"uploads_final_key_state_check":              {"final_key", "state", "'ready'"},
		"uploads_rejection_phase_check":              {"rejection_phase", "'staging_read'", "'decode_config'", "'decode'", "'candidate_verify'"},
		"uploads_rejection_reason_check":             {"rejection_class", "rejection_reason", "'invalid_input'", "'malformed_image'", "'internal_invariant'", "'candidate_integrity_mismatch'"},
		"uploads_state_shape_check":                  {"state", "'pending'", "'finalizing'", "'ready'", "'rejected'", "'expired'", "rejection_policy_version", "rejection_evidence", "expiry_reason"},
		"uploads_final_candidate_fkey":               {"FOREIGNKEY(upload_id,final_key)", "REFERENCESupload_candidates(upload_id,object_key)", "ONDELETERESTRICT"},
		"upload_candidates_pkey":                     {"PRIMARYKEY(upload_id,sha256)"},
		"upload_candidates_upload_id_object_key_key": {"UNIQUE(upload_id,object_key)"},
		"upload_candidates_upload_id_fkey":           {"FOREIGNKEY(upload_id)", "REFERENCESuploads(upload_id)", "ONDELETERESTRICT"},
		"upload_candidates_sha256_check":             {"sha256", "octet_length", "32"},
		"upload_candidates_object_key_check":         {"object_key", "'media/'", "upload_id::text", "encode(sha256,", "'hex'"},
		"upload_candidates_encoded_size_check":       {"encoded_size", "10485760"},
		"upload_candidates_policy_check":             {"validation_policy_version", "1"},
		"upload_candidates_image_format_check":       {"image_format", "'jpeg'", "'png'"},
		"upload_candidates_dimensions_check":         {"width", "height", "8192"},
		"upload_candidates_pixels_check":             {"width", "height", "8388608"},
		"cleanup_tombstones_pkey":                    {"PRIMARYKEY(object_key)"},
		"cleanup_tombstones_upload_id_fkey":          {"FOREIGNKEY(upload_id)", "REFERENCESuploads(upload_id)", "ONDELETERESTRICT"},
		"cleanup_tombstones_candidate_fkey":          {"FOREIGNKEY(upload_id,candidate_sha256)", "REFERENCESupload_candidates(upload_id,sha256)", "ONDELETERESTRICT"},
		"cleanup_tombstones_object_shape_check":      {"object_key", "candidate_sha256", "'staging/'", "'media/'", "upload_id::text"},
		"cleanup_tombstones_due_check":               {"next_attempt_at", "delete_not_before"},
		"cleanup_tombstones_reservation_pair_check":  {"reservation_token", "reservation_expires_at"},
		"cleanup_tombstones_attempt_pair_check":      {"last_attempt_at", "last_result"},
		"cleanup_tombstones_attempt_shape_check":     {"last_result", "failure_streak", "'delete_succeeded'", "'confirmed_absent'", "'transient_error'", "'other_deterministic_error'"},
	}
	rows, err := tx.Query(ctx, `
		SELECT conname, contype::text, condeferrable, convalidated,
		       pg_get_constraintdef(oid, true)
		FROM pg_constraint
		WHERE conrelid IN (
			'uploads'::regclass,
			'upload_candidates'::regclass,
			'cleanup_tombstones'::regclass
		)
		  AND contype IN ('c', 'f', 'p', 'u')
	`)
	if err != nil {
		t.Fatal("read schema constraints")
	}
	type constraintInfo struct {
		kind       string
		deferrable bool
		validated  bool
		definition string
	}
	constraints := make(map[string]constraintInfo)
	for rows.Next() {
		var name string
		var info constraintInfo
		if err := rows.Scan(
			&name,
			&info.kind,
			&info.deferrable,
			&info.validated,
			&info.definition,
		); err != nil {
			t.Fatal("scan constraint")
		}
		constraints[name] = info
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal("iterate constraints")
	}
	if len(constraints) != len(expectedConstraints) {
		t.Fatalf("found %d constraints, want %d", len(constraints), len(expectedConstraints))
	}
	for _, name := range expectedConstraints {
		info, found := constraints[name]
		if !found {
			t.Errorf("missing constraint %s", name)
			continue
		}
		wantKind := "c"
		switch {
		case strings.HasSuffix(name, "_pkey"):
			wantKind = "p"
		case strings.HasSuffix(name, "_fkey"):
			wantKind = "f"
		case strings.HasSuffix(name, "_key"):
			wantKind = "u"
		}
		if info.kind != wantKind {
			t.Errorf("constraint %s has type %s, want %s", name, info.kind, wantKind)
		}
		if info.deferrable {
			t.Errorf("constraint %s is deferrable", name)
		}
		if !info.validated {
			t.Errorf("constraint %s is not validated", name)
		}
		definition := strings.Join(strings.Fields(info.definition), "")
		for _, fragment := range expectedConstraintDefinitions[name] {
			if !strings.Contains(definition, fragment) {
				t.Errorf("constraint %s omitted %s", name, fragment)
			}
		}
	}

	indexRows, err := tx.Query(ctx, `
		SELECT indexname, indexdef
		FROM pg_indexes
		WHERE schemaname = current_schema()
		  AND tablename IN ('uploads', 'cleanup_tombstones')
		  AND indexname LIKE '%_due_idx'
	`)
	if err != nil {
		t.Fatal("read due indexes")
	}
	dueIndexes := make(map[string]string)
	for indexRows.Next() {
		var name string
		var definition string
		if err := indexRows.Scan(&name, &definition); err != nil {
			t.Fatal("scan due index")
		}
		dueIndexes[name] = definition
	}
	indexRows.Close()
	if len(dueIndexes) != 3 ||
		dueIndexes["uploads_pending_due_idx"] == "" ||
		dueIndexes["uploads_finalizing_due_idx"] == "" ||
		dueIndexes["cleanup_tombstones_due_idx"] == "" {
		t.Fatalf("unexpected due indexes: %v", dueIndexes)
	}
	for name, fragments := range map[string][]string{
		"uploads_pending_due_idx": {
			"(upload_deadline, upload_id)",
			"WHERE (state = 'pending'::text)",
		},
		"uploads_finalizing_due_idx": {
			"(reconcile_after, upload_id)",
			"WHERE (state = 'finalizing'::text)",
		},
		"cleanup_tombstones_due_idx": {"(next_attempt_at, object_key)"},
	} {
		for _, fragment := range fragments {
			if !strings.Contains(dueIndexes[name], fragment) {
				t.Errorf("index %s omitted %s", name, fragment)
			}
		}
	}

	expectConstraintError(t, ctx, tx, "uploads_declared_size_check", `
		WITH timestamp AS (SELECT clock_timestamp() AS created_at)
		INSERT INTO uploads (
			upload_id, idempotency_key, staging_key, declared_size,
			declared_content_type, state, created_at, upload_deadline,
			max_write_expires_at
		)
		SELECT
			'10000000-0000-4000-8000-000000000001',
			'20000000-0000-4000-8000-000000000001',
			'staging/10000000-0000-4000-8000-000000000001',
			0, 'image/png', 'pending', created_at,
			created_at + interval '24 hours', created_at + interval '15 minutes'
		FROM timestamp
	`)
	expectConstraintError(t, ctx, tx, "uploads_reconcile_failure_check", `
		WITH timestamp AS (SELECT clock_timestamp() AS created_at)
		INSERT INTO uploads (
			upload_id, idempotency_key, staging_key, declared_size,
			declared_content_type, state, created_at, upload_deadline,
			max_write_expires_at, completion_requested_at, reconcile_after,
			reconcile_failure_streak, reconcile_last_failure_at
		)
		SELECT
			'10000000-0000-4000-8000-000000000003',
			'20000000-0000-4000-8000-000000000003',
			'staging/10000000-0000-4000-8000-000000000003',
			1, 'image/png', 'finalizing', created_at,
			created_at + interval '24 hours', created_at + interval '15 minutes',
			created_at + interval '1 hour', created_at + interval '2 hours',
			1, created_at + interval '2 hours'
		FROM timestamp
	`)
	expectConstraintError(t, ctx, tx, "uploads_state_shape_check", `
		WITH timestamp AS (SELECT clock_timestamp() AS created_at)
		INSERT INTO uploads (
			upload_id, idempotency_key, staging_key, declared_size,
			declared_content_type, state, created_at, upload_deadline,
			max_write_expires_at, completion_requested_at, terminal_at,
			rejection_class, rejection_reason, rejection_phase,
			rejection_evidence
		)
		SELECT
			'10000000-0000-4000-8000-000000000004',
			'20000000-0000-4000-8000-000000000004',
			'staging/10000000-0000-4000-8000-000000000004',
			1, 'image/png', 'rejected', created_at,
			created_at + interval '24 hours', created_at + interval '15 minutes',
			created_at + interval '1 hour', created_at + interval '2 hours',
			'invalid_input', 'malformed_image', 'decode', '{"reason":"invalid"}'::jsonb
		FROM timestamp
	`)
	expectConstraintError(t, ctx, tx, "uploads_state_shape_check", `
		WITH timestamp AS (SELECT clock_timestamp() AS created_at)
		INSERT INTO uploads (
			upload_id, idempotency_key, staging_key, declared_size,
			declared_content_type, state, created_at, upload_deadline,
			max_write_expires_at, completion_requested_at, terminal_at,
			rejection_class, rejection_reason, rejection_policy_version,
			rejection_phase
		)
		SELECT
			'10000000-0000-4000-8000-000000000005',
			'20000000-0000-4000-8000-000000000005',
			'staging/10000000-0000-4000-8000-000000000005',
			1, 'image/png', 'rejected', created_at,
			created_at + interval '24 hours', created_at + interval '15 minutes',
			created_at + interval '1 hour', created_at + interval '2 hours',
			'invalid_input', 'malformed_image', 1, 'decode'
		FROM timestamp
	`)
	expectConstraintError(t, ctx, tx, "uploads_state_shape_check", `
		WITH timestamp AS (SELECT clock_timestamp() AS created_at)
		INSERT INTO uploads (
			upload_id, idempotency_key, staging_key, declared_size,
			declared_content_type, state, created_at, upload_deadline,
			max_write_expires_at, terminal_at
		)
		SELECT
			'10000000-0000-4000-8000-000000000006',
			'20000000-0000-4000-8000-000000000006',
			'staging/10000000-0000-4000-8000-000000000006',
			1, 'image/png', 'expired', created_at,
			created_at + interval '24 hours', created_at + interval '15 minutes',
			created_at + interval '25 hours'
		FROM timestamp
	`)

	uploadID := "10000000-0000-4000-8000-000000000002"
	_, err = tx.Exec(ctx, `
		WITH timestamp AS (SELECT clock_timestamp() AS created_at)
		INSERT INTO uploads (
			upload_id, idempotency_key, staging_key, declared_size,
			declared_content_type, state, created_at, upload_deadline,
			max_write_expires_at
		)
		SELECT
			$1::uuid, '20000000-0000-4000-8000-000000000002', 'staging/' || $1::text,
			1, 'image/png', 'pending', created_at,
			created_at + interval '24 hours', created_at + interval '15 minutes'
		FROM timestamp
	`, uploadID)
	if err != nil {
		t.Fatal("insert valid upload")
	}
	expectConstraintError(t, ctx, tx, "cleanup_tombstones_attempt_shape_check", `
		WITH timestamp AS (SELECT clock_timestamp() AS attempted_at)
		INSERT INTO cleanup_tombstones (
			object_key, upload_id, created_at, delete_not_before,
			next_attempt_at, failure_streak
		)
		SELECT
			'staging/' || $1::text, $1::uuid, attempted_at, attempted_at,
			attempted_at, 1
		FROM timestamp
	`, uploadID)
	missingKey := "media/" + uploadID + "/" + strings.Repeat("1", 64)
	expectConstraintError(t, ctx, tx, "uploads_final_candidate_fkey", `
		UPDATE uploads
		SET state = 'ready',
			completion_requested_at = created_at + interval '1 hour',
			terminal_at = created_at + interval '2 hours',
			final_key = $2
		WHERE upload_id = $1
	`, uploadID, missingKey)

	digest := strings.Repeat("0", 64)
	candidateKey := "media/" + uploadID + "/" + digest
	_, err = tx.Exec(ctx, `
		INSERT INTO upload_candidates (
			upload_id, sha256, object_key, encoded_size,
			validation_policy_version, image_format, width, height, registered_at
		)
		VALUES ($1, decode($2, 'hex'), $3, 1, 1, 'png', 1, 1, clock_timestamp())
	`, uploadID, digest, candidateKey)
	if err != nil {
		t.Fatal("insert valid candidate")
	}
	result, err := tx.Exec(ctx, `
		UPDATE uploads
		SET state = 'ready',
			completion_requested_at = created_at + interval '1 hour',
			terminal_at = created_at + interval '2 hours',
			final_key = $2
		WHERE upload_id = $1
	`, uploadID, candidateKey)
	if err != nil {
		t.Fatal("select tracked candidate")
	}
	if result.RowsAffected() != 1 {
		t.Fatal("tracked candidate was not selected")
	}
}

func integrationConfig(t *testing.T) config {
	t.Helper()
	if os.Getenv("TEST_DATABASE_URL") == "" {
		t.Fatal("TEST_DATABASE_URL is required for full integration tests")
	}
	cfg, err := loadConfig()
	if err != nil {
		t.Fatal("integration config is invalid")
	}
	cfg.DatabaseURL = os.Getenv("TEST_DATABASE_URL")
	return cfg
}

func expectConstraintError(
	t *testing.T,
	ctx context.Context,
	tx pgx.Tx,
	wantConstraint string,
	query string,
	arguments ...any,
) {
	t.Helper()
	if _, err := tx.Exec(ctx, "SAVEPOINT expected_constraint_error"); err != nil {
		t.Fatal("create constraint savepoint")
	}
	_, executionError := tx.Exec(ctx, query, arguments...)
	if _, err := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT expected_constraint_error"); err != nil {
		t.Fatal("rollback constraint savepoint")
	}
	if _, err := tx.Exec(ctx, "RELEASE SAVEPOINT expected_constraint_error"); err != nil {
		t.Fatal("release constraint savepoint")
	}
	if executionError == nil {
		t.Fatal("invalid schema mutation was accepted")
	}
	var postgresError *pgconn.PgError
	if !errors.As(executionError, &postgresError) || postgresError.ConstraintName != wantConstraint {
		t.Fatalf("mutation failed outside %s", wantConstraint)
	}
}
