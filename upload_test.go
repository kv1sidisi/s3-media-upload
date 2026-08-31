package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"expvar"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	testIdempotencyKey      = "01234567-89ab-4def-8123-456789abcdef"
	testOtherIdempotencyKey = "11234567-89ab-4def-8123-456789abcdef"
	testUploadID            = "21234567-89ab-4def-8123-456789abcdef"
)

func TestCreateUploadRequestContract(t *testing.T) {
	validBody := `{"size_bytes":1,"content_type":"image/png"}`
	body16384 := validBody + strings.Repeat(" ", 16384-len(validBody))
	body16385 := validBody + strings.Repeat(" ", 16385-len(validBody))

	tests := []struct {
		name         string
		keys         []string
		contentTypes []string
		body         string
		chunked      bool
		wantStatus   int
		wantCode     string
		wantCall     bool
		wantSize     int64
		wantType     string
	}{
		{"jpeg", []string{testIdempotencyKey}, []string{"application/json"}, `{"size_bytes":1,"content_type":"image/jpeg"}`, false, http.StatusCreated, "", true, 1, "image/jpeg"},
		{"png maximum and reordered JSON", []string{testIdempotencyKey}, []string{"application/json"}, ` { "content_type" : "image/png", "size_bytes" : 10485760 } `, false, http.StatusCreated, "", true, 10485760, "image/png"},
		{"body exactly 16384", []string{testIdempotencyKey}, []string{"application/json"}, body16384, false, http.StatusCreated, "", true, 1, "image/png"},
		{"chunked body exactly 16384", []string{testIdempotencyKey}, []string{"application/json"}, body16384, true, http.StatusCreated, "", true, 1, "image/png"},
		{"missing idempotency key", nil, []string{"application/json"}, validBody, false, http.StatusBadRequest, "invalid_idempotency_key", false, 0, ""},
		{"repeated idempotency key", []string{testIdempotencyKey, testOtherIdempotencyKey}, []string{"application/json"}, validBody, false, http.StatusBadRequest, "invalid_idempotency_key", false, 0, ""},
		{"comma joined idempotency key", []string{testIdempotencyKey + "," + testOtherIdempotencyKey}, []string{"application/json"}, validBody, false, http.StatusBadRequest, "invalid_idempotency_key", false, 0, ""},
		{"quoted idempotency key", []string{`"` + testIdempotencyKey + `"`}, []string{"application/json"}, validBody, false, http.StatusBadRequest, "invalid_idempotency_key", false, 0, ""},
		{"uppercase idempotency key", []string{strings.ToUpper(testIdempotencyKey)}, []string{"application/json"}, validBody, false, http.StatusBadRequest, "invalid_idempotency_key", false, 0, ""},
		{"non-v4 idempotency key", []string{"01234567-89ab-1def-8123-456789abcdef"}, []string{"application/json"}, validBody, false, http.StatusBadRequest, "invalid_idempotency_key", false, 0, ""},
		{"non-RFC variant idempotency key", []string{"01234567-89ab-4def-7123-456789abcdef"}, []string{"application/json"}, validBody, false, http.StatusBadRequest, "invalid_idempotency_key", false, 0, ""},
		{"spaced idempotency key", []string{" " + testIdempotencyKey}, []string{"application/json"}, validBody, false, http.StatusBadRequest, "invalid_idempotency_key", false, 0, ""},
		{"missing content type", []string{testIdempotencyKey}, nil, validBody, false, http.StatusUnsupportedMediaType, "unsupported_media_type", false, 0, ""},
		{"parameterized content type", []string{testIdempotencyKey}, []string{"application/json; charset=utf-8"}, validBody, false, http.StatusUnsupportedMediaType, "unsupported_media_type", false, 0, ""},
		{"case changed content type", []string{testIdempotencyKey}, []string{"Application/JSON"}, validBody, false, http.StatusUnsupportedMediaType, "unsupported_media_type", false, 0, ""},
		{"repeated content type", []string{testIdempotencyKey}, []string{"application/json", "application/json"}, validBody, false, http.StatusUnsupportedMediaType, "unsupported_media_type", false, 0, ""},
		{"malformed JSON", []string{testIdempotencyKey}, []string{"application/json"}, `{`, false, http.StatusBadRequest, "invalid_request", false, 0, ""},
		{"unknown JSON field", []string{testIdempotencyKey}, []string{"application/json"}, `{"size_bytes":1,"content_type":"image/png","filename":"secret.png"}`, false, http.StatusBadRequest, "invalid_request", false, 0, ""},
		{"missing size", []string{testIdempotencyKey}, []string{"application/json"}, `{"content_type":"image/png"}`, false, http.StatusBadRequest, "invalid_request", false, 0, ""},
		{"missing content type field", []string{testIdempotencyKey}, []string{"application/json"}, `{"size_bytes":1}`, false, http.StatusBadRequest, "invalid_request", false, 0, ""},
		{"duplicate size field", []string{testIdempotencyKey}, []string{"application/json"}, `{"size_bytes":1,"size_bytes":2,"content_type":"image/png"}`, false, http.StatusBadRequest, "invalid_request", false, 0, ""},
		{"duplicate content type field", []string{testIdempotencyKey}, []string{"application/json"}, `{"size_bytes":1,"content_type":"image/png","content_type":"image/jpeg"}`, false, http.StatusBadRequest, "invalid_request", false, 0, ""},
		{"wrong size type", []string{testIdempotencyKey}, []string{"application/json"}, `{"size_bytes":"1","content_type":"image/png"}`, false, http.StatusBadRequest, "invalid_request", false, 0, ""},
		{"fractional size", []string{testIdempotencyKey}, []string{"application/json"}, `{"size_bytes":1.0,"content_type":"image/png"}`, false, http.StatusBadRequest, "invalid_request", false, 0, ""},
		{"wrong content type type", []string{testIdempotencyKey}, []string{"application/json"}, `{"size_bytes":1,"content_type":7}`, false, http.StatusBadRequest, "invalid_request", false, 0, ""},
		{"null body", []string{testIdempotencyKey}, []string{"application/json"}, `null`, false, http.StatusBadRequest, "invalid_request", false, 0, ""},
		{"array body", []string{testIdempotencyKey}, []string{"application/json"}, `[]`, false, http.StatusBadRequest, "invalid_request", false, 0, ""},
		{"trailing JSON", []string{testIdempotencyKey}, []string{"application/json"}, validBody + `{}`, false, http.StatusBadRequest, "invalid_request", false, 0, ""},
		{"empty body", []string{testIdempotencyKey}, []string{"application/json"}, ``, false, http.StatusBadRequest, "invalid_request", false, 0, ""},
		{"zero size", []string{testIdempotencyKey}, []string{"application/json"}, `{"size_bytes":0,"content_type":"image/png"}`, false, http.StatusUnprocessableEntity, "invalid_upload_declaration", false, 0, ""},
		{"negative size", []string{testIdempotencyKey}, []string{"application/json"}, `{"size_bytes":-1,"content_type":"image/png"}`, false, http.StatusUnprocessableEntity, "invalid_upload_declaration", false, 0, ""},
		{"size above maximum", []string{testIdempotencyKey}, []string{"application/json"}, `{"size_bytes":10485761,"content_type":"image/png"}`, false, http.StatusUnprocessableEntity, "invalid_upload_declaration", false, 0, ""},
		{"unsupported declaration type", []string{testIdempotencyKey}, []string{"application/json"}, `{"size_bytes":1,"content_type":"image/gif"}`, false, http.StatusUnprocessableEntity, "invalid_upload_declaration", false, 0, ""},
		{"parameterized declaration type", []string{testIdempotencyKey}, []string{"application/json"}, `{"size_bytes":1,"content_type":"image/png; charset=binary"}`, false, http.StatusUnprocessableEntity, "invalid_upload_declaration", false, 0, ""},
		{"case changed declaration type", []string{testIdempotencyKey}, []string{"application/json"}, `{"size_bytes":1,"content_type":"IMAGE/PNG"}`, false, http.StatusUnprocessableEntity, "invalid_upload_declaration", false, 0, ""},
		{"body byte 16385", []string{testIdempotencyKey}, []string{"application/json"}, body16385, false, http.StatusRequestEntityTooLarge, "request_too_large", false, 0, ""},
		{"chunked body byte 16385", []string{testIdempotencyKey}, []string{"application/json"}, body16385, true, http.StatusRequestEntityTooLarge, "request_too_large", false, 0, ""},
	}

	deadline := time.Date(2026, time.August, 31, 12, 0, 0, 0, time.UTC)
	expiresAt := deadline.Add(-23*time.Hour - 45*time.Minute)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int64
			app := &application{
				logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
				createUpload: func(_ context.Context, command createUploadCommand) (createUploadResult, error) {
					calls.Add(1)
					if command.IdempotencyKey != testIdempotencyKey || command.SizeBytes != test.wantSize || command.ContentType != test.wantType {
						t.Fatalf("unexpected normalized command: %#v", command)
					}
					return createUploadResult{Created: true, Upload: uploadRepresentation{
						UploadID:            testUploadID,
						State:               "pending",
						DeclaredSizeBytes:   command.SizeBytes,
						DeclaredContentType: command.ContentType,
						UploadDeadline:      deadline,
						UploadRequest: &uploadRequest{
							Method:    http.MethodPut,
							URL:       "http://storage.invalid/bucket/staging/id?X-Amz-Signature=opaque",
							Headers:   map[string]string{"Content-Type": command.ContentType},
							ExpiresAt: expiresAt,
						},
					}}, nil
				},
			}
			request := httptest.NewRequest(http.MethodPost, "/uploads", strings.NewReader(test.body))
			if test.chunked {
				request.ContentLength = -1
				request.TransferEncoding = []string{"chunked"}
			}
			if test.keys != nil {
				request.Header["Idempotency-Key"] = append([]string(nil), test.keys...)
			}
			if test.contentTypes != nil {
				request.Header["Content-Type"] = append([]string(nil), test.contentTypes...)
			}
			recorder := httptest.NewRecorder()
			app.ServeHTTP(recorder, request)

			if recorder.Code != test.wantStatus {
				t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
			}
			if recorder.Header().Get("Content-Type") != "application/json" || recorder.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("unexpected response headers: %v", recorder.Header())
			}
			wantCalls := int64(0)
			if test.wantCall {
				wantCalls = 1
			}
			if calls.Load() != wantCalls {
				t.Fatalf("create calls=%d, want %d", calls.Load(), wantCalls)
			}
			if test.wantCode != "" {
				if code := responseErrorCode(t, recorder.Body.Bytes()); code != test.wantCode {
					t.Fatalf("error code=%q, want %q", code, test.wantCode)
				}
				return
			}
			if recorder.Header().Get("Location") != "/uploads/"+testUploadID {
				t.Fatalf("Location=%q", recorder.Header().Get("Location"))
			}
			var got map[string]any
			if err := json.Unmarshal(recorder.Body.Bytes(), &got); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			want := map[string]any{
				"upload_id":             testUploadID,
				"state":                 "pending",
				"declared_size_bytes":   float64(test.wantSize),
				"declared_content_type": test.wantType,
				"upload_deadline":       deadline.Format(time.RFC3339),
				"upload_request": map[string]any{
					"method":     http.MethodPut,
					"url":        "http://storage.invalid/bucket/staging/id?X-Amz-Signature=opaque",
					"headers":    map[string]any{"Content-Type": test.wantType},
					"expires_at": expiresAt.Format(time.RFC3339),
				},
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("representation=%#v, want %#v", got, want)
			}
		})
	}
}

func TestCreateUploadOutcomeContract(t *testing.T) {
	deadline := time.Date(2026, time.August, 31, 12, 0, 0, 0, time.UTC)
	base := uploadRepresentation{
		UploadID:            testUploadID,
		State:               "pending",
		DeclaredSizeBytes:   7,
		DeclaredContentType: "image/png",
		UploadDeadline:      deadline,
		UploadRequest: &uploadRequest{
			Method:    http.MethodPut,
			URL:       "http://storage.invalid/staging/id?X-Amz-Signature=opaque",
			Headers:   map[string]string{"Content-Type": "image/png"},
			ExpiresAt: deadline.Add(-23*time.Hour - 45*time.Minute),
		},
	}
	tests := []struct {
		name       string
		result     createUploadResult
		err        error
		wantStatus int
		wantCode   string
	}{
		{"exact replay", createUploadResult{Upload: base, Created: false}, nil, http.StatusOK, ""},
		{"reused key", createUploadResult{}, errIdempotencyKeyReused, http.StatusUnprocessableEntity, "idempotency_key_reused"},
		{"dependency unavailable", createUploadResult{}, errServiceUnavailable, http.StatusServiceUnavailable, "service_unavailable"},
		{"deterministic internal failure", createUploadResult{}, errors.New("raw-database-secret"), http.StatusInternalServerError, "internal_error"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app := &application{
				logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
				createUpload: func(context.Context, createUploadCommand) (createUploadResult, error) {
					return test.result, test.err
				},
			}
			request := httptest.NewRequest(http.MethodPost, "/uploads", strings.NewReader(`{"size_bytes":7,"content_type":"image/png"}`))
			request.Header.Set("Idempotency-Key", testIdempotencyKey)
			request.Header.Set("Content-Type", "application/json")
			recorder := httptest.NewRecorder()
			app.ServeHTTP(recorder, request)
			if recorder.Code != test.wantStatus || recorder.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("status=%d headers=%v body=%s", recorder.Code, recorder.Header(), recorder.Body.String())
			}
			if test.wantCode == "" {
				if recorder.Header().Get("Location") != "" || !strings.Contains(recorder.Body.String(), `"upload_id":"`+testUploadID+`"`) || !strings.Contains(recorder.Body.String(), `"upload_request"`) {
					t.Fatalf("replay response is not the exact 200 representation: headers=%v body=%s", recorder.Header(), recorder.Body.String())
				}
				return
			}
			if code := responseErrorCode(t, recorder.Body.Bytes()); code != test.wantCode {
				t.Fatalf("error code=%q, want %q", code, test.wantCode)
			}
			for _, secret := range []string{testIdempotencyKey, "image/png", "upload_request", "X-Amz-", "staging/", "raw-database-secret"} {
				if strings.Contains(recorder.Body.String(), secret) {
					t.Fatalf("error disclosed %q", secret)
				}
			}
		})
	}
}

func TestExactDispatcherAndRepresentation(t *testing.T) {
	deadline := time.Date(2026, time.August, 31, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name          string
		method        string
		path          string
		wantStatus    int
		wantAllow     string
		wantGets      int64
		wantCompletes int64
		wantState     string
		wantLocation  string
	}{
		{"get pending", http.MethodGet, "/uploads/opaque-id", http.StatusOK, "", 1, 0, "pending", ""},
		{"complete", http.MethodPost, "/uploads/opaque-id/complete", http.StatusAccepted, "", 0, 1, "finalizing", "/uploads/opaque-id"},
		{"HEAD is not GET", http.MethodHead, "/uploads/opaque-id", http.StatusMethodNotAllowed, http.MethodGet, 0, 0, "", ""},
		{"wrong status method", http.MethodPost, "/uploads/opaque-id", http.StatusMethodNotAllowed, http.MethodGet, 0, 0, "", ""},
		{"GET complete route", http.MethodGet, "/uploads/opaque-id/complete", http.StatusMethodNotAllowed, http.MethodPost, 0, 0, "", ""},
		{"HEAD complete route", http.MethodHead, "/uploads/opaque-id/complete", http.StatusMethodNotAllowed, http.MethodPost, 0, 0, "", ""},
		{"GET create route", http.MethodGet, "/uploads", http.StatusMethodNotAllowed, http.MethodPost, 0, 0, "", ""},
		{"HEAD create route", http.MethodHead, "/uploads", http.StatusMethodNotAllowed, http.MethodPost, 0, 0, "", ""},
		{"trailing create slash", http.MethodPost, "/uploads/", http.StatusNotFound, "", 0, 0, "", ""},
		{"empty status segment", http.MethodGet, "/uploads/", http.StatusNotFound, "", 0, 0, "", ""},
		{"trailing status slash", http.MethodGet, "/uploads/opaque-id/", http.StatusNotFound, "", 0, 0, "", ""},
		{"trailing complete slash", http.MethodPost, "/uploads/opaque-id/complete/", http.StatusNotFound, "", 0, 0, "", ""},
		{"empty completion ID", http.MethodPost, "/uploads//complete", http.StatusNotFound, "", 0, 0, "", ""},
		{"double slash", http.MethodGet, "/uploads//opaque-id", http.StatusNotFound, "", 0, 0, "", ""},
		{"cleanable path", http.MethodGet, "/uploads/opaque-id/../other", http.StatusNotFound, "", 0, 0, "", ""},
		{"cleanable complete path", http.MethodPost, "/uploads/opaque-id/../complete", http.StatusNotFound, "", 0, 0, "", ""},
		{"unknown path", http.MethodGet, "/upload/opaque-id", http.StatusNotFound, "", 0, 0, "", ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var gets atomic.Int64
			var completes atomic.Int64
			app := &application{
				logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
				completeUpload: func(_ context.Context, uploadID string) (completeUploadResult, error) {
					completes.Add(1)
					if uploadID != "opaque-id" {
						t.Fatalf("decoded completion upload ID=%q", uploadID)
					}
					return completeUploadResult{Upload: uploadRepresentation{
						UploadID:            uploadID,
						State:               "finalizing",
						DeclaredSizeBytes:   11,
						DeclaredContentType: "image/jpeg",
						UploadDeadline:      deadline,
					}}, nil
				},
				getUpload: func(_ context.Context, uploadID string) (uploadRepresentation, error) {
					gets.Add(1)
					if uploadID != "opaque-id" {
						t.Fatalf("decoded upload ID=%q", uploadID)
					}
					return uploadRepresentation{
						UploadID:            uploadID,
						State:               "pending",
						DeclaredSizeBytes:   11,
						DeclaredContentType: "image/jpeg",
						UploadDeadline:      deadline,
						UploadRequest: &uploadRequest{
							Method:  http.MethodPut,
							URL:     "http://must-not-leak.invalid/?X-Amz-Signature=opaque",
							Headers: map[string]string{"Content-Type": "image/jpeg"},
						},
					}, nil
				},
			}
			recorder := httptest.NewRecorder()
			app.ServeHTTP(recorder, httptest.NewRequest(test.method, test.path, nil))
			if recorder.Code != test.wantStatus || recorder.Header().Get("Allow") != test.wantAllow {
				t.Fatalf("status=%d Allow=%q body=%s", recorder.Code, recorder.Header().Get("Allow"), recorder.Body.String())
			}
			if gets.Load() != test.wantGets {
				t.Fatalf("get calls=%d, want %d", gets.Load(), test.wantGets)
			}
			if completes.Load() != test.wantCompletes {
				t.Fatalf("complete calls=%d, want %d", completes.Load(), test.wantCompletes)
			}
			if recorder.Header().Get("Cache-Control") != "no-store" || recorder.Header().Get("Content-Type") != "application/json" {
				t.Fatalf("unexpected response headers: %v", recorder.Header())
			}
			if test.wantStatus >= 300 && test.wantStatus < 400 {
				t.Fatal("dispatcher redirected a non-exact path")
			}
			if recorder.Header().Get("Location") != test.wantLocation {
				t.Fatalf("Location=%q, want %q", recorder.Header().Get("Location"), test.wantLocation)
			}
			if test.wantState == "" {
				return
			}
			var got map[string]any
			if err := json.Unmarshal(recorder.Body.Bytes(), &got); err != nil {
				t.Fatalf("decode GET representation: %v", err)
			}
			want := map[string]any{
				"upload_id":             "opaque-id",
				"state":                 test.wantState,
				"declared_size_bytes":   float64(11),
				"declared_content_type": "image/jpeg",
				"upload_deadline":       deadline.Format(time.RFC3339),
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("GET representation=%#v, want %#v", got, want)
			}
			for _, forbidden := range []string{"upload_request", "url", "bucket", "staging", "etag", "sha256", "claim", "retry"} {
				if strings.Contains(strings.ToLower(recorder.Body.String()), forbidden) {
					t.Fatalf("GET representation disclosed %q", forbidden)
				}
			}
		})
	}

	app := &application{
		logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		getUpload: func(context.Context, string) (uploadRepresentation, error) {
			return uploadRepresentation{}, errUploadNotFound
		},
	}
	recorder := httptest.NewRecorder()
	app.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/uploads/missing", nil))
	if recorder.Code != http.StatusNotFound || responseErrorCode(t, recorder.Body.Bytes()) != "upload_not_found" {
		t.Fatalf("unknown upload response: status=%d body=%s", recorder.Code, recorder.Body.String())
	}

	completionOutcomes := []struct {
		name         string
		state        string
		failure      *uploadFailure
		err          error
		wantStatus   int
		wantCode     string
		wantLocation string
	}{
		{"finalizing replay", "finalizing", nil, nil, http.StatusAccepted, "", "/uploads/" + testUploadID},
		{"ready replay", "ready", nil, nil, http.StatusOK, "", ""},
		{"rejected replay", "rejected", &uploadFailure{Code: "invalid_image"}, nil, http.StatusOK, "", ""},
		{"expired replay", "expired", &uploadFailure{Code: "upload_expired"}, nil, http.StatusOK, "", ""},
		{"unknown upload", "", nil, errUploadNotFound, http.StatusNotFound, "upload_not_found", ""},
		{"dependency unavailable", "", nil, errServiceUnavailable, http.StatusServiceUnavailable, "service_unavailable", ""},
		{"internal failure", "", nil, errors.New("raw-database-secret"), http.StatusInternalServerError, "internal_error", ""},
	}
	for _, test := range completionOutcomes {
		t.Run(test.name, func(t *testing.T) {
			app := &application{
				logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
				completeUpload: func(context.Context, string) (completeUploadResult, error) {
					return completeUploadResult{Upload: uploadRepresentation{
						UploadID:            testUploadID,
						State:               test.state,
						DeclaredSizeBytes:   11,
						DeclaredContentType: "image/jpeg",
						UploadDeadline:      deadline,
						Failure:             test.failure,
					}}, test.err
				},
			}
			recorder := httptest.NewRecorder()
			app.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/uploads/"+testUploadID+"/complete", nil))
			if recorder.Code != test.wantStatus || recorder.Header().Get("Location") != test.wantLocation || recorder.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("status=%d Location=%q headers=%v body=%s", recorder.Code, recorder.Header().Get("Location"), recorder.Header(), recorder.Body.String())
			}
			if test.wantCode != "" {
				if code := responseErrorCode(t, recorder.Body.Bytes()); code != test.wantCode {
					t.Fatalf("error code=%q, want %q", code, test.wantCode)
				}
				if strings.Contains(recorder.Body.String(), "raw-database-secret") {
					t.Fatal("completion error disclosed raw dependency error")
				}
				return
			}
			var representation map[string]any
			if err := json.Unmarshal(recorder.Body.Bytes(), &representation); err != nil {
				t.Fatal("decode completion representation")
			}
			if test.failure != nil {
				failure, ok := representation["failure"].(map[string]any)
				if !ok || failure["code"] != test.failure.Code {
					t.Fatalf("terminal failure=%#v", representation["failure"])
				}
			}
			for _, forbidden := range []string{"upload_request", "completion_requested_at", "reconcile_after", "staging", "claim", "retry"} {
				if strings.Contains(strings.ToLower(recorder.Body.String()), forbidden) {
					t.Fatalf("completion representation disclosed %q", forbidden)
				}
			}
		})
	}

	var bodylessCalls atomic.Int64
	app = &application{
		logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		getUpload: func(context.Context, string) (uploadRepresentation, error) {
			bodylessCalls.Add(1)
			return uploadRepresentation{}, nil
		},
		completeUpload: func(context.Context, string) (completeUploadResult, error) {
			bodylessCalls.Add(1)
			return completeUploadResult{}, nil
		},
	}
	recorder = httptest.NewRecorder()
	app.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/uploads/opaque-id", strings.NewReader(`{}`)))
	if recorder.Code != http.StatusBadRequest || responseErrorCode(t, recorder.Body.Bytes()) != "invalid_request" || bodylessCalls.Load() != 0 {
		t.Fatalf("non-empty GET body reached status read: status=%d calls=%d body=%s", recorder.Code, bodylessCalls.Load(), recorder.Body.String())
	}
	recorder = httptest.NewRecorder()
	app.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/uploads/opaque-id/complete", strings.NewReader(" ")))
	if recorder.Code != http.StatusBadRequest || responseErrorCode(t, recorder.Body.Bytes()) != "invalid_request" || bodylessCalls.Load() != 0 {
		t.Fatalf("non-empty completion body reached transaction: status=%d calls=%d body=%s", recorder.Code, bodylessCalls.Load(), recorder.Body.String())
	}

	var logs bytes.Buffer
	var completionCalls atomic.Int64
	transitionCounter := serviceMetrics.Get("upload_transitions_total").(*expvar.Map).Get("pending_to_finalizing").(*expvar.Int)
	counterBefore := transitionCounter.Value()
	app = &application{
		logger: slog.New(slog.NewJSONHandler(&logs, nil)),
		completeUpload: func(context.Context, string) (completeUploadResult, error) {
			transition := ""
			if completionCalls.Add(1) == 1 {
				transition = "pending_to_finalizing"
			}
			return completeUploadResult{Upload: uploadRepresentation{
				UploadID:            testUploadID,
				State:               "finalizing",
				DeclaredSizeBytes:   11,
				DeclaredContentType: "image/jpeg",
				UploadDeadline:      deadline,
			}, Transition: transition}, nil
		},
	}
	for range 2 {
		recorder = httptest.NewRecorder()
		app.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/uploads/"+testUploadID+"/complete", nil))
		if recorder.Code != http.StatusAccepted {
			t.Fatalf("completion replay status=%d body=%s", recorder.Code, recorder.Body.String())
		}
	}
	if transitionCounter.Value()-counterBefore != 1 || strings.Count(logs.String(), `"msg":"upload.transition"`) != 1 {
		t.Fatalf("transition counter delta=%d logs=%s", transitionCounter.Value()-counterBefore, logs.String())
	}
	for _, forbidden := range []string{"X-Amz-", "staging/", testIdempotencyKey, "raw-database-secret"} {
		if strings.Contains(logs.String(), forbidden) {
			t.Fatalf("completion logs disclosed %q", forbidden)
		}
	}
}

func TestIntegrationCreateIdempotencyAndRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("requires migrated PostgreSQL")
	}
	cfg := integrationConfig(t)

	t.Run("blocked waiter observes committed winner", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		pool := newSingleConnectionPool(t, ctx, cfg.DatabaseURL)
		defer pool.Close()
		holder := connectUploadTestPostgres(t, ctx, cfg.DatabaseURL)
		defer holder.Close(context.Background())
		observer := connectUploadTestPostgres(t, ctx, cfg.DatabaseURL)
		defer observer.Close(context.Background())

		idempotencyKey := testUUID(t)
		uploadID := testUUID(t)
		defer cleanupUploadByIdempotencyKey(t, pool, idempotencyKey)
		holderTx, err := holder.Begin(ctx)
		if err != nil {
			t.Fatal("begin holder transaction")
		}
		defer holderTx.Rollback(context.Background())
		if err := insertPendingUpload(ctx, holderTx, uploadID, idempotencyKey, 19, "image/png"); err != nil {
			t.Fatal("insert uncommitted upload")
		}

		workerPID := singlePoolPID(t, ctx, pool)
		presignEntered := make(chan struct{}, 1)
		var presignCalls atomic.Int64
		result := make(chan createCallOutcome, 1)
		go func() {
			created, err := createOrReplayUpload(
				ctx,
				pool,
				testUploadPresigner(&presignCalls, presignEntered),
				createUploadCommand{IdempotencyKey: idempotencyKey, SizeBytes: 19, ContentType: "image/png"},
			)
			result <- createCallOutcome{result: created, err: err}
		}()
		select {
		case <-presignEntered:
		case <-ctx.Done():
			t.Fatal("create did not reach presigning")
		}
		waitForPostgresBlock(t, ctx, observer, workerPID, holder.PgConn().PID())
		if err := holderTx.Commit(ctx); err != nil {
			t.Fatal("commit holder transaction")
		}
		outcome := receiveCreateOutcome(t, ctx, result)
		if outcome.err != nil || outcome.result.Created || outcome.result.Upload.UploadID != uploadID || outcome.result.Upload.UploadRequest == nil {
			t.Fatalf("commit waiter outcome=%#v err=%v", outcome.result, outcome.err)
		}
		if presignCalls.Load() != 2 {
			t.Fatalf("presign calls=%d, want speculative plus replay", presignCalls.Load())
		}
		if !strings.Contains(outcome.result.Upload.UploadRequest.URL, "/staging/"+uploadID) {
			t.Fatal("replay capability did not target the committed staging key")
		}
		assertOneUploadForKey(t, ctx, observer, idempotencyKey, uploadID, outcome.result.Upload.UploadRequest.ExpiresAt)
	})

	t.Run("blocked waiter becomes creator after rollback", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		pool := newSingleConnectionPool(t, ctx, cfg.DatabaseURL)
		defer pool.Close()
		holder := connectUploadTestPostgres(t, ctx, cfg.DatabaseURL)
		defer holder.Close(context.Background())
		observer := connectUploadTestPostgres(t, ctx, cfg.DatabaseURL)
		defer observer.Close(context.Background())

		idempotencyKey := testUUID(t)
		rolledBackUploadID := testUUID(t)
		defer cleanupUploadByIdempotencyKey(t, pool, idempotencyKey)
		holderTx, err := holder.Begin(ctx)
		if err != nil {
			t.Fatal("begin holder transaction")
		}
		defer holderTx.Rollback(context.Background())
		if err := insertPendingUpload(ctx, holderTx, rolledBackUploadID, idempotencyKey, 23, "image/jpeg"); err != nil {
			t.Fatal("insert rollback candidate")
		}

		workerPID := singlePoolPID(t, ctx, pool)
		presignEntered := make(chan struct{}, 1)
		var presignCalls atomic.Int64
		result := make(chan createCallOutcome, 1)
		go func() {
			created, err := createOrReplayUpload(
				ctx,
				pool,
				testUploadPresigner(&presignCalls, presignEntered),
				createUploadCommand{IdempotencyKey: idempotencyKey, SizeBytes: 23, ContentType: "image/jpeg"},
			)
			result <- createCallOutcome{result: created, err: err}
		}()
		select {
		case <-presignEntered:
		case <-ctx.Done():
			t.Fatal("create did not reach presigning")
		}
		waitForPostgresBlock(t, ctx, observer, workerPID, holder.PgConn().PID())
		if err := holderTx.Rollback(ctx); err != nil {
			t.Fatal("rollback holder transaction")
		}
		outcome := receiveCreateOutcome(t, ctx, result)
		if outcome.err != nil || !outcome.result.Created || outcome.result.Upload.UploadID == rolledBackUploadID || outcome.result.Upload.UploadRequest == nil {
			t.Fatalf("rollback waiter outcome=%#v err=%v", outcome.result, outcome.err)
		}
		if presignCalls.Load() != 1 {
			t.Fatalf("presign calls=%d, want one winning speculative request", presignCalls.Load())
		}
		assertOneUploadForKey(t, ctx, observer, idempotencyKey, outcome.result.Upload.UploadID, outcome.result.Upload.UploadRequest.ExpiresAt)
	})

	t.Run("blocked waiter fails within its request budget", func(t *testing.T) {
		setupContext, cancelSetup := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelSetup()
		pool := newSingleConnectionPool(t, setupContext, cfg.DatabaseURL)
		defer pool.Close()
		holder := connectUploadTestPostgres(t, setupContext, cfg.DatabaseURL)
		defer holder.Close(context.Background())
		observer := connectUploadTestPostgres(t, setupContext, cfg.DatabaseURL)
		defer observer.Close(context.Background())

		idempotencyKey := testUUID(t)
		defer cleanupUploadByIdempotencyKey(t, pool, idempotencyKey)
		holderTx, err := holder.Begin(setupContext)
		if err != nil {
			t.Fatal("begin bounded-wait holder transaction")
		}
		defer holderTx.Rollback(context.Background())
		if err := insertPendingUpload(setupContext, holderTx, testUUID(t), idempotencyKey, 27, "image/png"); err != nil {
			t.Fatal("insert bounded-wait holder upload")
		}

		outerContext, cancelOuter := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelOuter()
		workerPID := singlePoolPID(t, outerContext, pool)
		presignEntered := make(chan struct{}, 1)
		var presignCalls atomic.Int64
		result := make(chan createCallOutcome, 1)
		go func() {
			created, err := createOrReplayUpload(
				outerContext,
				pool,
				testUploadPresigner(&presignCalls, presignEntered),
				createUploadCommand{IdempotencyKey: idempotencyKey, SizeBytes: 27, ContentType: "image/png"},
			)
			result <- createCallOutcome{result: created, err: err}
		}()
		select {
		case <-presignEntered:
		case <-outerContext.Done():
			t.Fatal("bounded create did not reach presigning")
		}
		waitForPostgresBlock(t, outerContext, observer, workerPID, holder.PgConn().PID())
		outcome := receiveCreateOutcome(t, outerContext, result)
		if !errors.Is(outcome.err, errServiceUnavailable) || outcome.result.Upload.UploadID != "" || outcome.result.Upload.UploadRequest != nil || presignCalls.Load() != 1 {
			t.Fatalf("bounded waiter exposed outcome=%#v err=%v presigns=%d", outcome.result, outcome.err, presignCalls.Load())
		}
		var holderRows int
		if err := holderTx.QueryRow(setupContext, `
			SELECT count(*)
			FROM uploads
			WHERE idempotency_key = $1::uuid`, idempotencyKey).Scan(&holderRows); err != nil {
			t.Fatal("read still-uncommitted holder row")
		}
		if holderRows != 1 {
			t.Fatalf("holder transaction contains %d rows", holderRows)
		}
		var visibleRows int
		if err := observer.QueryRow(setupContext, `
			SELECT count(*)
			FROM uploads
			WHERE idempotency_key = $1::uuid`, idempotencyKey).Scan(&visibleRows); err != nil {
			t.Fatal("count rows while winner remains uncommitted")
		}
		if visibleRows != 0 {
			t.Fatalf("bounded waiter created %d visible rows", visibleRows)
		}
		if err := holderTx.Rollback(setupContext); err != nil {
			t.Fatal("rollback bounded-wait holder")
		}
		if err := observer.QueryRow(setupContext, `
			SELECT count(*)
			FROM uploads
			WHERE idempotency_key = $1::uuid`, idempotencyKey).Scan(&visibleRows); err != nil {
			t.Fatal("count rows after bounded-wait rollback")
		}
		if visibleRows != 0 {
			t.Fatalf("bounded wait left %d rows", visibleRows)
		}
	})

	t.Run("different tuple does not mutate the row", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		pool := newSingleConnectionPool(t, ctx, cfg.DatabaseURL)
		defer pool.Close()
		observer := connectUploadTestPostgres(t, ctx, cfg.DatabaseURL)
		defer observer.Close(context.Background())

		idempotencyKey := testUUID(t)
		defer cleanupUploadByIdempotencyKey(t, pool, idempotencyKey)
		var presignCalls atomic.Int64
		first, err := createOrReplayUpload(
			ctx,
			pool,
			testUploadPresigner(&presignCalls, nil),
			createUploadCommand{IdempotencyKey: idempotencyKey, SizeBytes: 29, ContentType: "image/png"},
		)
		if err != nil || !first.Created || first.Upload.UploadRequest == nil {
			t.Fatalf("initial create=%#v err=%v", first, err)
		}
		var horizonBefore time.Time
		if err := observer.QueryRow(ctx, `
			SELECT max_write_expires_at
			FROM uploads
			WHERE idempotency_key = $1::uuid`, idempotencyKey).Scan(&horizonBefore); err != nil {
			t.Fatal("read initial write horizon")
		}
		_, err = createOrReplayUpload(
			ctx,
			pool,
			testUploadPresigner(&presignCalls, nil),
			createUploadCommand{IdempotencyKey: idempotencyKey, SizeBytes: 30, ContentType: "image/png"},
		)
		if !errors.Is(err, errIdempotencyKeyReused) {
			t.Fatalf("different tuple error=%v", err)
		}
		var count int
		var storedUploadID string
		var storedSize int64
		var storedType string
		var horizonAfter time.Time
		if err := observer.QueryRow(ctx, `
			SELECT count(*), min(upload_id::text), min(declared_size),
			       min(declared_content_type), min(max_write_expires_at)
			FROM uploads
			WHERE idempotency_key = $1::uuid`, idempotencyKey).Scan(
			&count,
			&storedUploadID,
			&storedSize,
			&storedType,
			&horizonAfter,
		); err != nil {
			t.Fatal("read conflicting tuple outcome")
		}
		if count != 1 || storedUploadID != first.Upload.UploadID || storedSize != 29 || storedType != "image/png" || !horizonAfter.Equal(horizonBefore) {
			t.Fatalf("conflict mutated row: count=%d id=%s size=%d type=%s before=%s after=%s", count, storedUploadID, storedSize, storedType, horizonBefore, horizonAfter)
		}
	})

	t.Run("fresh actor recovers discarded committed result", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		idempotencyKey := testUUID(t)
		command := createUploadCommand{IdempotencyKey: idempotencyKey, SizeBytes: 31, ContentType: "image/jpeg"}
		var presignCalls atomic.Int64

		firstPool := newSingleConnectionPool(t, ctx, cfg.DatabaseURL)
		first, err := createOrReplayUpload(ctx, firstPool, testUploadPresigner(&presignCalls, nil), command)
		firstPool.Close()
		freshPool := newSingleConnectionPool(t, ctx, cfg.DatabaseURL)
		defer freshPool.Close()
		defer cleanupUploadByIdempotencyKey(t, freshPool, idempotencyKey)
		if err != nil || !first.Created || first.Upload.UploadRequest == nil {
			t.Fatalf("discarded create=%#v err=%v", first, err)
		}
		fresh, err := createOrReplayUpload(ctx, freshPool, testUploadPresigner(&presignCalls, nil), command)
		if err != nil || fresh.Created || fresh.Upload.UploadID != first.Upload.UploadID || fresh.Upload.UploadRequest == nil {
			t.Fatalf("fresh recovery=%#v err=%v", fresh, err)
		}
		observer := connectUploadTestPostgres(t, ctx, cfg.DatabaseURL)
		defer observer.Close(context.Background())
		assertOneUploadForKey(t, ctx, observer, idempotencyKey, first.Upload.UploadID, fresh.Upload.UploadRequest.ExpiresAt)
	})
}

func TestIntegrationGarageUploadWireContract(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Garage")
	}
	cfg := integrationConfig(t)
	endpoint, err := url.Parse(cfg.S3Endpoint)
	if err != nil || endpoint.Scheme == "" || endpoint.Host == "" {
		t.Fatal("full Garage test requires an explicit S3_ENDPOINT")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	storage, err := newS3Client(ctx, cfg)
	if err != nil {
		t.Fatal("create Garage client")
	}
	presigner := s3.NewPresignClient(storage)
	key := "staging/" + testUUID(t)
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if _, err := storage.DeleteObject(cleanupContext, &s3.DeleteObjectInput{
			Bucket: aws.String(cfg.S3Bucket),
			Key:    aws.String(key),
		}); err != nil {
			t.Error("delete Garage object")
		}
	})

	put, err := presignUploadPUT(ctx, presigner, cfg.S3Bucket, key, "image/png")
	if err != nil {
		t.Fatal("presign Garage PUT")
	}
	parsed, err := url.Parse(put.URL)
	if err != nil {
		t.Fatal("parse presigned URL")
	}
	if put.Method != http.MethodPut || parsed.Scheme != endpoint.Scheme || parsed.Host != endpoint.Host || parsed.Path != "/"+cfg.S3Bucket+"/"+key {
		t.Fatalf(
			"presigned target mismatch: method_ok=%t scheme_ok=%t host_ok=%t path_ok=%t",
			put.Method == http.MethodPut,
			parsed.Scheme == endpoint.Scheme,
			parsed.Host == endpoint.Host,
			parsed.Path == "/"+cfg.S3Bucket+"/"+key,
		)
	}
	query := parsed.Query()
	if query.Get("X-Amz-Expires") != "900" || query.Get("X-Amz-SignedHeaders") != "content-type;host" || query.Get("X-Amz-Signature") == "" {
		t.Fatalf("unexpected presign TTL or signed headers: expires=%q headers=%q", query.Get("X-Amz-Expires"), query.Get("X-Amz-SignedHeaders"))
	}
	if !reflect.DeepEqual(put.Headers, map[string]string{"Content-Type": "image/png"}) {
		t.Fatalf("authoritative headers=%v", put.Headers)
	}
	signedAt, err := time.Parse("20060102T150405Z", query.Get("X-Amz-Date"))
	if err != nil || !put.ExpiresAt.Equal(signedAt.Add(15*time.Minute)) {
		t.Fatalf("expires_at=%s signing time=%q", put.ExpiresAt, query.Get("X-Amz-Date"))
	}
	for name := range query {
		if uploadTestChecksumName(name) || strings.EqualFold(name, "Content-Length") {
			t.Fatalf("presign query unexpectedly binds %s", name)
		}
	}
	for name := range put.Headers {
		if uploadTestChecksumName(name) || strings.EqualFold(name, "Content-Length") {
			t.Fatalf("presign headers unexpectedly bind %s", name)
		}
	}

	rawClient := &http.Client{
		Timeout: 10 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	bodyA := []byte("garage-upload-body-a")
	bodyB := []byte("garage-overwrite-body-b-is-different")
	if status := rawPresignedPUT(t, ctx, rawClient, put, bodyA, ""); status != http.StatusOK {
		t.Fatalf("exact PUT status=%d", status)
	}
	assertGarageObject(t, ctx, storage, cfg.S3Bucket, key, bodyA, "image/png")
	if status := rawPresignedPUT(t, ctx, rawClient, put, []byte("must-not-overwrite"), "application/octet-stream"); status >= 200 && status < 300 {
		t.Fatalf("mismatched signed Content-Type was accepted with %d", status)
	}
	assertGarageObject(t, ctx, storage, cfg.S3Bucket, key, bodyA, "image/png")
	if status := rawPresignedPUT(t, ctx, rawClient, put, bodyA, ""); status != http.StatusOK {
		t.Fatalf("exact retry status=%d", status)
	}
	if status := rawPresignedPUT(t, ctx, rawClient, put, bodyB, ""); status != http.StatusOK {
		t.Fatalf("exact overwrite status=%d", status)
	}
	assertGarageObject(t, ctx, storage, cfg.S3Bucket, key, bodyB, "image/png")
}

func TestE2EHappyPathAndExactReplay(t *testing.T) {
	if testing.Short() {
		t.Skip("requires migrated PostgreSQL and Garage")
	}
	cfg := integrationConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
	app := &application{
		logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		postgresPing: pool.Ping,
		s3HeadBucket: func(ctx context.Context) error {
			return headBucket(ctx, storage, cfg.S3Bucket)
		},
		createUpload: func(ctx context.Context, command createUploadCommand) (createUploadResult, error) {
			return createOrReplayUpload(ctx, pool, func(ctx context.Context, key, contentType string) (uploadRequest, error) {
				return presignUploadPUT(ctx, presigner, cfg.S3Bucket, key, contentType)
			}, command)
		},
		completeUpload: func(ctx context.Context, uploadID string) (completeUploadResult, error) {
			return completeUploadByID(ctx, pool, uploadID)
		},
		getUpload: func(ctx context.Context, uploadID string) (uploadRepresentation, error) {
			return getUploadByID(ctx, pool, uploadID)
		},
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("start upload test listener")
	}
	server := newHTTPServer(listener.Addr().String(), app)
	serveResult := make(chan error, 1)
	go func() { serveResult <- server.Serve(listener) }()
	defer func() {
		_ = server.Close()
		<-serveResult
	}()

	client := &http.Client{
		Timeout: 10 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	idempotencyKey := testUUID(t)
	defer cleanupUploadByIdempotencyKey(t, pool, idempotencyKey)
	createRequest, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		"http://"+listener.Addr().String()+"/uploads",
		strings.NewReader(`{"size_bytes":21,"content_type":"image/png"}`),
	)
	if err != nil {
		t.Fatal("build create request")
	}
	createRequest.Header.Set("Idempotency-Key", idempotencyKey)
	createRequest.Header.Set("Content-Type", "application/json")
	createResponse, err := client.Do(createRequest)
	if err != nil {
		t.Fatal("send create request")
	}
	createBody, readErr := io.ReadAll(io.LimitReader(createResponse.Body, 32<<10))
	createResponse.Body.Close()
	if readErr != nil {
		t.Fatal("read create response")
	}
	if createResponse.StatusCode != http.StatusCreated ||
		createResponse.Header.Get("Content-Type") != "application/json" ||
		createResponse.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf(
			"create response contract mismatch: status=%d content_type=%q cache_control=%q",
			createResponse.StatusCode,
			createResponse.Header.Get("Content-Type"),
			createResponse.Header.Get("Cache-Control"),
		)
	}
	var created uploadRepresentation
	if err := json.Unmarshal(createBody, &created); err != nil || created.UploadRequest == nil {
		t.Fatalf("decode create representation: %v", err)
	}
	location := "/uploads/" + created.UploadID
	if createResponse.Header.Get("Location") != location || created.State != "pending" || created.DeclaredSizeBytes != 21 || created.DeclaredContentType != "image/png" {
		t.Fatalf(
			"create representation mismatch: location_ok=%t state=%q size=%d content_type=%q",
			createResponse.Header.Get("Location") == location,
			created.State,
			created.DeclaredSizeBytes,
			created.DeclaredContentType,
		)
	}
	replayRequest, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		"http://"+listener.Addr().String()+"/uploads",
		strings.NewReader(`{"size_bytes":21,"content_type":"image/png"}`),
	)
	if err != nil {
		t.Fatal("build exact replay request")
	}
	replayRequest.Header.Set("Idempotency-Key", idempotencyKey)
	replayRequest.Header.Set("Content-Type", "application/json")
	replayResponse, err := client.Do(replayRequest)
	if err != nil {
		t.Fatal("send exact replay request")
	}
	replayBody, readErr := io.ReadAll(io.LimitReader(replayResponse.Body, 32<<10))
	replayResponse.Body.Close()
	if readErr != nil {
		t.Fatal("read exact replay response")
	}
	var replayed uploadRepresentation
	if replayResponse.StatusCode != http.StatusOK || replayResponse.Header.Get("Location") != "" ||
		json.Unmarshal(replayBody, &replayed) != nil || replayed.UploadID != created.UploadID || replayed.UploadRequest == nil {
		t.Fatalf("exact replay mismatch: status=%d Location=%q body=%s", replayResponse.StatusCode, replayResponse.Header.Get("Location"), replayBody)
	}
	objectKey := "staging/" + created.UploadID
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if _, err := storage.DeleteObject(cleanupContext, &s3.DeleteObjectInput{
			Bucket: aws.String(cfg.S3Bucket),
			Key:    aws.String(objectKey),
		}); err != nil {
			t.Error("delete end-to-end object")
		}
	})

	rawBody := []byte("direct-put-raw-bytes!")
	if status := rawPresignedPUT(t, ctx, client, *replayed.UploadRequest, rawBody, ""); status != http.StatusOK {
		t.Fatalf("direct PUT status=%d", status)
	}
	getResponse, err := client.Get("http://" + listener.Addr().String() + location)
	if err != nil {
		t.Fatal("GET pending upload")
	}
	getBody, readErr := io.ReadAll(io.LimitReader(getResponse.Body, 32<<10))
	getResponse.Body.Close()
	if readErr != nil {
		t.Fatal("read pending representation")
	}
	if getResponse.StatusCode != http.StatusOK || getResponse.Header.Get("Content-Type") != "application/json" || getResponse.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf(
			"GET response contract mismatch: status=%d content_type=%q cache_control=%q",
			getResponse.StatusCode,
			getResponse.Header.Get("Content-Type"),
			getResponse.Header.Get("Cache-Control"),
		)
	}
	var pending map[string]any
	if err := json.Unmarshal(getBody, &pending); err != nil {
		t.Fatalf("decode pending representation: %v", err)
	}
	if len(pending) != 5 || pending["upload_id"] != created.UploadID || pending["state"] != "pending" {
		t.Fatalf(
			"pending representation mismatch: field_count=%d id_ok=%t state=%q",
			len(pending),
			pending["upload_id"] == created.UploadID,
			pending["state"],
		)
	}
	for _, forbidden := range []string{"upload_request", "url", "bucket", "staging", "etag", "sha256", "claim", "retry", "X-Amz-"} {
		if strings.Contains(strings.ToLower(string(getBody)), strings.ToLower(forbidden)) {
			t.Fatalf("pending representation disclosed %q", forbidden)
		}
	}
	var state string
	var horizon time.Time
	if err := pool.QueryRow(ctx, `
		SELECT state, max_write_expires_at
		FROM uploads
		WHERE upload_id = $1::uuid`, created.UploadID).Scan(&state, &horizon); err != nil {
		t.Fatal("read durable upload after direct PUT")
	}
	if state != "pending" || !horizon.Equal(replayed.UploadRequest.ExpiresAt) {
		t.Fatalf("direct PUT changed DB state=%q or horizon=%s, want %s", state, horizon, replayed.UploadRequest.ExpiresAt)
	}
	assertGarageObject(t, ctx, storage, cfg.S3Bucket, objectKey, rawBody, "image/png")

	complete := func(idempotencyHeader bool) map[string]any {
		t.Helper()
		request, err := http.NewRequestWithContext(
			ctx,
			http.MethodPost,
			"http://"+listener.Addr().String()+location+"/complete",
			nil,
		)
		if err != nil {
			t.Fatal("build completion request")
		}
		if idempotencyHeader {
			request.Header.Set("Idempotency-Key", testOtherIdempotencyKey)
		}
		response, err := client.Do(request)
		if err != nil {
			t.Fatal("send completion request")
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 32<<10))
		response.Body.Close()
		if readErr != nil {
			t.Fatal("read completion response")
		}
		if response.StatusCode != http.StatusAccepted || response.Header.Get("Location") != location ||
			response.Header.Get("Content-Type") != "application/json" || response.Header.Get("Cache-Control") != "no-store" {
			t.Fatalf("completion response mismatch: status=%d Location=%q headers=%v body=%s", response.StatusCode, response.Header.Get("Location"), response.Header, body)
		}
		var representation map[string]any
		if err := json.Unmarshal(body, &representation); err != nil || len(representation) != 5 ||
			representation["upload_id"] != created.UploadID || representation["state"] != "finalizing" {
			t.Fatalf("completion representation=%#v decode_error=%v", representation, err)
		}
		for _, forbidden := range []string{"upload_request", "url", "staging", "completion_requested_at", "reconcile_after", "claim", "retry", "X-Amz-"} {
			if strings.Contains(strings.ToLower(string(body)), strings.ToLower(forbidden)) {
				t.Fatalf("completion representation disclosed %q", forbidden)
			}
		}
		return representation
	}
	complete(false)
	var completionRequestedAt time.Time
	var reconcileAfter time.Time
	if err := pool.QueryRow(ctx, `
		SELECT completion_requested_at, reconcile_after
		FROM uploads
		WHERE upload_id = $1::uuid`, created.UploadID).Scan(&completionRequestedAt, &reconcileAfter); err != nil {
		t.Fatal("read durable completion intent")
	}
	if !completionRequestedAt.Equal(reconcileAfter) {
		t.Fatalf("initial completion=%s reconcile_after=%s", completionRequestedAt, reconcileAfter)
	}
	complete(true)
	var replayedCompletionRequestedAt time.Time
	var replayedReconcileAfter time.Time
	if err := pool.QueryRow(ctx, `
		SELECT completion_requested_at, reconcile_after
		FROM uploads
		WHERE upload_id = $1::uuid`, created.UploadID).Scan(&replayedCompletionRequestedAt, &replayedReconcileAfter); err != nil {
		t.Fatal("read replayed completion intent")
	}
	if !replayedCompletionRequestedAt.Equal(completionRequestedAt) || replayedReconcileAfter.After(reconcileAfter) {
		t.Fatalf("completion replay changed intent=%t or delayed reconcile from %s to %s", !replayedCompletionRequestedAt.Equal(completionRequestedAt), reconcileAfter, replayedReconcileAfter)
	}
	finalStatus, err := client.Get("http://" + listener.Addr().String() + location)
	if err != nil {
		t.Fatal("GET finalizing upload")
	}
	finalStatusBody, readErr := io.ReadAll(io.LimitReader(finalStatus.Body, 32<<10))
	finalStatus.Body.Close()
	if readErr != nil || finalStatus.StatusCode != http.StatusOK || !strings.Contains(string(finalStatusBody), `"state":"finalizing"`) {
		t.Fatalf("finalizing status mismatch: status=%d read_error=%v body=%s", finalStatus.StatusCode, readErr, finalStatusBody)
	}
}

type createCallOutcome struct {
	result createUploadResult
	err    error
}

func responseErrorCode(t *testing.T, body []byte) string {
	t.Helper()
	var envelope struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("decode error response: %v body=%s", err, body)
	}
	return envelope.Error.Code
}

func newSingleConnectionPool(t *testing.T, ctx context.Context, databaseURL string) *pgxpool.Pool {
	t.Helper()
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal("parse integration PostgreSQL URL")
	}
	poolConfig.MaxConns = 1
	poolConfig.ConnConfig.RuntimeParams["application_name"] = "s3-media-upload-create-test-" + testUUID(t)
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Fatal("create single-connection PostgreSQL pool")
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatal("ping single-connection PostgreSQL pool")
	}
	if err := checkSchemaV1(ctx, pool); err != nil {
		pool.Close()
		t.Fatal("check schema from upload test pool")
	}
	return pool
}

func connectUploadTestPostgres(t *testing.T, ctx context.Context, databaseURL string) *pgx.Conn {
	t.Helper()
	connection, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal("connect independent PostgreSQL actor")
	}
	return connection
}

func singlePoolPID(t *testing.T, ctx context.Context, pool *pgxpool.Pool) uint32 {
	t.Helper()
	connection, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal("acquire worker connection")
	}
	pid := connection.Conn().PgConn().PID()
	connection.Release()
	return pid
}

func insertPendingUpload(
	ctx context.Context,
	tx pgx.Tx,
	uploadID, idempotencyKey string,
	size int64,
	contentType string,
) error {
	_, err := tx.Exec(ctx, `
		WITH sampled AS MATERIALIZED (
			SELECT clock_timestamp() AS created_at
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
			$3::bigint,
			$4::text,
			'pending',
			created_at,
			created_at + interval '24 hours',
			created_at + interval '15 minutes'
		FROM sampled`,
		uploadID,
		idempotencyKey,
		size,
		contentType,
	)
	return err
}

func testUploadPresigner(calls *atomic.Int64, entered chan<- struct{}) func(context.Context, string, string) (uploadRequest, error) {
	return func(_ context.Context, key, contentType string) (uploadRequest, error) {
		calls.Add(1)
		if entered != nil {
			select {
			case entered <- struct{}{}:
			default:
			}
		}
		expiresAt := time.Now().UTC().Add(15 * time.Minute).Truncate(time.Microsecond)
		return uploadRequest{
			Method:    http.MethodPut,
			URL:       (&url.URL{Scheme: "https", Host: "storage.invalid", Path: "/" + key, RawQuery: "signature=opaque"}).String(),
			Headers:   map[string]string{"Content-Type": contentType},
			ExpiresAt: expiresAt,
		}, nil
	}
}

func receiveCreateOutcome(t *testing.T, ctx context.Context, result <-chan createCallOutcome) createCallOutcome {
	t.Helper()
	select {
	case outcome := <-result:
		return outcome
	case <-ctx.Done():
		t.Fatal("create call did not finish")
		return createCallOutcome{}
	}
}

func waitForPostgresBlock(
	t *testing.T,
	ctx context.Context,
	observer *pgx.Conn,
	waiterPID, blockerPID uint32,
) {
	t.Helper()
	for ctx.Err() == nil {
		var blocked bool
		err := observer.QueryRow(ctx, `
			SELECT $1::integer = ANY(pg_blocking_pids($2::integer))`,
			int32(blockerPID),
			int32(waiterPID),
		).Scan(&blocked)
		if err != nil {
			t.Fatal("observe PostgreSQL blocker")
		}
		if blocked {
			return
		}
	}
	t.Fatal("waiter was not observed blocked by the holder")
}

func assertOneUploadForKey(
	t *testing.T,
	ctx context.Context,
	observer *pgx.Conn,
	idempotencyKey, wantUploadID string,
	wantHorizon time.Time,
) {
	t.Helper()
	var count int
	if err := observer.QueryRow(ctx, `
		SELECT count(*)
		FROM uploads
		WHERE idempotency_key = $1::uuid`, idempotencyKey).Scan(&count); err != nil {
		t.Fatal("count uploads for idempotency key")
	}
	var uploadID string
	var horizon time.Time
	if err := observer.QueryRow(ctx, `
		SELECT upload_id::text, max_write_expires_at
		FROM uploads
		WHERE idempotency_key = $1::uuid`, idempotencyKey).Scan(&uploadID, &horizon); err != nil {
		t.Fatal("read upload for idempotency key")
	}
	if count != 1 || uploadID != wantUploadID || !horizon.Equal(wantHorizon) {
		t.Fatalf("durable upload count=%d id=%s horizon=%s, want id=%s horizon=%s", count, uploadID, horizon, wantUploadID, wantHorizon)
	}
}

func cleanupUploadByIdempotencyKey(t *testing.T, pool *pgxpool.Pool, idempotencyKey string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Error("begin upload cleanup transaction")
		return
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, `
		DELETE FROM cleanup_tombstones
		WHERE upload_id IN (
			SELECT upload_id
			FROM uploads
			WHERE idempotency_key = $1::uuid
		)`, idempotencyKey); err != nil {
		t.Error("delete test upload tombstones")
		return
	}
	if _, err := tx.Exec(ctx, `DELETE FROM uploads WHERE idempotency_key = $1::uuid`, idempotencyKey); err != nil {
		t.Error("delete test upload")
		return
	}
	if err := tx.Commit(ctx); err != nil {
		t.Error("commit upload cleanup")
	}
}

func rawPresignedPUT(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	put uploadRequest,
	body []byte,
	overrideContentType string,
) int {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, put.Method, put.URL, bytes.NewReader(body))
	if err != nil {
		t.Fatal("build raw presigned PUT")
	}
	for name, value := range put.Headers {
		request.Header.Set(name, value)
	}
	if overrideContentType != "" {
		request.Header.Set("Content-Type", overrideContentType)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal("send raw presigned PUT")
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
	response.Body.Close()
	return response.StatusCode
}

func assertGarageObject(
	t *testing.T,
	ctx context.Context,
	client *s3.Client,
	bucket, key string,
	wantBody []byte,
	wantContentType string,
) {
	t.Helper()
	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		t.Fatal("HEAD Garage object")
	}
	if head.ContentLength == nil || *head.ContentLength != int64(len(wantBody)) || head.ContentType == nil || *head.ContentType != wantContentType || len(head.Metadata) != 0 {
		t.Fatalf("unexpected HEAD: length=%v type=%v metadata=%v", head.ContentLength, head.ContentType, head.Metadata)
	}
	object, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatal("GET Garage object")
	}
	body, readErr := io.ReadAll(io.LimitReader(object.Body, int64(len(wantBody))+1))
	closeErr := object.Body.Close()
	if readErr != nil || closeErr != nil {
		t.Fatal("read full Garage object")
	}
	if !bytes.Equal(body, wantBody) {
		t.Fatalf("full Garage GET bytes=%q, want %q", body, wantBody)
	}
}

func uploadTestChecksumName(name string) bool {
	lower := strings.ToLower(name)
	return lower == "content-md5" || strings.Contains(lower, "checksum") || strings.Contains(lower, "trailer")
}

func testUUID(t *testing.T) string {
	t.Helper()
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		t.Fatal("generate test UUID")
	}
	raw[6] = raw[6]&0x0f | 0x40
	raw[8] = raw[8]&0x3f | 0x80
	encoded := hex.EncodeToString(raw[:])
	return encoded[:8] + "-" +
		encoded[8:12] + "-" +
		encoded[12:16] + "-" +
		encoded[16:20] + "-" +
		encoded[20:]
}
