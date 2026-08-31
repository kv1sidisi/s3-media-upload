package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func testGetContentOutcomes(t *testing.T) {
	expiresAt := time.Date(2026, time.August, 31, 20, 15, 0, 0, time.UTC)
	const capabilityURL = "http://storage.invalid/media/opaque?X-Amz-Signature=must-not-enter-logs"
	tests := []struct {
		name       string
		result     contentReadResult
		err        error
		wantStatus int
		wantCode   string
	}{
		{"ready", contentReadResult{UploadID: testUploadID, State: "ready", URL: capabilityURL, ExpiresAt: expiresAt}, nil, http.StatusTemporaryRedirect, ""},
		{"pending", contentReadResult{UploadID: testUploadID, State: "pending"}, nil, http.StatusConflict, "upload_not_ready"},
		{"finalizing", contentReadResult{UploadID: testUploadID, State: "finalizing"}, nil, http.StatusConflict, "upload_not_ready"},
		{"too large", contentReadResult{UploadID: testUploadID, State: "rejected", FailureCode: "image_too_large"}, nil, http.StatusUnprocessableEntity, "image_too_large"},
		{"invalid image", contentReadResult{UploadID: testUploadID, State: "rejected", FailureCode: "invalid_image"}, nil, http.StatusUnprocessableEntity, "invalid_image"},
		{"processing failed", contentReadResult{UploadID: testUploadID, State: "rejected", FailureCode: "upload_processing_failed"}, nil, http.StatusInternalServerError, "upload_processing_failed"},
		{"expired", contentReadResult{UploadID: testUploadID, State: "expired"}, nil, http.StatusGone, "upload_expired"},
		{"unknown rejection", contentReadResult{UploadID: testUploadID, State: "rejected", FailureCode: "raw-rejection-secret"}, nil, http.StatusInternalServerError, "internal_error"},
		{"unknown state", contentReadResult{UploadID: testUploadID, State: "raw-state-secret"}, nil, http.StatusInternalServerError, "internal_error"},
		{"ready without capability", contentReadResult{UploadID: testUploadID, State: "ready"}, nil, http.StatusInternalServerError, "internal_error"},
		{"missing", contentReadResult{}, errUploadNotFound, http.StatusNotFound, "upload_not_found"},
		{"unavailable", contentReadResult{}, errServiceUnavailable, http.StatusServiceUnavailable, "service_unavailable"},
		{"internal failure", contentReadResult{}, errors.New("raw-content-error-secret"), http.StatusInternalServerError, "internal_error"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var logs bytes.Buffer
			app := &application{
				logger: slog.New(slog.NewJSONHandler(&logs, nil)),
				getContent: func(_ context.Context, uploadID string) (contentReadResult, error) {
					if uploadID != testUploadID {
						t.Fatalf("upload ID=%q", uploadID)
					}
					return test.result, test.err
				},
			}
			recorder := httptest.NewRecorder()
			app.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/uploads/"+testUploadID+"/content", nil))

			if recorder.Code != test.wantStatus {
				t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
			}
			if test.wantCode == "" {
				if recorder.Body.Len() != 0 ||
					recorder.Header().Get("Location") != capabilityURL ||
					recorder.Header().Get("Cache-Control") != "private, no-store" ||
					recorder.Header().Get("X-Content-Type-Options") != "nosniff" ||
					recorder.Header().Get("Content-Type") != "" {
					t.Fatalf("redirect contract mismatch: headers=%v body=%q", recorder.Header(), recorder.Body.String())
				}
				if !strings.Contains(logs.String(), `"msg":"capability.issued"`) ||
					!strings.Contains(logs.String(), `"kind":"content_get"`) ||
					!strings.Contains(logs.String(), `"source":"ready_read"`) ||
					!strings.Contains(logs.String(), testUploadID) {
					t.Fatalf("capability issuance was not logged: %s", logs.String())
				}
			} else {
				if recorder.Header().Get("Cache-Control") != "no-store" ||
					recorder.Header().Get("Content-Type") != "application/json" {
					t.Fatalf("error headers=%v", recorder.Header())
				}
				var response struct {
					Error struct {
						Code string `json:"code"`
					} `json:"error"`
				}
				if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil || response.Error.Code != test.wantCode {
					t.Fatalf("error response=%s, want code %q", recorder.Body.String(), test.wantCode)
				}
				if strings.Contains(logs.String(), `"msg":"capability.issued"`) {
					t.Fatalf("failed read logged capability issuance: %s", logs.String())
				}
			}
			for _, secret := range []string{capabilityURL, "X-Amz-Signature", "raw-content-error-secret", "raw-rejection-secret", "raw-state-secret"} {
				if strings.Contains(logs.String(), secret) {
					t.Fatalf("logs disclosed %q: %s", secret, logs.String())
				}
			}
		})
	}
}

func testGetContentExactRouteAndEmptyRequest(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		wantStatus int
		wantAllow  string
		wantCalls  int
		wantCode   string
	}{
		{"exact route", http.MethodGet, "/uploads/opaque-id/content", "", http.StatusConflict, "", 1, "upload_not_ready"},
		{"wrong method", http.MethodPost, "/uploads/opaque-id/content", "", http.StatusMethodNotAllowed, http.MethodGet, 0, "method_not_allowed"},
		{"HEAD is not GET", http.MethodHead, "/uploads/opaque-id/content", "", http.StatusMethodNotAllowed, http.MethodGet, 0, "method_not_allowed"},
		{"request body is not empty", http.MethodGet, "/uploads/opaque-id/content", `{}`, http.StatusBadRequest, "", 0, "invalid_request"},
		{"trailing slash", http.MethodGet, "/uploads/opaque-id/content/", "", http.StatusNotFound, "", 0, "not_found"},
		{"empty ID", http.MethodGet, "/uploads//content", "", http.StatusNotFound, "", 0, "not_found"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			app := &application{
				logger: slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)),
				getContent: func(_ context.Context, uploadID string) (contentReadResult, error) {
					calls++
					if uploadID != "opaque-id" {
						t.Fatalf("upload ID=%q", uploadID)
					}
					return contentReadResult{UploadID: uploadID, State: "pending"}, nil
				},
				getUpload: func(context.Context, string) (uploadRepresentation, error) {
					t.Fatal("content route reached generic upload handler")
					return uploadRepresentation{}, nil
				},
			}
			recorder := httptest.NewRecorder()
			app.ServeHTTP(recorder, httptest.NewRequest(test.method, test.path, strings.NewReader(test.body)))

			if recorder.Code != test.wantStatus || recorder.Header().Get("Allow") != test.wantAllow || calls != test.wantCalls {
				t.Fatalf("status=%d Allow=%q calls=%d body=%s", recorder.Code, recorder.Header().Get("Allow"), calls, recorder.Body.String())
			}
			var response struct {
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil || response.Error.Code != test.wantCode {
				t.Fatalf("error response=%s, want code %q", recorder.Body.String(), test.wantCode)
			}
		})
	}
}

func testUploadRepresentationImageIsOptional(t *testing.T) {
	deadline := time.Date(2026, time.August, 31, 12, 0, 0, 0, time.UTC)
	base := uploadRepresentation{
		UploadID:            testUploadID,
		State:               "pending",
		DeclaredSizeBytes:   11,
		DeclaredContentType: "image/jpeg",
		UploadDeadline:      deadline,
	}
	tests := []struct {
		name  string
		value uploadRepresentation
		want  map[string]any
	}{
		{
			name:  "not ready omits image",
			value: base,
			want: map[string]any{
				"upload_id":             testUploadID,
				"state":                 "pending",
				"declared_size_bytes":   float64(11),
				"declared_content_type": "image/jpeg",
				"upload_deadline":       deadline.Format(time.RFC3339),
			},
		},
		{
			name: "ready includes exact image",
			value: func() uploadRepresentation {
				ready := base
				ready.State = "ready"
				ready.Image = &uploadImage{SizeBytes: 9, ContentType: "image/jpeg", Width: 3, Height: 2}
				return ready
			}(),
			want: map[string]any{
				"upload_id":             testUploadID,
				"state":                 "ready",
				"declared_size_bytes":   float64(11),
				"declared_content_type": "image/jpeg",
				"upload_deadline":       deadline.Format(time.RFC3339),
				"image": map[string]any{
					"size_bytes":   float64(9),
					"content_type": "image/jpeg",
					"width":        float64(3),
					"height":       float64(2),
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := json.Marshal(test.value)
			if err != nil {
				t.Fatal(err)
			}
			var got map[string]any
			if err := json.Unmarshal(encoded, &got); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("representation=%#v, want %#v", got, test.want)
			}
		})
	}
}
