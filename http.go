package main

import (
	"context"
	"encoding/json"
	"expvar"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"
)

const readinessCheckTimeout = 2 * time.Second

type application struct {
	logger       *slog.Logger
	postgresPing func(context.Context) error
	s3HeadBucket func(context.Context) error
	stopping     atomic.Bool
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (w *statusRecorder) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusRecorder) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(body)
}

func (a *application) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	started := time.Now()
	recorder := &statusRecorder{ResponseWriter: writer}
	route := operationalRoute(request.URL.Path)
	outcome := "ok"
	errorCode := ""

	switch {
	case route == "":
		outcome = "error"
		errorCode = "not_found"
		writeError(recorder, http.StatusNotFound, errorCode, "route not found")
	case request.Method != http.MethodGet:
		outcome = "error"
		errorCode = "method_not_allowed"
		recorder.Header().Set("Allow", http.MethodGet)
		writeError(recorder, http.StatusMethodNotAllowed, errorCode, "method not allowed")
	case route == "/livez":
		if a.stopping.Load() {
			outcome = "error"
			errorCode = "service_unavailable"
			writeJSON(recorder, http.StatusServiceUnavailable, map[string]string{"status": "stopping"})
		} else {
			writeJSON(recorder, http.StatusOK, map[string]string{"status": "ok"})
		}
	case route == "/readyz":
		if !a.ready(recorder, request) {
			outcome = "error"
			errorCode = "service_unavailable"
		}
	case route == "/debug/vars":
		recorder.Header().Set("Cache-Control", "no-store")
		expvar.Handler().ServeHTTP(recorder, request)
	}

	attrs := []any{
		"route", routeLabel(route),
		"method", methodLabel(request.Method),
		"status", recorder.status,
		"duration_ms", time.Since(started).Milliseconds(),
		"outcome", outcome,
	}
	if errorCode != "" {
		attrs = append(attrs, "error_code", errorCode)
	}
	a.logger.Info("http.request_finished", attrs...)
}

func operationalRoute(path string) string {
	switch path {
	case "/livez", "/readyz", "/debug/vars":
		return path
	default:
		return ""
	}
}

func routeLabel(route string) string {
	if route == "" {
		return "unknown"
	}
	return route
}

func methodLabel(method string) string {
	switch method {
	case http.MethodGet,
		http.MethodHead,
		http.MethodPost,
		http.MethodPut,
		http.MethodPatch,
		http.MethodDelete,
		http.MethodOptions:
		return method
	default:
		return "other"
	}
}

func (a *application) ready(writer http.ResponseWriter, request *http.Request) bool {
	postgresStatus := "ok"
	s3Status := "ok"

	started := time.Now()
	postgresContext, cancelPostgres := context.WithTimeout(request.Context(), readinessCheckTimeout)
	postgresError := a.postgresPing(postgresContext)
	cancelPostgres()
	if postgresError != nil {
		postgresStatus = "error"
		a.logger.Info(
			"readiness.failed",
			"dependency", "postgres",
			"error_class", "unavailable",
			"duration_ms", time.Since(started).Milliseconds(),
		)
	}

	started = time.Now()
	s3Context, cancelS3 := context.WithTimeout(request.Context(), readinessCheckTimeout)
	s3Error := a.s3HeadBucket(s3Context)
	cancelS3()
	if s3Error != nil {
		s3Status = "error"
		a.logger.Info(
			"readiness.failed",
			"dependency", "s3",
			"error_class", "unavailable",
			"duration_ms", time.Since(started).Milliseconds(),
		)
	}

	status := http.StatusOK
	result := "ok"
	if postgresError != nil || s3Error != nil {
		status = http.StatusServiceUnavailable
		result = "error"
	}
	writeJSON(writer, status, struct {
		Status string `json:"status"`
		Checks struct {
			Postgres string `json:"postgres"`
			S3       string `json:"s3"`
		} `json:"checks"`
	}{
		Status: result,
		Checks: struct {
			Postgres string `json:"postgres"`
			S3       string `json:"s3"`
		}{Postgres: postgresStatus, S3: s3Status},
	})
	return status == http.StatusOK
}

func writeError(writer http.ResponseWriter, status int, code, message string) {
	writeJSON(writer, status, struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}{
		Error: struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		}{Code: code, Message: message},
	})
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Cache-Control", "no-store")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

func newServiceMetrics() *expvar.Map {
	root := expvar.NewMap("media_upload_service")
	started := new(expvar.Int)
	started.Set(time.Now().Unix())
	root.Set("process_started_unix_seconds", started)
	root.Set("upload_transitions_total", fixedExpvarMap([]string{
		"none_to_pending",
		"pending_to_finalizing",
		"pending_to_expired",
		"finalizing_to_ready",
		"finalizing_to_rejected",
		"finalizing_to_expired",
	}))

	retryKeys := make([]string, 0, 10)
	for _, owner := range []string{"finalizer", "cleanup"} {
		for _, errorClass := range []string{
			"transient",
			"ambiguous",
			"auth",
			"configuration",
			"other_deterministic",
		} {
			retryKeys = append(retryKeys, owner+"_"+errorClass)
		}
	}
	root.Set("retries_scheduled_total", fixedExpvarMap(retryKeys))
	root.Set("validation_rejects_total", fixedExpvarMap([]string{
		"object_too_large",
		"dimensions_limit_exceeded",
		"pixel_limit_exceeded",
		"declared_size_mismatch",
		"invalid_image_encoding",
		"declared_content_type_mismatch",
		"malformed_image",
		"decoder_invariant_mismatch",
		"candidate_integrity_mismatch",
	}))
	root.Set("cleanup_attempts_total", new(expvar.Int))
	root.Set("cleanup_outcomes_total", fixedExpvarMap([]string{
		"delete_succeeded",
		"confirmed_absent",
		"transient_error",
		"ambiguous_error",
		"auth_error",
		"configuration_error",
		"other_deterministic_error",
	}))
	root.Set("cleanup_due", new(expvar.Int))
	root.Set("cleanup_oldest_due_age_seconds", new(expvar.Int))
	root.Set("cleanup_snapshot_unix_seconds", new(expvar.Int))
	return root
}

func fixedExpvarMap(keys []string) *expvar.Map {
	values := new(expvar.Map)
	values.Init()
	for _, key := range keys {
		values.Set(key, new(expvar.Int))
	}
	return values
}

var serviceMetrics = newServiceMetrics()
