package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"expvar"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"sync/atomic"
	"time"
)

const (
	readinessCheckTimeout    = 2 * time.Second
	maxCreateUploadBodyBytes = 16_384
	maxUploadSizeBytes       = 10_485_760
)

var (
	errIdempotencyKeyReused = errors.New("idempotency key reused")
	errUploadNotFound       = errors.New("upload not found")
	errServiceUnavailable   = errors.New("service unavailable")
	idempotencyKeyPattern   = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
)

type application struct {
	logger         *slog.Logger
	postgresPing   func(context.Context) error
	s3HeadBucket   func(context.Context) error
	createUpload   func(context.Context, createUploadCommand) (createUploadResult, error)
	completeUpload func(context.Context, string) (completeUploadResult, error)
	getUpload      func(context.Context, string) (uploadRepresentation, error)
	getContent     func(context.Context, string) (contentReadResult, error)
	stopping       atomic.Bool
}

type createUploadCommand struct {
	IdempotencyKey string
	SizeBytes      int64
	ContentType    string
}

type uploadRequest struct {
	Method    string            `json:"method"`
	URL       string            `json:"url"`
	Headers   map[string]string `json:"headers"`
	ExpiresAt time.Time         `json:"expires_at"`
}

type uploadRepresentation struct {
	UploadID            string         `json:"upload_id"`
	State               string         `json:"state"`
	DeclaredSizeBytes   int64          `json:"declared_size_bytes"`
	DeclaredContentType string         `json:"declared_content_type"`
	UploadDeadline      time.Time      `json:"upload_deadline"`
	UploadRequest       *uploadRequest `json:"upload_request,omitempty"`
	Failure             *uploadFailure `json:"failure,omitempty"`
	Image               *uploadImage   `json:"image,omitempty"`
}

type uploadFailure struct {
	Code string `json:"code"`
}

type uploadImage struct {
	SizeBytes   int64  `json:"size_bytes"`
	ContentType string `json:"content_type"`
	Width       int    `json:"width"`
	Height      int    `json:"height"`
}

type contentReadResult struct {
	UploadID    string
	State       string
	FailureCode string
	URL         string
	ExpiresAt   time.Time
}

type createUploadResult struct {
	Upload  uploadRepresentation
	Created bool
}

type completeUploadResult struct {
	Upload     uploadRepresentation
	Transition string
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
	route, uploadID := applicationRoute(request.URL.Path)
	loggedUploadID := ""
	errorCode := ""
	allowedMethod := http.MethodGet
	if route == "/uploads" || route == "/uploads/{upload_id}/complete" {
		allowedMethod = http.MethodPost
	}

	switch {
	case route == "":
		errorCode = "not_found"
		writeError(recorder, http.StatusNotFound, errorCode, "route not found")
	case request.Method != allowedMethod:
		errorCode = "method_not_allowed"
		recorder.Header().Set("Allow", allowedMethod)
		writeError(recorder, http.StatusMethodNotAllowed, errorCode, "method not allowed")
	case route == "/livez":
		if a.stopping.Load() {
			errorCode = "service_unavailable"
			writeJSON(recorder, http.StatusServiceUnavailable, map[string]string{"status": "stopping"})
		} else {
			writeJSON(recorder, http.StatusOK, map[string]string{"status": "ok"})
		}
	case route == "/readyz":
		if !a.ready(recorder, request) {
			errorCode = "service_unavailable"
		}
	case route == "/debug/vars":
		recorder.Header().Set("Cache-Control", "no-store")
		expvar.Handler().ServeHTTP(recorder, request)
	case route == "/uploads":
		loggedUploadID, errorCode = a.serveCreateUpload(recorder, request)
	case route == "/uploads/{upload_id}/complete":
		loggedUploadID, errorCode = a.serveCompleteUpload(recorder, request, uploadID)
	case route == "/uploads/{upload_id}/content":
		loggedUploadID, errorCode = a.serveGetContent(recorder, request, uploadID)
	case route == "/uploads/{upload_id}":
		loggedUploadID, errorCode = a.serveGetUpload(recorder, request, uploadID)
	}

	outcome := "ok"
	if errorCode != "" {
		outcome = "error"
	}
	attrs := []any{
		"route", routeLabel(route),
		"method", methodLabel(request.Method),
		"status", recorder.status,
		"duration_ms", time.Since(started).Milliseconds(),
		"outcome", outcome,
	}
	if loggedUploadID != "" {
		attrs = append(attrs, "upload_id", loggedUploadID)
	}
	if errorCode != "" {
		attrs = append(attrs, "error_code", errorCode)
	}
	a.logger.Info("http.request_finished", attrs...)
}

func applicationRoute(path string) (string, string) {
	if route := operationalRoute(path); route != "" {
		return route, ""
	}
	if path == "/uploads" {
		return path, ""
	}
	const prefix = "/uploads/"
	if strings.HasPrefix(path, prefix) {
		uploadID := strings.TrimPrefix(path, prefix)
		if strings.HasSuffix(uploadID, "/content") {
			uploadID = strings.TrimSuffix(uploadID, "/content")
			if uploadID != "" && !strings.Contains(uploadID, "/") {
				return "/uploads/{upload_id}/content", uploadID
			}
			return "", ""
		}
		if strings.HasSuffix(uploadID, "/complete") {
			uploadID = strings.TrimSuffix(uploadID, "/complete")
			if uploadID != "" && !strings.Contains(uploadID, "/") {
				return "/uploads/{upload_id}/complete", uploadID
			}
			return "", ""
		}
		if uploadID != "" && !strings.Contains(uploadID, "/") {
			return "/uploads/{upload_id}", uploadID
		}
	}
	return "", ""
}

func (a *application) serveCreateUpload(writer http.ResponseWriter, request *http.Request) (string, string) {
	idempotencyKeys := request.Header.Values("Idempotency-Key")
	if len(idempotencyKeys) != 1 || !idempotencyKeyPattern.MatchString(idempotencyKeys[0]) {
		writeError(writer, http.StatusBadRequest, "invalid_idempotency_key", "invalid idempotency key")
		return "", "invalid_idempotency_key"
	}
	contentTypes := request.Header.Values("Content-Type")
	if len(contentTypes) != 1 || contentTypes[0] != "application/json" {
		writeError(writer, http.StatusUnsupportedMediaType, "unsupported_media_type", "unsupported media type")
		return "", "unsupported_media_type"
	}

	body := http.MaxBytesReader(writer, request.Body, maxCreateUploadBodyBytes)
	encoded, err := io.ReadAll(body)
	_ = body.Close()
	if err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			writeError(writer, http.StatusRequestEntityTooLarge, "request_too_large", "request body is too large")
			return "", "request_too_large"
		}
		writeError(writer, http.StatusBadRequest, "invalid_request", "invalid request body")
		return "", "invalid_request"
	}
	command, ok := decodeCreateUploadBody(encoded)
	if !ok {
		writeError(writer, http.StatusBadRequest, "invalid_request", "invalid request body")
		return "", "invalid_request"
	}
	if command.SizeBytes < 1 || command.SizeBytes > maxUploadSizeBytes ||
		(command.ContentType != "image/jpeg" && command.ContentType != "image/png") {
		writeError(writer, http.StatusUnprocessableEntity, "invalid_upload_declaration", "invalid upload declaration")
		return "", "invalid_upload_declaration"
	}
	command.IdempotencyKey = idempotencyKeys[0]

	result, err := a.createUpload(request.Context(), command)
	if err != nil {
		switch {
		case errors.Is(err, errIdempotencyKeyReused):
			writeError(writer, http.StatusUnprocessableEntity, "idempotency_key_reused", "idempotency key was reused")
			return "", "idempotency_key_reused"
		case errors.Is(err, errServiceUnavailable):
			writeError(writer, http.StatusServiceUnavailable, "service_unavailable", "service unavailable")
			return "", "service_unavailable"
		default:
			writeError(writer, http.StatusInternalServerError, "internal_error", "internal server error")
			return "", "internal_error"
		}
	}

	if result.Created {
		serviceMetrics.Get("upload_transitions_total").(*expvar.Map).Get("none_to_pending").(*expvar.Int).Add(1)
		a.logger.Info(
			"upload.transition",
			"upload_id", result.Upload.UploadID,
			"state_from", "none",
			"state_to", "pending",
			"trigger", "create",
		)
	}
	if result.Upload.UploadRequest != nil {
		source := "replay"
		if result.Created {
			source = "initial"
		}
		a.logger.Info(
			"capability.issued",
			"upload_id", result.Upload.UploadID,
			"kind", "upload_put",
			"source", source,
			"expires_at", result.Upload.UploadRequest.ExpiresAt,
		)
	}

	status := http.StatusOK
	if result.Created {
		status = http.StatusCreated
		writer.Header().Set("Location", "/uploads/"+result.Upload.UploadID)
	}
	writeJSON(writer, status, result.Upload)
	return result.Upload.UploadID, ""
}

func decodeCreateUploadBody(encoded []byte) (createUploadCommand, bool) {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return createUploadCommand{}, false
	}
	var command createUploadCommand
	var sizeSeen, contentTypeSeen bool
	for decoder.More() {
		token, err = decoder.Token()
		field, ok := token.(string)
		if err != nil || !ok {
			return createUploadCommand{}, false
		}
		switch field {
		case "size_bytes":
			if sizeSeen {
				return createUploadCommand{}, false
			}
			sizeSeen = true
			var size *int64
			if decoder.Decode(&size) != nil || size == nil {
				return createUploadCommand{}, false
			}
			command.SizeBytes = *size
		case "content_type":
			if contentTypeSeen {
				return createUploadCommand{}, false
			}
			contentTypeSeen = true
			var contentType *string
			if decoder.Decode(&contentType) != nil || contentType == nil {
				return createUploadCommand{}, false
			}
			command.ContentType = *contentType
		default:
			return createUploadCommand{}, false
		}
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') || !sizeSeen || !contentTypeSeen {
		return createUploadCommand{}, false
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return createUploadCommand{}, false
	}
	return command, true
}

func (a *application) serveCompleteUpload(writer http.ResponseWriter, request *http.Request, uploadID string) (string, string) {
	body := http.MaxBytesReader(writer, request.Body, 0)
	_, err := io.ReadAll(body)
	_ = body.Close()
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", "request body must be empty")
		return "", "invalid_request"
	}

	result, err := a.completeUpload(request.Context(), uploadID)
	if err != nil {
		switch {
		case errors.Is(err, errUploadNotFound):
			writeError(writer, http.StatusNotFound, "upload_not_found", "upload not found")
			return "", "upload_not_found"
		case errors.Is(err, errServiceUnavailable):
			writeError(writer, http.StatusServiceUnavailable, "service_unavailable", "service unavailable")
			return "", "service_unavailable"
		default:
			writeError(writer, http.StatusInternalServerError, "internal_error", "internal server error")
			return "", "internal_error"
		}
	}
	result.Upload.UploadRequest = nil
	if result.Transition != "" &&
		!((result.Transition == "pending_to_finalizing" && result.Upload.State == "finalizing") ||
			(result.Transition == "pending_to_expired" && result.Upload.State == "expired")) {
		writeError(writer, http.StatusInternalServerError, "internal_error", "internal server error")
		return "", "internal_error"
	}

	status := http.StatusOK
	if result.Upload.State == "finalizing" {
		status = http.StatusAccepted
		writer.Header().Set("Location", "/uploads/"+result.Upload.UploadID)
	} else if result.Upload.State != "ready" && result.Upload.State != "rejected" && result.Upload.State != "expired" {
		writeError(writer, http.StatusInternalServerError, "internal_error", "internal server error")
		return "", "internal_error"
	}

	if result.Transition != "" {
		serviceMetrics.Get("upload_transitions_total").(*expvar.Map).Get(result.Transition).(*expvar.Int).Add(1)
		attributes := []any{
			"upload_id", result.Upload.UploadID,
			"state_from", "pending",
			"state_to", result.Upload.State,
			"trigger", "completion",
		}
		if result.Upload.State == "expired" {
			attributes = append(attributes, "reason_code", "upload_deadline_elapsed")
		}
		a.logger.Info("upload.transition", attributes...)
	}

	writeJSON(writer, status, result.Upload)
	return result.Upload.UploadID, ""
}

func (a *application) serveGetUpload(writer http.ResponseWriter, request *http.Request, uploadID string) (string, string) {
	body := http.MaxBytesReader(writer, request.Body, 0)
	_, err := io.ReadAll(body)
	_ = body.Close()
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", "request body must be empty")
		return "", "invalid_request"
	}

	upload, err := a.getUpload(request.Context(), uploadID)
	if err != nil {
		switch {
		case errors.Is(err, errUploadNotFound):
			writeError(writer, http.StatusNotFound, "upload_not_found", "upload not found")
			return "", "upload_not_found"
		case errors.Is(err, errServiceUnavailable):
			writeError(writer, http.StatusServiceUnavailable, "service_unavailable", "service unavailable")
			return "", "service_unavailable"
		default:
			writeError(writer, http.StatusInternalServerError, "internal_error", "internal server error")
			return "", "internal_error"
		}
	}
	upload.UploadRequest = nil
	writeJSON(writer, http.StatusOK, upload)
	return upload.UploadID, ""
}

func (a *application) serveGetContent(writer http.ResponseWriter, request *http.Request, uploadID string) (string, string) {
	body := http.MaxBytesReader(writer, request.Body, 0)
	_, err := io.ReadAll(body)
	_ = body.Close()
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", "request body must be empty")
		return "", "invalid_request"
	}

	result, err := a.getContent(request.Context(), uploadID)
	if err != nil {
		switch {
		case errors.Is(err, errUploadNotFound):
			writeError(writer, http.StatusNotFound, "upload_not_found", "upload not found")
			return "", "upload_not_found"
		case errors.Is(err, errServiceUnavailable):
			writeError(writer, http.StatusServiceUnavailable, "service_unavailable", "service unavailable")
			return "", "service_unavailable"
		default:
			writeError(writer, http.StatusInternalServerError, "internal_error", "internal server error")
			return "", "internal_error"
		}
	}

	switch result.State {
	case "ready":
		if result.URL == "" {
			writeError(writer, http.StatusInternalServerError, "internal_error", "internal server error")
			return result.UploadID, "internal_error"
		}
		writer.Header().Set("Location", result.URL)
		writer.Header().Set("Cache-Control", "private, no-store")
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		writer.WriteHeader(http.StatusTemporaryRedirect)
		a.logger.Info(
			"capability.issued",
			"upload_id", result.UploadID,
			"kind", "content_get",
			"source", "ready_read",
			"expires_at", result.ExpiresAt,
		)
		return result.UploadID, ""
	case "pending", "finalizing":
		writeError(writer, http.StatusConflict, "upload_not_ready", "upload is not ready")
		return result.UploadID, "upload_not_ready"
	case "rejected":
		switch result.FailureCode {
		case "image_too_large":
			writeError(writer, http.StatusUnprocessableEntity, result.FailureCode, "image is too large")
		case "invalid_image":
			writeError(writer, http.StatusUnprocessableEntity, result.FailureCode, "invalid image")
		case "upload_processing_failed":
			writeError(writer, http.StatusInternalServerError, result.FailureCode, "upload processing failed")
		default:
			writeError(writer, http.StatusInternalServerError, "internal_error", "internal server error")
			return result.UploadID, "internal_error"
		}
		return result.UploadID, result.FailureCode
	case "expired":
		writeError(writer, http.StatusGone, "upload_expired", "upload expired")
		return result.UploadID, "upload_expired"
	default:
		writeError(writer, http.StatusInternalServerError, "internal_error", "internal server error")
		return result.UploadID, "internal_error"
	}
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
