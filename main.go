package main

import (
	"context"
	"errors"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	shutdownTimeout = 10 * time.Second
	maxHeaderBytes  = 12288
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, logger); err != nil {
		logger.Error("service.start_failed", "component", "service")
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	pool, err := openPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	storage, err := newS3Client(ctx, cfg)
	if err != nil {
		return err
	}
	app := &application{
		logger:       logger,
		postgresPing: pool.Ping,
		s3HeadBucket: func(ctx context.Context) error {
			return headBucket(ctx, storage, cfg.S3Bucket)
		},
	}
	server := newHTTPServer(cfg.HTTPAddr, app)
	listener, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return errors.New("http listener creation failed")
	}
	serveResult := make(chan error, 1)
	go func() {
		serveResult <- server.Serve(listener)
	}()
	logger.Info("service.started", "component", "service")

	select {
	case err := <-serveResult:
		if !errors.Is(err, http.ErrServerClosed) {
			return errors.New("http server stopped unexpectedly")
		}
		return nil
	case <-ctx.Done():
	}

	app.stopping.Store(true)
	started := time.Now()
	logger.Info(
		"service.stopping",
		"component", "service",
		"shutdown_reason", "context_canceled",
		"duration_ms", int64(0),
	)
	shutdownContext, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownContext); err != nil {
		_ = server.Close()
		<-serveResult
		return errors.New("http server shutdown failed")
	}
	if err := <-serveResult; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return errors.New("http server stopped unexpectedly")
	}
	logger.Info(
		"service.stopped",
		"component", "service",
		"shutdown_reason", "context_canceled",
		"duration_ms", time.Since(started).Milliseconds(),
	)
	return nil
}

func newHTTPServer(address string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              address,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    maxHeaderBytes,
		// net/http's default error log can expose raw errors and panic stacks.
		ErrorLog: log.New(io.Discard, "", 0),
	}
}
