package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestIntegrationReadinessOutageRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("requires migrated PostgreSQL and Garage")
	}
	cfg := integrationConfig(t)
	postgresURL, err := url.Parse(cfg.DatabaseURL)
	if err != nil || postgresURL.Host == "" {
		t.Fatal("integration database URL is invalid")
	}
	postgresProxy := newTestTCPProxy(t, postgresURL.Host)
	postgresURL.Host = postgresProxy.address()
	cfg.DatabaseURL = postgresURL.String()

	storageURL, err := url.Parse(cfg.S3Endpoint)
	if err != nil || storageURL.Host == "" {
		t.Fatal("loopback S3_ENDPOINT is required for Garage integration")
	}
	storageProxy := newTestTCPProxy(t, storageURL.Host)
	storageURL.Host = storageProxy.address()
	cfg.S3Endpoint = storageURL.String()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := openPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		t.Fatal("open PostgreSQL through test outage gate")
	}
	defer pool.Close()
	storage, err := newS3Client(ctx, cfg)
	if err != nil {
		t.Fatal("create Garage client through test outage gate")
	}
	app := &application{
		logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		postgresPing: pool.Ping,
		s3HeadBucket: func(ctx context.Context) error {
			return headBucket(ctx, storage, cfg.S3Bucket)
		},
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("start readiness listener")
	}
	server := newHTTPServer(listener.Addr().String(), app)
	serveResult := make(chan error, 1)
	go func() { serveResult <- server.Serve(listener) }()
	defer func() {
		_ = server.Close()
		<-serveResult
	}()
	client := &http.Client{Timeout: 5 * time.Second}
	baseURL := "http://" + listener.Addr().String()

	waitHTTPStatus(t, client, baseURL+"/readyz", http.StatusOK)
	postgresProxy.setEnabled(false)
	requireBoundedUnavailable(t, client, baseURL+"/readyz")
	waitHTTPStatus(t, client, baseURL+"/livez", http.StatusOK)
	postgresProxy.setEnabled(true)
	waitHTTPStatus(t, client, baseURL+"/readyz", http.StatusOK)

	storageProxy.setEnabled(false)
	requireBoundedUnavailable(t, client, baseURL+"/readyz")
	waitHTTPStatus(t, client, baseURL+"/livez", http.StatusOK)
	storageProxy.setEnabled(true)
	waitHTTPStatus(t, client, baseURL+"/readyz", http.StatusOK)
}

func requireBoundedUnavailable(t *testing.T, client *http.Client, endpoint string) {
	t.Helper()
	started := time.Now()
	response, err := client.Get(endpoint)
	if err != nil {
		t.Fatal("readiness outage request failed")
	}
	_, _ = io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("readiness outage returned %d", response.StatusCode)
	}
	if time.Since(started) > 4500*time.Millisecond {
		t.Fatal("readiness outage response exceeded the four-second dependency budget")
	}
}

func waitHTTPStatus(t *testing.T, client *http.Client, endpoint string, want int) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for {
		response, err := client.Get(endpoint)
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			response.Body.Close()
			if response.StatusCode == want {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("endpoint did not reach status %d", want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

type testTCPProxy struct {
	listener    net.Listener
	target      string
	enabled     atomic.Bool
	mu          sync.Mutex
	connections map[net.Conn]struct{}
}

func newTestTCPProxy(t *testing.T, target string) *testTCPProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("start test outage gate")
	}
	proxy := &testTCPProxy{
		listener:    listener,
		target:      target,
		connections: make(map[net.Conn]struct{}),
	}
	proxy.enabled.Store(true)
	go proxy.accept()
	t.Cleanup(proxy.close)
	return proxy
}

func (p *testTCPProxy) address() string {
	return p.listener.Addr().String()
}

func (p *testTCPProxy) accept() {
	for {
		connection, err := p.listener.Accept()
		if err != nil {
			return
		}
		go p.forward(connection)
	}
}

func (p *testTCPProxy) forward(client net.Conn) {
	if !p.enabled.Load() {
		client.Close()
		return
	}
	upstream, err := net.DialTimeout("tcp", p.target, time.Second)
	if err != nil {
		client.Close()
		return
	}
	p.mu.Lock()
	if !p.enabled.Load() {
		p.mu.Unlock()
		client.Close()
		upstream.Close()
		return
	}
	p.connections[client] = struct{}{}
	p.connections[upstream] = struct{}{}
	p.mu.Unlock()

	done := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(upstream, client)
		done <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(client, upstream)
		done <- struct{}{}
	}()
	<-done
	client.Close()
	upstream.Close()
	<-done

	p.mu.Lock()
	delete(p.connections, client)
	delete(p.connections, upstream)
	p.mu.Unlock()
}

func (p *testTCPProxy) setEnabled(enabled bool) {
	p.enabled.Store(enabled)
	if enabled {
		return
	}
	p.mu.Lock()
	for connection := range p.connections {
		connection.Close()
	}
	p.mu.Unlock()
}

func (p *testTCPProxy) close() {
	p.setEnabled(false)
	p.listener.Close()
}
