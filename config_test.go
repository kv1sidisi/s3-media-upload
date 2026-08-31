package main

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestConfig(t *testing.T) {
	base := map[string]string{
		"DATABASE_URL": "postgres://sentinel-user:sentinel-password@127.0.0.1/database",
		"S3_BUCKET":    "media-upload",
		"AWS_REGION":   "garage",
	}
	getenv := func(values map[string]string) func(string) string {
		return func(name string) string { return values[name] }
	}
	clone := func() map[string]string {
		values := make(map[string]string, len(base))
		for key, value := range base {
			values[key] = value
		}
		return values
	}

	cfg, err := loadConfigFrom(getenv(base))
	if err != nil {
		t.Fatalf("default config: %v", err)
	}
	if cfg.HTTPAddr != defaultHTTPAddr || cfg.FinalizeClaimLease != defaultFinalizeClaimLease {
		t.Fatal("unexpected config defaults")
	}

	valid := []struct {
		name     string
		variable string
		value    string
	}{
		{"IPv4 HTTP", "HTTP_ADDR", "127.0.0.2:8080"},
		{"IPv6 HTTP", "HTTP_ADDR", "[::1]:8080"},
		{"IPv4 S3", "S3_ENDPOINT", "http://127.0.0.1:3900"},
		{"IPv6 S3", "S3_ENDPOINT", "http://[::1]:3900"},
		{"claim lease", "FINALIZE_CLAIM_LEASE", "10.000000001s"},
	}
	for _, test := range valid {
		t.Run(test.name, func(t *testing.T) {
			values := clone()
			values[test.variable] = test.value
			if _, err := loadConfigFrom(getenv(values)); err != nil {
				t.Fatalf("valid config rejected: %v", err)
			}
		})
	}

	invalid := []struct {
		name     string
		variable string
		value    string
	}{
		{"missing database", "DATABASE_URL", ""},
		{"missing bucket", "S3_BUCKET", ""},
		{"missing region", "AWS_REGION", ""},
		{"wildcard HTTP", "HTTP_ADDR", "0.0.0.0:8080"},
		{"hostname HTTP", "HTTP_ADDR", "localhost:8080"},
		{"missing HTTP port", "HTTP_ADDR", "127.0.0.1"},
		{"zero HTTP port", "HTTP_ADDR", "127.0.0.1:0"},
		{"HTTPS endpoint", "S3_ENDPOINT", "https://127.0.0.1:3900"},
		{"remote endpoint", "S3_ENDPOINT", "http://192.0.2.1:3900"},
		{"endpoint userinfo", "S3_ENDPOINT", "http://user@127.0.0.1:3900"},
		{"endpoint path", "S3_ENDPOINT", "http://127.0.0.1:3900/path"},
		{"endpoint trailing slash", "S3_ENDPOINT", "http://127.0.0.1:3900/"},
		{"empty endpoint query", "S3_ENDPOINT", "http://127.0.0.1:3900?"},
		{"endpoint query", "S3_ENDPOINT", "http://127.0.0.1:3900?x=1"},
		{"empty endpoint fragment", "S3_ENDPOINT", "http://127.0.0.1:3900#"},
		{"endpoint fragment", "S3_ENDPOINT", "http://127.0.0.1:3900#x"},
		{"short lease", "FINALIZE_CLAIM_LEASE", "10s"},
		{"invalid lease", "FINALIZE_CLAIM_LEASE", "later"},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			values := clone()
			values[test.variable] = test.value
			_, err := loadConfigFrom(getenv(values))
			if err == nil {
				t.Fatal("invalid config accepted")
			}
			for _, secret := range []string{"sentinel-user", "sentinel-password"} {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("config error exposed %q", secret)
				}
			}
		})
	}

	values := clone()
	values["FINALIZE_CLAIM_LEASE"] = "11s"
	cfg, err = loadConfigFrom(getenv(values))
	if err != nil || cfg.FinalizeClaimLease != 11*time.Second {
		t.Fatalf("claim lease not loaded: %v, %v", cfg.FinalizeClaimLease, err)
	}
}

func TestS3ClientEndpointIsExplicit(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
	t.Setenv("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "false")

	for _, variable := range []string{"AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3"} {
		t.Run(variable, func(t *testing.T) {
			t.Setenv("AWS_ENDPOINT_URL", "")
			t.Setenv("AWS_ENDPOINT_URL_S3", "")
			t.Setenv(variable, "https://storage.invalid")
			client, err := newS3Client(context.Background(), config{AWSRegion: "us-east-1"})
			if err != nil {
				t.Fatal("create AWS S3 client")
			}
			if endpoint := client.Options().BaseEndpoint; endpoint != nil {
				t.Fatalf("ambient endpoint reached S3 client: %q", *endpoint)
			}
		})
	}

	t.Setenv("AWS_ENDPOINT_URL", "https://global.invalid")
	t.Setenv("AWS_ENDPOINT_URL_S3", "https://s3.invalid")
	client, err := newS3Client(context.Background(), config{
		AWSRegion:  "garage",
		S3Endpoint: "http://127.0.0.1:3900",
	})
	if err != nil {
		t.Fatal("create local S3 client")
	}
	options := client.Options()
	if options.BaseEndpoint == nil || *options.BaseEndpoint != "http://127.0.0.1:3900" || !options.UsePathStyle {
		t.Fatal("local endpoint was not applied explicitly")
	}
}
