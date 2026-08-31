package main

import (
	"errors"
	"net"
	"net/netip"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultHTTPAddr           = "127.0.0.1:8080"
	defaultFinalizeClaimLease = 30 * time.Second
	s3OperationTimeout        = 10 * time.Second
)

type config struct {
	HTTPAddr           string
	DatabaseURL        string
	S3Bucket           string
	AWSRegion          string
	S3Endpoint         string
	FinalizeClaimLease time.Duration
}

func loadConfig() (config, error) {
	return loadConfigFrom(os.Getenv)
}

func loadConfigFrom(getenv func(string) string) (config, error) {
	cfg := config{
		HTTPAddr:           strings.TrimSpace(getenv("HTTP_ADDR")),
		DatabaseURL:        getenv("DATABASE_URL"),
		S3Bucket:           strings.TrimSpace(getenv("S3_BUCKET")),
		AWSRegion:          strings.TrimSpace(getenv("AWS_REGION")),
		S3Endpoint:         strings.TrimSpace(getenv("S3_ENDPOINT")),
		FinalizeClaimLease: defaultFinalizeClaimLease,
	}
	if cfg.HTTPAddr == "" {
		cfg.HTTPAddr = defaultHTTPAddr
	}
	if cfg.DatabaseURL == "" {
		return config{}, errors.New("missing DATABASE_URL")
	}
	if cfg.S3Bucket == "" {
		return config{}, errors.New("missing S3_BUCKET")
	}
	if cfg.AWSRegion == "" {
		return config{}, errors.New("missing AWS_REGION")
	}
	if !validLoopbackAddress(cfg.HTTPAddr) {
		return config{}, errors.New("invalid HTTP_ADDR")
	}
	if cfg.S3Endpoint != "" && !validS3Endpoint(cfg.S3Endpoint) {
		return config{}, errors.New("invalid S3_ENDPOINT")
	}
	if rawLease := strings.TrimSpace(getenv("FINALIZE_CLAIM_LEASE")); rawLease != "" {
		lease, err := time.ParseDuration(rawLease)
		if err != nil {
			return config{}, errors.New("invalid FINALIZE_CLAIM_LEASE")
		}
		cfg.FinalizeClaimLease = lease
	}
	if cfg.FinalizeClaimLease < s3OperationTimeout+time.Microsecond {
		return config{}, errors.New("claim lease does not exceed the S3 operation timeout")
	}
	return cfg, nil
}

func validLoopbackAddress(value string) bool {
	host, port, err := net.SplitHostPort(value)
	if err != nil || port == "" {
		return false
	}
	parsedPort, err := strconv.ParseUint(port, 10, 16)
	if err != nil || parsedPort == 0 {
		return false
	}
	address, err := netip.ParseAddr(host)
	return err == nil && address.IsLoopback()
}

func validS3Endpoint(value string) bool {
	if strings.Contains(value, "#") {
		return false
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "http" || parsed.Host == "" || parsed.User != nil {
		return false
	}
	if parsed.Path != "" || parsed.RawPath != "" || parsed.ForceQuery || parsed.RawQuery != "" || parsed.Fragment != "" {
		return false
	}
	return validLoopbackAddress(parsed.Host)
}
