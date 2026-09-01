package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type storageFinalizeHTTPClientFunc func(*http.Request) (*http.Response, error)

func (function storageFinalizeHTTPClientFunc) Do(request *http.Request) (*http.Response, error) {
	return function(request)
}

type storageFinalizeTestBody struct {
	reader *bytes.Reader
	reads  int
	closed bool
}

func (body *storageFinalizeTestBody) Read(buffer []byte) (int, error) {
	body.reads++
	return body.reader.Read(buffer)
}

func (body *storageFinalizeTestBody) Close() error {
	body.closed = true
	return nil
}

func newStorageFinalizeTestClient(httpClient s3.HTTPClient) *s3.Client {
	return s3.New(s3.Options{
		BaseEndpoint: aws.String("https://storage.invalid"),
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     "test-access-key",
				SecretAccessKey: "test-secret-key",
				Source:          "storage-finalize-test",
			}, nil
		}),
		HTTPClient:                 httpClient,
		Region:                     "us-east-1",
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		Retryer:                    aws.NopRetryer{},
		UsePathStyle:               true,
	})
}

func TestCaptureS3ObjectBoundsAndLength(t *testing.T) {
	declaredThree := int64(3)
	declaredFour := int64(4)
	tests := []struct {
		name          string
		body          string
		declared      *int64
		maxBytes      int64
		wantBytes     []byte
		wantLength    int64
		wantMismatch  bool
		wantBodyRead  bool
		wantRemaining int
	}{
		{
			name:         "exact",
			body:         "abc",
			declared:     &declaredThree,
			maxBytes:     3,
			wantBytes:    []byte("abc"),
			wantLength:   3,
			wantBodyRead: true,
		},
		{
			name:          "declared over limit is not read",
			body:          "abcd",
			declared:      &declaredFour,
			maxBytes:      3,
			wantLength:    4,
			wantBodyRead:  false,
			wantRemaining: 4,
		},
		{
			name:          "actual body is bounded at max plus one",
			body:          "abcde",
			declared:      &declaredThree,
			maxBytes:      3,
			wantBytes:     []byte("abcd"),
			wantLength:    3,
			wantBodyRead:  true,
			wantRemaining: 1,
		},
		{
			name:         "short body mismatches declaration",
			body:         "ab",
			declared:     &declaredThree,
			maxBytes:     3,
			wantMismatch: true,
			wantBodyRead: true,
		},
		{
			name:         "unknown declaration",
			body:         "abc",
			maxBytes:     3,
			wantBytes:    []byte("abc"),
			wantLength:   -1,
			wantBodyRead: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := &storageFinalizeTestBody{reader: bytes.NewReader([]byte(test.body))}
			client := newStorageFinalizeTestClient(storageFinalizeHTTPClientFunc(func(request *http.Request) (*http.Response, error) {
				if request.Method != http.MethodGet || request.Header.Get("Accept-Encoding") != "identity" {
					t.Fatalf("request method=%q accept-encoding=%q", request.Method, request.Header.Get("Accept-Encoding"))
				}
				header := make(http.Header)
				contentLength := int64(-1)
				if test.declared != nil {
					contentLength = *test.declared
					header.Set("Content-Length", strconv.FormatInt(contentLength, 10))
				}
				return &http.Response{
					StatusCode:    http.StatusOK,
					Status:        "200 OK",
					Header:        header,
					Body:          body,
					ContentLength: contentLength,
					Request:       request,
				}, nil
			}))

			snapshot, err := captureS3Object(context.Background(), client, "bucket", "key", test.maxBytes)
			if !errors.Is(err, errObjectLengthMismatch) && test.wantMismatch {
				t.Fatalf("error=%v, want length mismatch", err)
			}
			if err != nil && !test.wantMismatch {
				t.Fatalf("capture object: %v", err)
			}
			if err == nil && test.wantMismatch {
				t.Fatal("capture object succeeded, want length mismatch")
			}
			if !test.wantMismatch && (!bytes.Equal(snapshot.Bytes, test.wantBytes) || snapshot.ContentLength != test.wantLength) {
				t.Fatalf("snapshot=%#v, want bytes=%q length=%d", snapshot, test.wantBytes, test.wantLength)
			}
			if (body.reads > 0) != test.wantBodyRead {
				t.Fatalf("body reads=%d, want read=%t", body.reads, test.wantBodyRead)
			}
			if !body.closed || body.reader.Len() != test.wantRemaining {
				t.Fatalf("body closed=%t remaining=%d, want remaining=%d", body.closed, body.reader.Len(), test.wantRemaining)
			}
		})
	}
}

func TestPutCandidateObjectSendsExactBytes(t *testing.T) {
	raw := []byte{0x00, 0x01, 0xfe, 0xff}
	client := newStorageFinalizeTestClient(storageFinalizeHTTPClientFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodPut || request.ContentLength != int64(len(raw)) || request.Header.Get("Content-Type") != "image/png" {
			t.Fatalf("request method=%q length=%d content-type=%q", request.Method, request.ContentLength, request.Header.Get("Content-Type"))
		}
		actual, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatal("read candidate request body")
		}
		if !bytes.Equal(actual, raw) {
			t.Fatalf("candidate bytes=%x, want %x", actual, raw)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header:     make(http.Header),
			Body:       http.NoBody,
			Request:    request,
		}, nil
	}))

	if err := putCandidateObject(context.Background(), client, "bucket", "candidate", raw, "image/png"); err != nil {
		t.Fatalf("put candidate: %v", err)
	}
}

func TestCandidateHiddenPutResultCanBeVerified(t *testing.T) {
	raw := []byte("validated candidate bytes")
	var stored []byte
	client := newStorageFinalizeTestClient(storageFinalizeHTTPClientFunc(func(request *http.Request) (*http.Response, error) {
		switch request.Method {
		case http.MethodPut:
			body, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatal("read hidden PUT body")
			}
			stored = append([]byte(nil), body...)
			return nil, errors.New("response was hidden after apply")
		case http.MethodGet:
			return &http.Response{
				StatusCode:    http.StatusOK,
				Status:        "200 OK",
				Header:        make(http.Header),
				Body:          io.NopCloser(bytes.NewReader(stored)),
				ContentLength: int64(len(stored)),
				Request:       request,
			}, nil
		default:
			t.Fatalf("unexpected method %s", request.Method)
			return nil, errors.New("unexpected method")
		}
	}))
	if err := putCandidateObject(context.Background(), client, "bucket", "candidate", raw, "image/png"); err == nil {
		t.Fatal("hidden PUT result unexpectedly reported success")
	}
	snapshot, err := captureS3Object(context.Background(), client, "bucket", "candidate", maxUploadSizeBytes)
	if err != nil {
		t.Fatalf("verify hidden PUT result: %v", err)
	}
	candidate := trackedCandidate{SHA256: sha256.Sum256(raw), EncodedSize: int64(len(raw))}
	if !candidateMatchesSnapshot(candidate, snapshot) {
		t.Fatal("applied hidden PUT did not reconcile from full GET")
	}
}

func TestPresignContentGET(t *testing.T) {
	client := newStorageFinalizeTestClient(storageFinalizeHTTPClientFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("presigning unexpectedly sent an HTTP request")
		return nil, errors.New("unexpected HTTP request")
	}))

	rawURL, expiresAt, err := presignContentGET(context.Background(), s3.NewPresignClient(client), "bucket", "media/id/digest")
	if err != nil {
		t.Fatalf("presign content GET: %v", err)
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal("parse presigned GET URL")
	}
	query := parsed.Query()
	if !parsed.IsAbs() || parsed.Scheme != "https" || parsed.Host != "storage.invalid" ||
		query.Get("X-Amz-Expires") != "300" || query.Get("X-Amz-SignedHeaders") != "host" ||
		query.Get("X-Amz-Signature") == "" || query.Get("X-Amz-Date") == "" {
		t.Fatalf("unexpected presigned GET URL: %s", rawURL)
	}
	signedAt, err := time.Parse("20060102T150405Z", query.Get("X-Amz-Date"))
	if err != nil || !expiresAt.Equal(signedAt.Add(5*time.Minute)) {
		t.Fatalf("expires_at=%s signing time=%q", expiresAt, query.Get("X-Amz-Date"))
	}
}
