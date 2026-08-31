package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	errObjectLengthMismatch  = errors.New("object content length mismatch")
	errContentSigningInvalid = errors.New("content signing invalid")
)

type objectSnapshot struct {
	Bytes         []byte
	ContentLength int64
}

func captureS3Object(
	ctx context.Context,
	client *s3.Client,
	bucket, key string,
	maxBytes int64,
) (snapshot objectSnapshot, err error) {
	operationContext, cancel := context.WithTimeout(ctx, s3OperationTimeout)
	defer cancel()

	object, err := client.GetObject(operationContext, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return objectSnapshot{}, err
	}
	if object == nil || object.Body == nil {
		return objectSnapshot{}, errObjectLengthMismatch
	}
	defer func() {
		if closeErr := object.Body.Close(); err == nil {
			err = closeErr
		}
	}()

	snapshot.ContentLength = -1
	if object.ContentLength != nil {
		snapshot.ContentLength = *object.ContentLength
	}
	if snapshot.ContentLength > maxBytes {
		return snapshot, nil
	}

	snapshot.Bytes, err = io.ReadAll(io.LimitReader(object.Body, maxBytes+1))
	if err != nil {
		return objectSnapshot{}, err
	}
	if int64(len(snapshot.Bytes)) <= maxBytes && snapshot.ContentLength >= 0 &&
		int64(len(snapshot.Bytes)) != snapshot.ContentLength {
		return objectSnapshot{}, errObjectLengthMismatch
	}
	return snapshot, nil
}

func putCandidateObject(
	ctx context.Context,
	client *s3.Client,
	bucket, key string,
	raw []byte,
	contentType string,
) error {
	operationContext, cancel := context.WithTimeout(ctx, s3OperationTimeout)
	defer cancel()

	_, err := client.PutObject(operationContext, &s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(raw),
		ContentLength: aws.Int64(int64(len(raw))),
		ContentType:   aws.String(contentType),
	})
	return err
}

func presignContentGET(
	ctx context.Context,
	presigner *s3.PresignClient,
	bucket, key string,
) (string, time.Time, error) {
	if presigner == nil {
		return "", time.Time{}, errContentSigningInvalid
	}

	operationContext, cancel := context.WithTimeout(ctx, s3OperationTimeout)
	defer cancel()

	signed, err := presigner.PresignGetObject(operationContext, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(5*time.Minute))
	if err != nil {
		return "", time.Time{}, err
	}
	if signed == nil || signed.Method != http.MethodGet {
		return "", time.Time{}, errContentSigningInvalid
	}

	parsedURL, err := url.Parse(signed.URL)
	if err != nil || !parsedURL.IsAbs() || parsedURL.Host == "" || parsedURL.User != nil ||
		(parsedURL.Scheme != "http" && parsedURL.Scheme != "https") || parsedURL.Fragment != "" {
		return "", time.Time{}, errContentSigningInvalid
	}
	for name, values := range signed.SignedHeader {
		if !strings.EqualFold(name, "Host") || len(values) != 1 || !strings.EqualFold(values[0], parsedURL.Host) {
			return "", time.Time{}, errContentSigningInvalid
		}
	}
	query := parsedURL.Query()
	expiresValues := query["X-Amz-Expires"]
	signedHeadersValues := query["X-Amz-SignedHeaders"]
	signingTimeValues := query["X-Amz-Date"]
	signatureValues := query["X-Amz-Signature"]
	if len(expiresValues) != 1 || expiresValues[0] != "300" ||
		len(signedHeadersValues) != 1 || signedHeadersValues[0] != "host" ||
		len(signingTimeValues) != 1 || signingTimeValues[0] == "" ||
		len(signatureValues) != 1 || signatureValues[0] == "" {
		return "", time.Time{}, errContentSigningInvalid
	}

	expiresSeconds, err := strconv.ParseInt(expiresValues[0], 10, 64)
	if err != nil {
		return "", time.Time{}, errContentSigningInvalid
	}
	signingTime, err := time.Parse("20060102T150405Z", signingTimeValues[0])
	if err != nil {
		return "", time.Time{}, errContentSigningInvalid
	}
	return signed.URL, signingTime.Add(time.Duration(expiresSeconds) * time.Second), nil
}
