package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var errUploadSigningInvalid = errors.New("upload signing invalid")

// unknownEmpty deliberately does not implement io.Seeker or Len.
type unknownEmpty struct{}

func (unknownEmpty) Read([]byte) (int, error) { return 0, io.EOF }

func newS3Client(ctx context.Context, cfg config) (*s3.Client, error) {
	awsConfig, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.AWSRegion))
	if err != nil {
		return nil, errors.New("aws configuration load failed")
	}
	return s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		// Ignore endpoint URLs from the ambient AWS environment and shared config.
		options.BaseEndpoint = nil
		options.UsePathStyle = cfg.S3Endpoint != ""
		if cfg.S3Endpoint != "" {
			options.BaseEndpoint = aws.String(cfg.S3Endpoint)
		}
	}), nil
}

func headBucket(ctx context.Context, client *s3.Client, bucket string) error {
	_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	return err
}

func presignUploadPUT(
	ctx context.Context,
	presigner *s3.PresignClient,
	bucket, key, contentType string,
) (uploadRequest, error) {
	if presigner == nil {
		return uploadRequest{}, errUploadSigningInvalid
	}

	operationContext, cancel := context.WithTimeout(ctx, s3OperationTimeout)
	defer cancel()

	signed, err := presigner.PresignPutObject(operationContext, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		ContentType: aws.String(contentType),
		Body:        unknownEmpty{},
	}, s3.WithPresignExpires(15*time.Minute))
	if err != nil {
		return uploadRequest{}, errors.New("upload signing failed")
	}

	if signed == nil || signed.Method != http.MethodPut {
		return uploadRequest{}, errUploadSigningInvalid
	}
	parsedURL, err := url.Parse(signed.URL)
	if err != nil || !parsedURL.IsAbs() || parsedURL.Host == "" || parsedURL.RawQuery == "" {
		return uploadRequest{}, errUploadSigningInvalid
	}
	query := parsedURL.Query()
	expiresValues := query["X-Amz-Expires"]
	signedHeadersValues := query["X-Amz-SignedHeaders"]
	signingTimeValues := query["X-Amz-Date"]
	signatureValues := query["X-Amz-Signature"]
	if len(expiresValues) != 1 || expiresValues[0] != "900" ||
		len(signedHeadersValues) != 1 || signedHeadersValues[0] != "content-type;host" ||
		len(signingTimeValues) != 1 || len(signatureValues) != 1 || signatureValues[0] == "" {
		return uploadRequest{}, errUploadSigningInvalid
	}
	for name := range query {
		if strings.Contains(strings.ToLower(name), "checksum") {
			return uploadRequest{}, errUploadSigningInvalid
		}
	}

	expiresSeconds, err := strconv.ParseInt(expiresValues[0], 10, 64)
	if err != nil {
		return uploadRequest{}, errUploadSigningInvalid
	}
	signingTime, err := time.Parse("20060102T150405Z", signingTimeValues[0])
	if err != nil {
		return uploadRequest{}, errUploadSigningInvalid
	}

	contentTypes := signed.SignedHeader.Values("Content-Type")
	if len(contentTypes) != 1 || contentTypes[0] != contentType {
		return uploadRequest{}, errUploadSigningInvalid
	}

	return uploadRequest{
		Method:    signed.Method,
		URL:       signed.URL,
		Headers:   map[string]string{"Content-Type": contentTypes[0]},
		ExpiresAt: signingTime.Add(time.Duration(expiresSeconds) * time.Second),
	}, nil
}
