package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"reflect"
	"testing"
	"testing/iotest"
)

func TestValidationPolicy(t *testing.T) {
	tests := []struct {
		format      string
		contentType string
	}{
		{"jpeg", "image/jpeg"},
		{"png", "image/png"},
	}
	for _, test := range tests {
		t.Run(test.format, func(t *testing.T) {
			encoded := encodeValidationImage(t, test.format)
			got, failure, err := validateImage(bytes.NewReader(encoded), int64(len(encoded)), int64(len(encoded)), test.contentType)
			if err != nil || failure != nil {
				t.Fatalf("validateImage failure=%#v error=%v", failure, err)
			}
			if !bytes.Equal(got.Bytes, encoded) || got.SHA256 != sha256.Sum256(encoded) {
				t.Fatal("validated bytes or SHA-256 changed")
			}
			if got.SizeBytes != int64(len(encoded)) || got.ContentType != test.contentType || got.Format != test.format || got.Width != 2 || got.Height != 3 {
				t.Fatalf("validated image=%#v", got)
			}
		})
	}

	t.Run("honest trailing bytes stay exact", func(t *testing.T) {
		encoded := append(encodeValidationImage(t, "png"), []byte("trailing bytes")...)
		got, failure, err := validateImage(bytes.NewReader(encoded), int64(len(encoded)), int64(len(encoded)), "image/png")
		if err != nil || failure != nil || !bytes.Equal(got.Bytes, encoded) || got.SHA256 != sha256.Sum256(encoded) {
			t.Fatalf("validated=%#v failure=%#v error=%v", got, failure, err)
		}
	})

	t.Run("axis boundary", func(t *testing.T) {
		var encoded bytes.Buffer
		if err := png.Encode(&encoded, image.NewGray(image.Rect(0, 0, maxImageAxis, 1))); err != nil {
			t.Fatal("encode axis-boundary PNG")
		}
		got, failure, err := validateImage(bytes.NewReader(encoded.Bytes()), int64(encoded.Len()), int64(encoded.Len()), "image/png")
		if err != nil || failure != nil || got.Width != maxImageAxis || got.Height != 1 {
			t.Fatalf("validated=%#v failure=%#v error=%v", got, failure, err)
		}
	})

	t.Run("candidate mismatch", func(t *testing.T) {
		expected := []byte("expected")
		observed := []byte("observed")
		candidate := trackedCandidate{SHA256: sha256.Sum256(expected), EncodedSize: int64(len(expected))}
		snapshot := objectSnapshot{Bytes: observed, ContentLength: int64(len(observed))}
		if candidateMatchesSnapshot(candidate, snapshot) {
			t.Fatal("different candidate bytes matched")
		}
		failure := candidateMismatchFailure(candidate, snapshot)
		if failure.Class != "internal_invariant" || failure.Reason != "candidate_integrity_mismatch" || failure.Phase != "candidate_verify" {
			t.Fatalf("candidate mismatch failure=%#v", failure)
		}
	})
}

func TestValidateImagePolicyFailures(t *testing.T) {
	validPNG := encodeValidationImage(t, "png")
	oversized := make([]byte, maxUploadSizeBytes+1)
	tests := []struct {
		name          string
		encoded       []byte
		contentLength int64
		declaredSize  int64
		declaredType  string
		want          *validationFailure
	}{
		{
			name:          "empty object",
			encoded:       nil,
			contentLength: 0,
			declaredSize:  1,
			declaredType:  "image/png",
			want: expectedValidationFailure("invalid_input", "declared_size_mismatch", "staging_read", map[string]any{
				"expected_size":   int64(1),
				"observed_size":   int64(0),
				"observed_sha256": validationDigest(nil),
			}),
		},
		{
			name:          "bounded read detects byte after limit",
			encoded:       oversized,
			contentLength: -1,
			declaredSize:  int64(len(oversized)),
			declaredType:  "image/png",
			want: expectedValidationFailure("invalid_input", "object_too_large", "staging_read", map[string]any{
				"limit_bytes":   int64(maxUploadSizeBytes),
				"observed_size": int64(maxUploadSizeBytes + 1),
			}),
		},
		{
			name:          "zero dimension encoding",
			encoded:       validationPNGConfig(0, 1),
			contentLength: int64(len(validationPNGConfig(0, 1))),
			declaredSize:  int64(len(validationPNGConfig(0, 1))),
			declaredType:  "image/png",
			want: expectedValidationFailure("invalid_input", "invalid_image_encoding", "decode_config", map[string]any{
				"observed_size":   int64(len(validationPNGConfig(0, 1))),
				"observed_sha256": validationDigest(validationPNGConfig(0, 1)),
			}),
		},
		{
			name:          "declared size mismatch",
			encoded:       validPNG,
			contentLength: int64(len(validPNG)),
			declaredSize:  int64(len(validPNG) + 1),
			declaredType:  "image/png",
			want: expectedValidationFailure("invalid_input", "declared_size_mismatch", "staging_read", map[string]any{
				"expected_size":   int64(len(validPNG) + 1),
				"observed_size":   int64(len(validPNG)),
				"observed_sha256": validationDigest(validPNG),
			}),
		},
		{
			name:          "unsupported encoding",
			encoded:       []byte("not an image"),
			contentLength: 12,
			declaredSize:  12,
			declaredType:  "image/png",
			want: expectedValidationFailure("invalid_input", "invalid_image_encoding", "decode_config", map[string]any{
				"observed_size":   int64(12),
				"observed_sha256": validationDigest([]byte("not an image")),
			}),
		},
		{
			name:          "declared content type mismatch",
			encoded:       validPNG,
			contentLength: int64(len(validPNG)),
			declaredSize:  int64(len(validPNG)),
			declaredType:  "image/jpeg",
			want: expectedValidationFailure("invalid_input", "declared_content_type_mismatch", "decode_config", map[string]any{
				"expected_content_type": "image/jpeg",
				"observed_content_type": "image/png",
				"format":                "png",
				"observed_size":         int64(len(validPNG)),
				"observed_sha256":       validationDigest(validPNG),
			}),
		},
		{
			name:          "axis limit",
			encoded:       validationPNGConfig(maxImageAxis+1, 1),
			contentLength: int64(len(validationPNGConfig(maxImageAxis+1, 1))),
			declaredSize:  int64(len(validationPNGConfig(maxImageAxis+1, 1))),
			declaredType:  "image/png",
			want: expectedValidationFailure("invalid_input", "dimensions_limit_exceeded", "decode_config", map[string]any{
				"axis_limit":      maxImageAxis,
				"format":          "png",
				"width":           maxImageAxis + 1,
				"height":          1,
				"observed_size":   int64(len(validationPNGConfig(maxImageAxis+1, 1))),
				"observed_sha256": validationDigest(validationPNGConfig(maxImageAxis+1, 1)),
			}),
		},
		{
			name:          "pixel limit",
			encoded:       validationPNGConfig(4_096, 2_049),
			contentLength: int64(len(validationPNGConfig(4_096, 2_049))),
			declaredSize:  int64(len(validationPNGConfig(4_096, 2_049))),
			declaredType:  "image/png",
			want: expectedValidationFailure("invalid_input", "pixel_limit_exceeded", "decode_config", map[string]any{
				"pixel_limit":     maxImagePixels,
				"format":          "png",
				"width":           4_096,
				"height":          2_049,
				"pixels":          int64(4_096 * 2_049),
				"observed_size":   int64(len(validationPNGConfig(4_096, 2_049))),
				"observed_sha256": validationDigest(validationPNGConfig(4_096, 2_049)),
			}),
		},
		{
			name:          "full decode rejects truncated image",
			encoded:       validationPNGConfig(1, 1),
			contentLength: int64(len(validationPNGConfig(1, 1))),
			declaredSize:  int64(len(validationPNGConfig(1, 1))),
			declaredType:  "image/png",
			want: expectedValidationFailure("invalid_input", "malformed_image", "decode", map[string]any{
				"format":          "png",
				"width":           1,
				"height":          1,
				"observed_size":   int64(len(validationPNGConfig(1, 1))),
				"observed_sha256": validationDigest(validationPNGConfig(1, 1)),
			}),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, failure, err := validateImage(bytes.NewReader(test.encoded), test.contentLength, test.declaredSize, test.declaredType)
			if err != nil {
				t.Fatalf("validateImage error=%v", err)
			}
			if !reflect.DeepEqual(failure, test.want) {
				t.Fatalf("failure=%#v, want %#v", failure, test.want)
			}
			if !reflect.DeepEqual(got, validatedImage{}) {
				t.Fatalf("rejected image returned data: %#v", got)
			}
		})
	}
}

func TestValidateImageReturnsStagingReadErrorsForRetry(t *testing.T) {
	dependencyError := errors.New("dependency failed")
	tests := []struct {
		name          string
		reader        *bytes.Reader
		contentLength int64
	}{
		{"content length exceeds body", bytes.NewReader([]byte("x")), 2},
		{"content length is shorter than body", bytes.NewReader([]byte("xx")), 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, failure, err := validateImage(test.reader, test.contentLength, 1, "image/png")
			if !errors.Is(err, errStagingRead) || failure != nil || !reflect.DeepEqual(got, validatedImage{}) {
				t.Fatalf("validated=%#v failure=%#v error=%v", got, failure, err)
			}
		})
	}

	got, failure, err := validateImage(iotest.ErrReader(dependencyError), -1, 1, "image/png")
	if !errors.Is(err, errStagingRead) || !errors.Is(err, dependencyError) || failure != nil || !reflect.DeepEqual(got, validatedImage{}) {
		t.Fatalf("validated=%#v failure=%#v error=%v", got, failure, err)
	}
}

func TestValidateImageRejectsOversizedContentLengthWithoutReading(t *testing.T) {
	dependencyError := errors.New("reader must not be called")
	got, failure, err := validateImage(iotest.ErrReader(dependencyError), maxUploadSizeBytes+1, 1, "image/png")
	want := expectedValidationFailure("invalid_input", "object_too_large", "staging_read", map[string]any{
		"limit_bytes":   int64(maxUploadSizeBytes),
		"observed_size": int64(maxUploadSizeBytes + 1),
	})
	if err != nil || !reflect.DeepEqual(failure, want) || !reflect.DeepEqual(got, validatedImage{}) {
		t.Fatalf("validated=%#v failure=%#v error=%v", got, failure, err)
	}
}

func encodeValidationImage(t *testing.T, format string) []byte {
	t.Helper()
	encoded := new(bytes.Buffer)
	source := image.NewRGBA(image.Rect(0, 0, 2, 3))
	source.Set(1, 2, color.RGBA{R: 0x12, G: 0x34, B: 0x56, A: 0xff})
	var err error
	switch format {
	case "jpeg":
		err = jpeg.Encode(encoded, source, nil)
	case "png":
		err = png.Encode(encoded, source)
	default:
		t.Fatalf("unsupported test format %q", format)
	}
	if err != nil {
		t.Fatalf("encode %s: %v", format, err)
	}
	return encoded.Bytes()
}

func validationPNGConfig(width, height int) []byte {
	ihdr := make([]byte, 13)
	binary.BigEndian.PutUint32(ihdr[0:4], uint32(width))
	binary.BigEndian.PutUint32(ihdr[4:8], uint32(height))
	ihdr[8] = 8
	ihdr[9] = 2
	encoded := []byte("\x89PNG\r\n\x1a\n")
	encoded = appendValidationPNGChunk(encoded, "IHDR", ihdr)
	return appendValidationPNGChunk(encoded, "IDAT", nil)
}

func appendValidationPNGChunk(encoded []byte, name string, data []byte) []byte {
	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(len(data)))
	encoded = append(encoded, length...)
	chunk := append([]byte(name), data...)
	encoded = append(encoded, chunk...)
	checksum := make([]byte, 4)
	binary.BigEndian.PutUint32(checksum, crc32.ChecksumIEEE(chunk))
	return append(encoded, checksum...)
}

func expectedValidationFailure(class, reason, phase string, evidence map[string]any) *validationFailure {
	if evidence == nil {
		evidence = map[string]any{}
	}
	evidence["policy_version"] = 1
	return &validationFailure{Class: class, Reason: reason, Phase: phase, Evidence: evidence}
}

func validationDigest(encoded []byte) string {
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}
