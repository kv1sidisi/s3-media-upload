package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"image"
	_ "image/jpeg"
	_ "image/png"
	"io"
)

const (
	validationPolicyVersion       = 1
	maxImageAxis                  = 8_192
	maxImagePixels          int64 = 8_388_608
)

var errStagingRead = errors.New("staging object read failed")

type validatedImage struct {
	Bytes       []byte
	SHA256      [sha256.Size]byte
	SizeBytes   int64
	ContentType string
	Format      string
	Width       int
	Height      int
}

type validationFailure struct {
	Class    string
	Reason   string
	Phase    string
	Evidence map[string]any
}

func validateImage(reader io.Reader, contentLength, declaredSize int64, declaredContentType string) (validatedImage, *validationFailure, error) {
	if contentLength > maxUploadSizeBytes {
		return validatedImage{}, rejectImage("invalid_input", "object_too_large", "staging_read", map[string]any{
			"limit_bytes":   int64(maxUploadSizeBytes),
			"observed_size": contentLength,
		}), nil
	}
	if reader == nil {
		return validatedImage{}, nil, errStagingRead
	}

	encoded, err := io.ReadAll(io.LimitReader(reader, maxUploadSizeBytes+1))
	if err != nil {
		return validatedImage{}, nil, fmt.Errorf("%w: %w", errStagingRead, err)
	}
	return validateImageBytes(encoded, contentLength, declaredSize, declaredContentType)
}

func validateImageBytes(encoded []byte, contentLength, declaredSize int64, declaredContentType string) (validatedImage, *validationFailure, error) {
	if contentLength > maxUploadSizeBytes {
		return validatedImage{}, rejectImage("invalid_input", "object_too_large", "staging_read", map[string]any{
			"limit_bytes":   int64(maxUploadSizeBytes),
			"observed_size": contentLength,
		}), nil
	}
	observedSize := int64(len(encoded))
	if observedSize > maxUploadSizeBytes {
		return validatedImage{}, rejectImage("invalid_input", "object_too_large", "staging_read", map[string]any{
			"limit_bytes":   int64(maxUploadSizeBytes),
			"observed_size": observedSize,
		}), nil
	}
	if contentLength >= 0 && contentLength != observedSize {
		return validatedImage{}, nil, fmt.Errorf("%w: content length does not match body", errStagingRead)
	}
	digest := sha256.Sum256(encoded)
	digestHex := hex.EncodeToString(digest[:])
	if observedSize != declaredSize {
		return validatedImage{}, rejectImage("invalid_input", "declared_size_mismatch", "staging_read", map[string]any{
			"expected_size":   declaredSize,
			"observed_size":   observedSize,
			"observed_sha256": digestHex,
		}), nil
	}

	config, format, err := image.DecodeConfig(bytes.NewReader(encoded))
	if err != nil {
		return validatedImage{}, rejectImage("invalid_input", "invalid_image_encoding", "decode_config", map[string]any{
			"observed_size":   observedSize,
			"observed_sha256": digestHex,
		}), nil
	}
	contentType := contentTypeForImageFormat(format)
	if contentType == "" {
		return validatedImage{}, rejectImage("invalid_input", "invalid_image_encoding", "decode_config", map[string]any{
			"format":          format,
			"observed_size":   observedSize,
			"observed_sha256": digestHex,
		}), nil
	}
	if contentType != declaredContentType {
		return validatedImage{}, rejectImage("invalid_input", "declared_content_type_mismatch", "decode_config", map[string]any{
			"expected_content_type": declaredContentType,
			"observed_content_type": contentType,
			"format":                format,
			"observed_size":         observedSize,
			"observed_sha256":       digestHex,
		}), nil
	}
	if config.Width < 1 || config.Width > maxImageAxis || config.Height < 1 || config.Height > maxImageAxis {
		return validatedImage{}, rejectImage("invalid_input", "dimensions_limit_exceeded", "decode_config", map[string]any{
			"axis_limit":      maxImageAxis,
			"format":          format,
			"width":           config.Width,
			"height":          config.Height,
			"observed_size":   observedSize,
			"observed_sha256": digestHex,
		}), nil
	}
	pixels := int64(config.Width) * int64(config.Height)
	if pixels > maxImagePixels {
		return validatedImage{}, rejectImage("invalid_input", "pixel_limit_exceeded", "decode_config", map[string]any{
			"pixel_limit":     maxImagePixels,
			"format":          format,
			"width":           config.Width,
			"height":          config.Height,
			"pixels":          pixels,
			"observed_size":   observedSize,
			"observed_sha256": digestHex,
		}), nil
	}

	decoded, decodedFormat, err := image.Decode(bytes.NewReader(encoded))
	if err != nil {
		return validatedImage{}, rejectImage("invalid_input", "malformed_image", "decode", map[string]any{
			"format":          format,
			"width":           config.Width,
			"height":          config.Height,
			"observed_size":   observedSize,
			"observed_sha256": digestHex,
		}), nil
	}
	bounds := decoded.Bounds()
	if decodedFormat != format || bounds.Dx() != config.Width || bounds.Dy() != config.Height {
		return validatedImage{}, rejectImage("internal_invariant", "decoder_invariant_mismatch", "decode", map[string]any{
			"expected_format": format,
			"observed_format": decodedFormat,
			"expected_width":  config.Width,
			"observed_width":  bounds.Dx(),
			"expected_height": config.Height,
			"observed_height": bounds.Dy(),
			"observed_size":   observedSize,
			"observed_sha256": digestHex,
		}), nil
	}

	return validatedImage{
		Bytes:       encoded,
		SHA256:      digest,
		SizeBytes:   observedSize,
		ContentType: contentType,
		Format:      format,
		Width:       config.Width,
		Height:      config.Height,
	}, nil, nil
}

func contentTypeForImageFormat(format string) string {
	switch format {
	case "jpeg":
		return "image/jpeg"
	case "png":
		return "image/png"
	default:
		return ""
	}
}

func rejectImage(class, reason, phase string, evidence map[string]any) *validationFailure {
	if evidence == nil {
		evidence = make(map[string]any, 1)
	}
	evidence["policy_version"] = validationPolicyVersion
	return &validationFailure{Class: class, Reason: reason, Phase: phase, Evidence: evidence}
}
