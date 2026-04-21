// Package oci implements an OCI Distribution v2 registry frontend that
// translates OCI API calls into warp's storage.Backend interface.
package oci

import (
	"encoding/json"
	"net/http"
)

// OCI spec error codes.
// https://github.com/opencontainers/distribution-spec/blob/main/spec.md#error-codes
const (
	ErrCodeBlobUnknown         = "BLOB_UNKNOWN"
	ErrCodeBlobUploadInvalid   = "BLOB_UPLOAD_INVALID"
	ErrCodeBlobUploadUnknown   = "BLOB_UPLOAD_UNKNOWN"
	ErrCodeDigestInvalid       = "DIGEST_INVALID"
	ErrCodeManifestBlobUnknown = "MANIFEST_BLOB_UNKNOWN"
	ErrCodeManifestInvalid     = "MANIFEST_INVALID"
	ErrCodeManifestUnknown     = "MANIFEST_UNKNOWN"
	ErrCodeNameInvalid         = "NAME_INVALID"
	ErrCodeNameUnknown         = "NAME_UNKNOWN"
	ErrCodeSizeInvalid         = "SIZE_INVALID"
	ErrCodeUnauthorized        = "UNAUTHORIZED"
	ErrCodeDenied              = "DENIED"
	ErrCodeUnsupported         = "UNSUPPORTED"
)

type errorBody struct {
	Errors []errorItem `json:"errors"`
}

type errorItem struct {
	Code    string      `json:"code"`
	Message string      `json:"message"`
	Detail  interface{} `json:"detail,omitempty"`
}

// writeError writes an OCI-spec error response.
func writeError(w http.ResponseWriter, status int, code, message string) {
	writeErrorDetail(w, status, code, message, nil)
}

func writeErrorDetail(w http.ResponseWriter, status int, code, message string, detail interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorBody{
		Errors: []errorItem{{Code: code, Message: message, Detail: detail}},
	})
}
