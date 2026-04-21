package oci

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
)

// The Referrers API lets cosign/notation find signatures or attestations for
// an image by asking "what manifests reference <digest> as their subject?".
// Spec:
//   https://github.com/opencontainers/distribution-spec/blob/main/spec.md#listing-referrers
//
// We maintain an index at manifest-PUT time: whenever a manifest has a
// top-level `subject.digest` pointing at <subjectDigest>, we write
//
//	referrers/<repo>/<subjectDigest>/<manifestDigest>
//
// containing the full descriptor for the referring manifest (so we can serve
// the index without re-reading every manifest). The descriptors are standard
// OCI image descriptors: mediaType, digest, size, artifactType, annotations.

type descriptor struct {
	MediaType    string            `json:"mediaType"`
	Digest       string            `json:"digest"`
	Size         int64             `json:"size"`
	ArtifactType string            `json:"artifactType,omitempty"`
	Annotations  map[string]string `json:"annotations,omitempty"`
}

type referrersIndex struct {
	SchemaVersion int          `json:"schemaVersion"`
	MediaType     string       `json:"mediaType"`
	Manifests     []descriptor `json:"manifests"`
}

// manifestSubject is the minimal shape we parse out of a pushed manifest to
// detect referrers. We do not validate other fields — that's up to the client.
type manifestSubject struct {
	MediaType    string            `json:"mediaType"`
	ArtifactType string            `json:"artifactType,omitempty"`
	Annotations  map[string]string `json:"annotations,omitempty"`
	Subject      *descriptor       `json:"subject,omitempty"`
}

// indexReferrerIfNeeded inspects a manifest body being PUT. If it carries a
// `subject` field, we write an entry under referrers/<repo>/<subjectDigest>.
// Errors are logged but not propagated — the manifest push itself has already
// succeeded, and a missing referrers entry is recoverable (the client can
// re-push, or cosign will simply not find the signature).
func (h *Handler) indexReferrerIfNeeded(r *http.Request, repo, manifestDigest, mediaType string, body []byte) {
	var ms manifestSubject
	if err := json.Unmarshal(body, &ms); err != nil {
		return
	}
	if ms.Subject == nil || ms.Subject.Digest == "" {
		return
	}
	if !validDigest(ms.Subject.Digest) {
		return
	}

	desc := descriptor{
		MediaType:    mediaType,
		Digest:       manifestDigest,
		Size:         int64(len(body)),
		ArtifactType: ms.ArtifactType,
		Annotations:  ms.Annotations,
	}
	encoded, err := json.Marshal(desc)
	if err != nil {
		return
	}

	key := fmt.Sprintf("referrers/%s/%s/%s", repo, digestHex(ms.Subject.Digest), digestHex(manifestDigest))
	_ = h.backend.PutObject(
		r.Context(),
		h.cfg.Bucket,
		key,
		bytes.NewReader(encoded),
		int64(len(encoded)),
		map[string]string{"Content-Type": "application/json"},
	)
}

// handleReferrers answers GET /v2/<name>/referrers/<digest>. Returns an OCI
// image index whose manifests[] lists everything we've recorded pointing at
// <digest> via the `subject` field.
func (h *Handler) handleReferrers(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	digest := vars["digest"]

	if !validRepoName(name) {
		writeError(w, http.StatusBadRequest, ErrCodeNameInvalid, "invalid repository name")
		return
	}
	if !validDigest(digest) {
		writeError(w, http.StatusBadRequest, ErrCodeDigestInvalid, "invalid digest format")
		return
	}

	prefix := fmt.Sprintf("referrers/%s/%s/", name, digestHex(digest))
	result, err := h.backend.ListObjects(r.Context(), h.cfg.Bucket, prefix, "", 0)
	if err != nil {
		writeError(w, http.StatusInternalServerError, ErrCodeUnsupported, "failed to list referrers")
		return
	}

	manifests := make([]descriptor, 0, len(result.Contents))

	// Optional artifactType filter per the spec.
	artifactType := r.URL.Query().Get("artifactType")

	for _, obj := range result.Contents {
		if !strings.HasPrefix(obj.Key, prefix) {
			continue
		}
		entry, err := h.backend.GetObject(r.Context(), h.cfg.Bucket, obj.Key)
		if err != nil {
			continue
		}
		body, err := io.ReadAll(entry.Body)
		_ = entry.Body.Close()
		if err != nil {
			continue
		}
		var d descriptor
		if err := json.Unmarshal(body, &d); err != nil {
			continue
		}
		if artifactType != "" && d.ArtifactType != artifactType {
			continue
		}
		manifests = append(manifests, d)
	}

	if artifactType != "" {
		w.Header().Set("OCI-Filters-Applied", "artifactType")
	}

	index := referrersIndex{
		SchemaVersion: 2,
		MediaType:     "application/vnd.oci.image.index.v1+json",
		Manifests:     manifests,
	}
	w.Header().Set("Content-Type", "application/vnd.oci.image.index.v1+json")
	_ = json.NewEncoder(w).Encode(index)
}
