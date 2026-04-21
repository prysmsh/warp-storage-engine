package oci

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
)

// Manifest resolution order — the {reference} segment is either a digest or a tag.
// On read:
//   - if reference looks like a digest, load manifests/<repo>/<hex-of-digest>.
//   - else treat as tag: read tags/<repo>/<tag> to get the digest, then load the
//     manifest by digest. This two-step keeps tag→digest mapping small and
//     lets multiple tags share one manifest.
//
// On write (PUT):
//   - compute digest of the body.
//   - store manifest at manifests/<repo>/<digest-hex>.
//   - if the reference was a tag (not a digest), write tags/<repo>/<tag>
//     containing the digest string.

func (h *Handler) handleGetManifest(w http.ResponseWriter, r *http.Request) {
	h.getOrHeadManifest(w, r, true)
}

func (h *Handler) handleHeadManifest(w http.ResponseWriter, r *http.Request) {
	h.getOrHeadManifest(w, r, false)
}

func (h *Handler) getOrHeadManifest(w http.ResponseWriter, r *http.Request, writeBody bool) {
	vars := mux.Vars(r)
	name := vars["name"]
	ref := vars["reference"]

	if !validRepoName(name) {
		writeError(w, http.StatusBadRequest, ErrCodeNameInvalid, "invalid repository name")
		return
	}
	if !validTagOrDigest(ref) {
		writeError(w, http.StatusBadRequest, ErrCodeManifestInvalid, "invalid reference")
		return
	}

	digest, err := h.resolveManifestDigest(r, name, ref)
	if err != nil {
		writeError(w, http.StatusNotFound, ErrCodeManifestUnknown, "manifest not found")
		return
	}

	obj, err := h.backend.GetObject(r.Context(), h.cfg.Bucket, manifestKeyByDigest(name, digest))
	if err != nil {
		writeError(w, http.StatusNotFound, ErrCodeManifestUnknown, "manifest not found")
		return
	}
	defer obj.Body.Close()

	body, err := io.ReadAll(obj.Body)
	if err != nil {
		writeError(w, http.StatusInternalServerError, ErrCodeManifestUnknown, "failed to read manifest")
		return
	}

	contentType := obj.ContentType
	if contentType == "" {
		contentType = detectManifestMediaType(body)
	}

	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Docker-Content-Digest", digest)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	if writeBody {
		_, _ = w.Write(body)
	}
}

// resolveManifestDigest turns a {reference} into a digest, following a tag
// pointer if needed.
func (h *Handler) resolveManifestDigest(r *http.Request, name, ref string) (string, error) {
	if validDigest(ref) {
		return ref, nil
	}
	// Tag: fetch the pointer object.
	obj, err := h.backend.GetObject(r.Context(), h.cfg.Bucket, tagKey(name, ref))
	if err != nil {
		return "", err
	}
	defer obj.Body.Close()
	body, err := io.ReadAll(obj.Body)
	if err != nil {
		return "", err
	}
	d := string(bytes.TrimSpace(body))
	if !validDigest(d) {
		return "", errInvalidTagPointer
	}
	return d, nil
}

// handlePutManifest stores a manifest, addressed by the digest of its body.
// If the reference is a tag, also writes the tag→digest pointer.
func (h *Handler) handlePutManifest(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	ref := vars["reference"]

	if !validRepoName(name) {
		writeError(w, http.StatusBadRequest, ErrCodeNameInvalid, "invalid repository name")
		return
	}
	if !validTagOrDigest(ref) {
		writeError(w, http.StatusBadRequest, ErrCodeManifestInvalid, "invalid reference")
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, ErrCodeManifestInvalid, "failed to read manifest body")
		return
	}

	hash := sha256.Sum256(body)
	digest := "sha256:" + hex.EncodeToString(hash[:])

	// If the caller gave a digest reference, it must match the body.
	if validDigest(ref) && ref != digest {
		writeError(w, http.StatusBadRequest, ErrCodeDigestInvalid, "reference digest does not match body")
		return
	}

	mediaType := r.Header.Get("Content-Type")
	if mediaType == "" {
		mediaType = detectManifestMediaType(body)
	}

	meta := map[string]string{"Content-Type": mediaType}

	if err := h.backend.PutObject(
		r.Context(),
		h.cfg.Bucket,
		manifestKeyByDigest(name, digest),
		bytes.NewReader(body),
		int64(len(body)),
		meta,
	); err != nil {
		writeError(w, http.StatusInternalServerError, ErrCodeManifestInvalid, err.Error())
		return
	}

	// If this manifest declares a `subject`, add it to the referrers index so
	// cosign/notation can find it via the Referrers API.
	h.indexReferrerIfNeeded(r, name, digest, mediaType, body)

	// If the manifest has a subject, signal that via the response header so
	// clients know the referrers API is available.
	if hasSubject(body) {
		w.Header().Set("OCI-Subject", extractSubjectDigest(body))
	}

	// Update the tag pointer if the caller pushed by tag.
	if !validDigest(ref) {
		if err := h.backend.PutObject(
			r.Context(),
			h.cfg.Bucket,
			tagKey(name, ref),
			bytes.NewReader([]byte(digest)),
			int64(len(digest)),
			nil,
		); err != nil {
			writeError(w, http.StatusInternalServerError, ErrCodeManifestInvalid, err.Error())
			return
		}
	}

	w.Header().Set("Location", "/v2/"+name+"/manifests/"+digest)
	w.Header().Set("Docker-Content-Digest", digest)
	w.WriteHeader(http.StatusCreated)
}

// handleDeleteManifest removes either a tag pointer (ref is a tag) or the
// manifest by digest. Blob deletion is deliberately NOT cascaded — clients
// can explicitly delete blobs or rely on out-of-band GC.
func (h *Handler) handleDeleteManifest(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	ref := vars["reference"]

	if !validRepoName(name) {
		writeError(w, http.StatusBadRequest, ErrCodeNameInvalid, "invalid repository name")
		return
	}

	if validDigest(ref) {
		if err := h.backend.DeleteObject(r.Context(), h.cfg.Bucket, manifestKeyByDigest(name, ref)); err != nil {
			writeError(w, http.StatusNotFound, ErrCodeManifestUnknown, "manifest not found")
			return
		}
	} else if tagPattern.MatchString(ref) {
		if err := h.backend.DeleteObject(r.Context(), h.cfg.Bucket, tagKey(name, ref)); err != nil {
			writeError(w, http.StatusNotFound, ErrCodeManifestUnknown, "tag not found")
			return
		}
	} else {
		writeError(w, http.StatusBadRequest, ErrCodeManifestInvalid, "invalid reference")
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

// detectManifestMediaType peeks at the JSON to guess the OCI vs Docker media type.
// Good-enough heuristic — clients typically pass Content-Type explicitly anyway.
func detectManifestMediaType(body []byte) string {
	s := string(body)
	switch {
	case bytes.Contains(body, []byte(`"application/vnd.oci.image.index.v1+json"`)):
		return "application/vnd.oci.image.index.v1+json"
	case bytes.Contains(body, []byte(`"application/vnd.oci.image.manifest.v1+json"`)):
		return "application/vnd.oci.image.manifest.v1+json"
	case bytes.Contains(body, []byte(`"application/vnd.docker.distribution.manifest.list.v2+json"`)):
		return "application/vnd.docker.distribution.manifest.list.v2+json"
	case bytes.Contains(body, []byte(`"application/vnd.docker.distribution.manifest.v2+json"`)):
		return "application/vnd.docker.distribution.manifest.v2+json"
	case len(s) > 0 && s[0] == '{':
		return "application/vnd.oci.image.manifest.v1+json"
	}
	return "application/octet-stream"
}

var errInvalidTagPointer = &ociInternalError{msg: "tag points to invalid digest"}

// hasSubject/extractSubjectDigest are cheap inspections used to set the
// OCI-Subject response header without re-parsing the whole manifest.
func hasSubject(body []byte) bool {
	var ms manifestSubject
	_ = json.Unmarshal(body, &ms)
	return ms.Subject != nil && ms.Subject.Digest != ""
}

func extractSubjectDigest(body []byte) string {
	var ms manifestSubject
	if err := json.Unmarshal(body, &ms); err != nil || ms.Subject == nil {
		return ""
	}
	return ms.Subject.Digest
}
