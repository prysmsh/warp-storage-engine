package oci

import (
	"io"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
)

// handleGetBlob serves a blob's content. Clients pull layers and config blobs
// through this endpoint.
func (h *Handler) handleGetBlob(w http.ResponseWriter, r *http.Request) {
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

	obj, err := h.backend.GetObject(r.Context(), h.cfg.Bucket, blobKey(digest))
	if err != nil {
		writeError(w, http.StatusNotFound, ErrCodeBlobUnknown, "blob not found")
		return
	}
	defer obj.Body.Close()

	w.Header().Set("Docker-Content-Digest", digest)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(obj.Size, 10))
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, obj.Body)
}

// handleHeadBlob answers content-existence probes. Returns 200 with headers
// and no body when the blob is present, 404 otherwise.
func (h *Handler) handleHeadBlob(w http.ResponseWriter, r *http.Request) {
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

	info, err := h.backend.HeadObject(r.Context(), h.cfg.Bucket, blobKey(digest))
	if err != nil {
		writeError(w, http.StatusNotFound, ErrCodeBlobUnknown, "blob not found")
		return
	}

	w.Header().Set("Docker-Content-Digest", digest)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(info.Size, 10))
	w.WriteHeader(http.StatusOK)
}

// handleDeleteBlob removes a blob from the backend. Note that OCI clients
// rarely call this directly — manifest deletion is the typical path.
func (h *Handler) handleDeleteBlob(w http.ResponseWriter, r *http.Request) {
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

	if err := h.backend.DeleteObject(r.Context(), h.cfg.Bucket, blobKey(digest)); err != nil {
		writeError(w, http.StatusNotFound, ErrCodeBlobUnknown, "blob not found")
		return
	}
	w.WriteHeader(http.StatusAccepted)
}
