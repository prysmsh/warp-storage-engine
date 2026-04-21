package oci

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
)

// The upload flow:
//  1. POST /v2/<name>/blobs/uploads/          → 202 + Location: .../uploads/<uuid>
//  2. PATCH .../uploads/<uuid>  (body=bytes)  → appends to the session tmp file
//  3. PUT   .../uploads/<uuid>?digest=<d>     → verifies digest, copies into backend,
//                                               removes session, returns 201 + Location: blobs/<d>
//
// Monolithic upload (used by helm push and oras): POST with ?digest= + body in one shot.
// We accept both forms.

// handleStartUpload creates a new upload session.
func (h *Handler) handleStartUpload(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]

	if !validRepoName(name) {
		writeError(w, http.StatusBadRequest, ErrCodeNameInvalid, "invalid repository name")
		return
	}

	// Cross-repo blob mount: the client asks "I know <digest> already exists
	// in <from-repo>; can you alias it into <name> without a re-upload?"
	// Because all blobs are stored content-addressed in a single
	// blobs/<hex-digest> object (shared across repos), a mount is a no-op
	// lookup: if the blob exists, we return 201 immediately; otherwise we
	// fall through to the normal upload start and the client will PATCH/PUT.
	if mount := r.URL.Query().Get("mount"); mount != "" {
		if h.tryBlobMount(w, r, name, mount) {
			return
		}
	}

	// Monolithic upload: client sent ?digest=<d> and a body in a single POST.
	if digest := r.URL.Query().Get("digest"); digest != "" {
		h.finishMonolithic(w, r, name, digest)
		return
	}

	uuid, err := newUploadUUID()
	if err != nil {
		writeError(w, http.StatusInternalServerError, ErrCodeBlobUploadInvalid, "failed to allocate upload ID")
		return
	}

	session := &uploadSession{uuid: uuid, repo: name, path: h.sessionPath(uuid)}
	if err := touchFile(session.path); err != nil {
		writeError(w, http.StatusInternalServerError, ErrCodeBlobUploadInvalid, "failed to create upload file")
		return
	}

	h.mu.Lock()
	h.sessions[uuid] = session
	h.mu.Unlock()

	w.Header().Set("Location", fmt.Sprintf("/v2/%s/blobs/uploads/%s", name, uuid))
	w.Header().Set("Range", "0-0")
	w.Header().Set("Docker-Upload-UUID", uuid)
	w.WriteHeader(http.StatusAccepted)
}

// tryBlobMount returns true if the mount succeeded or was definitively
// rejected (and a response was already written). Returns false when the
// client should proceed with a normal upload.
func (h *Handler) tryBlobMount(w http.ResponseWriter, r *http.Request, name, digest string) bool {
	if !validDigest(digest) {
		// Bad mount request — fall through to normal upload start, which is
		// what real registries do for unmountable content.
		return false
	}
	if _, err := h.backend.HeadObject(r.Context(), h.cfg.Bucket, blobKey(digest)); err != nil {
		// Blob isn't here — client must upload.
		return false
	}
	w.Header().Set("Location", fmt.Sprintf("/v2/%s/blobs/%s", name, digest))
	w.Header().Set("Docker-Content-Digest", digest)
	w.WriteHeader(http.StatusCreated)
	return true
}

func (h *Handler) finishMonolithic(w http.ResponseWriter, r *http.Request, name, digest string) {
	if !validDigest(digest) {
		writeError(w, http.StatusBadRequest, ErrCodeDigestInvalid, "invalid digest format")
		return
	}

	hasher := sha256.New()
	if err := h.backend.PutObject(
		r.Context(),
		h.cfg.Bucket,
		blobKey(digest),
		io.TeeReader(r.Body, hasher),
		r.ContentLength,
		nil,
	); err != nil {
		writeError(w, http.StatusInternalServerError, ErrCodeBlobUploadInvalid, err.Error())
		return
	}

	computed := "sha256:" + hex.EncodeToString(hasher.Sum(nil))
	if computed != digest {
		// Best-effort cleanup of the bad blob.
		_ = h.backend.DeleteObject(r.Context(), h.cfg.Bucket, blobKey(digest))
		writeError(w, http.StatusBadRequest, ErrCodeDigestInvalid, "digest mismatch")
		return
	}

	w.Header().Set("Location", fmt.Sprintf("/v2/%s/blobs/%s", name, digest))
	w.Header().Set("Docker-Content-Digest", digest)
	w.WriteHeader(http.StatusCreated)
}

// handleUploadChunk appends bytes to an in-progress upload. Clients may send
// one or many PATCHes; the Range header tells them what we've received so far.
func (h *Handler) handleUploadChunk(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	uuid := vars["uuid"]

	if !validRepoName(name) {
		writeError(w, http.StatusBadRequest, ErrCodeNameInvalid, "invalid repository name")
		return
	}

	session := h.takeSession(uuid)
	if session == nil {
		writeError(w, http.StatusNotFound, ErrCodeBlobUploadUnknown, "upload session not found")
		return
	}
	defer h.returnSession(session)

	f, err := os.OpenFile(session.path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		writeError(w, http.StatusInternalServerError, ErrCodeBlobUploadInvalid, "failed to open upload file")
		return
	}
	defer f.Close()

	n, err := io.Copy(f, r.Body)
	if err != nil {
		writeError(w, http.StatusInternalServerError, ErrCodeBlobUploadInvalid, "failed to write chunk")
		return
	}
	session.size += n

	w.Header().Set("Location", fmt.Sprintf("/v2/%s/blobs/uploads/%s", name, uuid))
	w.Header().Set("Range", fmt.Sprintf("0-%d", session.size-1))
	w.Header().Set("Docker-Upload-UUID", uuid)
	w.WriteHeader(http.StatusAccepted)
}

// handleFinishUpload finalizes an in-progress upload. The optional final body
// is appended, the session is hashed, and — if the digest matches the client
// claim — the content is copied into the backend under blobs/<digest>.
func (h *Handler) handleFinishUpload(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	uuid := vars["uuid"]
	digest := r.URL.Query().Get("digest")

	if !validRepoName(name) {
		writeError(w, http.StatusBadRequest, ErrCodeNameInvalid, "invalid repository name")
		return
	}
	if !validDigest(digest) {
		writeError(w, http.StatusBadRequest, ErrCodeDigestInvalid, "invalid digest format")
		return
	}

	session := h.removeSession(uuid)
	if session == nil {
		writeError(w, http.StatusNotFound, ErrCodeBlobUploadUnknown, "upload session not found")
		return
	}
	defer func() { _ = os.Remove(session.path) }()

	// Append any final body bytes before we hash.
	if r.ContentLength != 0 {
		f, err := os.OpenFile(session.path, os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			writeError(w, http.StatusInternalServerError, ErrCodeBlobUploadInvalid, "failed to open upload file")
			return
		}
		_, copyErr := io.Copy(f, r.Body)
		_ = f.Close()
		if copyErr != nil {
			writeError(w, http.StatusInternalServerError, ErrCodeBlobUploadInvalid, "failed to write final chunk")
			return
		}
	}

	// Hash + compare before we touch the backend. Avoids half-finalized uploads.
	computed, size, err := hashFile(session.path)
	if err != nil {
		writeError(w, http.StatusInternalServerError, ErrCodeBlobUploadInvalid, "failed to hash upload")
		return
	}
	if computed != digest {
		writeError(w, http.StatusBadRequest, ErrCodeDigestInvalid, "digest mismatch")
		return
	}

	f, err := os.Open(session.path)
	if err != nil {
		writeError(w, http.StatusInternalServerError, ErrCodeBlobUploadInvalid, "failed to reopen upload")
		return
	}
	defer f.Close()

	if err := h.backend.PutObject(r.Context(), h.cfg.Bucket, blobKey(digest), f, size, nil); err != nil {
		writeError(w, http.StatusInternalServerError, ErrCodeBlobUploadInvalid, err.Error())
		return
	}

	w.Header().Set("Location", fmt.Sprintf("/v2/%s/blobs/%s", name, digest))
	w.Header().Set("Docker-Content-Digest", digest)
	w.WriteHeader(http.StatusCreated)
}

// handleUploadStatus reports progress on an in-progress upload.
func (h *Handler) handleUploadStatus(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	uuid := vars["uuid"]

	session := h.takeSession(uuid)
	if session == nil {
		writeError(w, http.StatusNotFound, ErrCodeBlobUploadUnknown, "upload session not found")
		return
	}
	defer h.returnSession(session)

	w.Header().Set("Location", fmt.Sprintf("/v2/%s/blobs/uploads/%s", name, uuid))
	w.Header().Set("Range", fmt.Sprintf("0-%d", max64(session.size-1, 0)))
	w.Header().Set("Docker-Upload-UUID", uuid)
	w.WriteHeader(http.StatusNoContent)
}

// handleCancelUpload discards an in-progress upload's local state.
func (h *Handler) handleCancelUpload(w http.ResponseWriter, r *http.Request) {
	uuid := mux.Vars(r)["uuid"]

	session := h.removeSession(uuid)
	if session == nil {
		writeError(w, http.StatusNotFound, ErrCodeBlobUploadUnknown, "upload session not found")
		return
	}
	_ = os.Remove(session.path)
	w.WriteHeader(http.StatusNoContent)
}

// Session bookkeeping helpers.
// takeSession returns a pointer while holding no lock. Callers must
// returnSession when they're done to protect against concurrent finalize/cancel.

func (h *Handler) takeSession(uuid string) *uploadSession {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sessions[uuid]
}

func (h *Handler) returnSession(_ *uploadSession) {
	// no-op — kept for readability and future lock-upgrade work
}

func (h *Handler) removeSession(uuid string) *uploadSession {
	h.mu.Lock()
	defer h.mu.Unlock()
	s, ok := h.sessions[uuid]
	if !ok {
		return nil
	}
	delete(h.sessions, uuid)
	return s
}

func newUploadUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func touchFile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	return f.Close()
}

func hashFile(path string) (digest string, size int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	hasher := sha256.New()
	n, err := io.Copy(hasher, f)
	if err != nil {
		return "", 0, err
	}
	return "sha256:" + hex.EncodeToString(hasher.Sum(nil)), n, nil
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// contentLengthOrZero parses a possibly-absent Content-Length header.
// Unused directly today, retained for the streaming path we'll want later.
func contentLengthOrZero(h http.Header) int64 {
	v := h.Get("Content-Length")
	if v == "" {
		return 0
	}
	n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return n
}
