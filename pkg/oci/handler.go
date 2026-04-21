package oci

import (
	"crypto/subtle"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"

	"github.com/prysmsh/warp-storage-engine/internal/config"
	"github.com/prysmsh/warp-storage-engine/internal/storage"
)

// Handler implements the OCI Distribution v2 HTTP API on top of a
// storage.Backend. It terminates OCI protocol concerns (digest validation,
// upload session state, WWW-Authenticate challenges, content negotiation)
// and issues S3-style object reads/writes to the underlying backend.
type Handler struct {
	backend storage.Backend
	cfg     config.OCIConfig

	// Upload sessions are streamed to a local temp directory; on PUT we read
	// them back and issue a single backend.PutObject.
	uploadsDir string

	// Guards the upload-session ownership map (ownership is not strictly
	// required for correctness today, but we keep it ready for multi-tenant
	// isolation in Phase 2b).
	mu       sync.Mutex
	sessions map[string]*uploadSession
}

type uploadSession struct {
	uuid string
	repo string
	path string // local tmp file
	size int64
}

// Router returns a gorilla/mux router configured with all OCI Distribution v2
// routes. The router is intentionally self-contained so callers (main.go) can
// mount it on a dedicated listener.
func (h *Handler) Router() http.Handler {
	r := mux.NewRouter()

	// API version check — must be unauthenticated to expose the challenge.
	r.HandleFunc("/v2/", h.handleV2Root).Methods(http.MethodGet)
	r.HandleFunc("/v2", h.handleV2Root).Methods(http.MethodGet)

	// Token endpoint for bearer-auth clients. Always mounted — returns 404
	// with UNSUPPORTED code if bearer is disabled, so clients get a clear signal.
	r.HandleFunc("/auth/token", h.handleToken).Methods(http.MethodGet, http.MethodPost)

	// Catalog listing. Separate from the authenticated subrouter so it can
	// honour a dedicated registry:catalog:* scope later; for now it uses
	// the same middleware.
	sub := r.PathPrefix("/v2").Subrouter()
	sub.Use(h.authMiddleware)

	sub.HandleFunc("/_catalog", h.handleCatalog).Methods(http.MethodGet)

	// Referrers API — cosign and notation discover signatures by asking
	// "which manifests reference this subject digest?".
	sub.HandleFunc("/{name:.+}/referrers/{digest}", h.handleReferrers).Methods(http.MethodGet)

	// Uploads. POST initiates, PATCH appends, PUT finalizes, GET status, DELETE aborts.
	// Registered before the manifest/blob routes to avoid the "uploads" segment
	// being interpreted as a reference.
	sub.HandleFunc("/{name:.+}/blobs/uploads/", h.handleStartUpload).Methods(http.MethodPost)
	sub.HandleFunc("/{name:.+}/blobs/uploads/{uuid}", h.handleUploadChunk).Methods(http.MethodPatch)
	sub.HandleFunc("/{name:.+}/blobs/uploads/{uuid}", h.handleFinishUpload).Methods(http.MethodPut)
	sub.HandleFunc("/{name:.+}/blobs/uploads/{uuid}", h.handleUploadStatus).Methods(http.MethodGet)
	sub.HandleFunc("/{name:.+}/blobs/uploads/{uuid}", h.handleCancelUpload).Methods(http.MethodDelete)

	// Blob read/check/delete.
	sub.HandleFunc("/{name:.+}/blobs/{digest}", h.handleGetBlob).Methods(http.MethodGet)
	sub.HandleFunc("/{name:.+}/blobs/{digest}", h.handleHeadBlob).Methods(http.MethodHead)
	sub.HandleFunc("/{name:.+}/blobs/{digest}", h.handleDeleteBlob).Methods(http.MethodDelete)

	// Manifest read/check/put/delete.
	sub.HandleFunc("/{name:.+}/manifests/{reference}", h.handleGetManifest).Methods(http.MethodGet)
	sub.HandleFunc("/{name:.+}/manifests/{reference}", h.handleHeadManifest).Methods(http.MethodHead)
	sub.HandleFunc("/{name:.+}/manifests/{reference}", h.handlePutManifest).Methods(http.MethodPut)
	sub.HandleFunc("/{name:.+}/manifests/{reference}", h.handleDeleteManifest).Methods(http.MethodDelete)

	// Tag list.
	sub.HandleFunc("/{name:.+}/tags/list", h.handleListTags).Methods(http.MethodGet)

	return r
}

// NewHandler constructs an OCI Distribution handler.
// backend is the configured storage backend (the same one the S3 frontend
// uses). cfg controls auth, listen address, and bucket.
func NewHandler(backend storage.Backend, cfg config.OCIConfig) (*Handler, error) {
	if backend == nil {
		return nil, errNilBackend
	}
	if cfg.Bucket == "" {
		cfg.Bucket = "oci"
	}

	uploadsDir, err := os.MkdirTemp("", "warp-oci-uploads-*")
	if err != nil {
		return nil, err
	}

	return &Handler{
		backend:    backend,
		cfg:        cfg,
		uploadsDir: uploadsDir,
		sessions:   make(map[string]*uploadSession),
	}, nil
}

// Close releases resources. Safe to call multiple times.
func (h *Handler) Close() error {
	if h.uploadsDir == "" {
		return nil
	}
	dir := h.uploadsDir
	h.uploadsDir = ""
	return os.RemoveAll(dir)
}

// handleV2Root answers GET /v2/ with the version header. Returns 401 if
// credentials are configured and none were supplied, per the OCI challenge
// flow so clients know to send an Authorization header next.
func (h *Handler) handleV2Root(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
	if h.authRequired() && !h.requestAuthenticated(r) {
		h.writeChallenge(w, "")
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !h.authRequired() {
			next.ServeHTTP(w, r)
			return
		}
		if !h.requestAuthenticated(r) {
			h.writeChallenge(w, "")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// authRequired reports whether any auth mode is configured. Basic is
// triggered by a non-empty Username; Bearer by its own toggle.
func (h *Handler) authRequired() bool {
	return h.cfg.Username != "" || h.cfg.Bearer.Enabled
}

// requestAuthenticated accepts a request if it passes either configured
// mechanism. Bearer is preferred when enabled — if validation fails we still
// fall through to Basic so the same listener can serve mixed clients.
func (h *Handler) requestAuthenticated(r *http.Request) bool {
	authz := r.Header.Get("Authorization")
	if h.cfg.Bearer.Enabled && strings.HasPrefix(authz, "Bearer ") {
		raw := strings.TrimPrefix(authz, "Bearer ")
		claims, _, _ := h.validateBearer(raw)
		return claims != nil
	}
	if h.cfg.Username != "" && h.basicAuthOK(r) {
		return true
	}
	return false
}

func (h *Handler) basicAuthOK(r *http.Request) bool {
	user, pass, ok := r.BasicAuth()
	if !ok {
		return false
	}
	userOK := subtle.ConstantTimeCompare([]byte(user), []byte(h.cfg.Username)) == 1
	passOK := subtle.ConstantTimeCompare([]byte(pass), []byte(h.cfg.Password)) == 1
	return userOK && passOK
}

// writeChallenge emits the appropriate WWW-Authenticate header. Bearer is
// preferred when configured — some clients (docker, helm) will only read a
// single challenge scheme, so we pick one rather than stacking both.
func (h *Handler) writeChallenge(w http.ResponseWriter, scope string) {
	if h.cfg.Bearer.Enabled {
		h.bearerChallenge(w, scope)
		return
	}
	w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
	w.Header().Set("WWW-Authenticate", `Basic realm="warp-storage-engine"`)
	writeError(w, http.StatusUnauthorized, ErrCodeUnauthorized, "authentication required")
}

// sessionPath returns the local tmp path for an upload UUID. It does basic
// path-escape sanitization — the UUID itself is allocated by us (never
// attacker-controlled) but defense-in-depth is cheap.
func (h *Handler) sessionPath(uuid string) string {
	return filepath.Join(h.uploadsDir, strings.ReplaceAll(uuid, "..", "_"))
}

var errNilBackend = &ociInternalError{msg: "storage backend is nil"}

type ociInternalError struct{ msg string }

func (e *ociInternalError) Error() string { return e.msg }

// logger gives every handler a namespaced log entry without leaking a
// package-level singleton into unit tests.
func (h *Handler) logger() *logrus.Entry {
	return logrus.WithField("subsystem", "oci")
}
