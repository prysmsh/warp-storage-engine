package proxy

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/bcrypt"

	"github.com/prysmsh/warp-storage-engine/internal/security"
	"github.com/prysmsh/warp-storage-engine/internal/storage"
)

type ShareLink struct {
	ID           string    `json:"id"`
	ObjectKey    string    `json:"object_key"`
	BucketName   string    `json:"bucket_name"`
	CreatedAt    time.Time `json:"created_at"`
	ExpiresAt    time.Time `json:"expires_at"`
	CreatedBy    string    `json:"created_by"`
	AccessCount  int       `json:"access_count"`
	MaxAccess    int       `json:"max_access"`    // Maximum number of times the link can be accessed
	PasswordHash string    `json:"password_hash"` // BCrypt hash of the password (if set)
	SingleUse    bool      `json:"single_use"`    // If true, link expires after first use
}

type ShareLinkManager struct {
	links map[string]*ShareLink
	mu    sync.RWMutex
}

func NewShareLinkManager() *ShareLinkManager {
	manager := &ShareLinkManager{
		links: make(map[string]*ShareLink),
	}

	// Start cleanup goroutine
	go manager.cleanupExpiredLinks()

	return manager
}

func generateID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return base64.URLEncoding.EncodeToString(b)
}

func (m *ShareLinkManager) CreateShareLink(bucketName, objectKey, createdBy string, ttl time.Duration, password string, singleUse bool) (*ShareLink, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	link := &ShareLink{
		ID:          generateID(),
		ObjectKey:   objectKey,
		BucketName:  bucketName,
		CreatedAt:   time.Now(),
		ExpiresAt:   time.Now().Add(ttl),
		CreatedBy:   createdBy,
		AccessCount: 0,
		SingleUse:   singleUse,
	}

	// Set max access based on single use
	if singleUse {
		link.MaxAccess = 1
	} else {
		link.MaxAccess = -1 // Unlimited
	}

	// Hash password if provided
	if password != "" {
		hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
		if err != nil {
			return nil, fmt.Errorf("failed to hash password: %w", err)
		}
		link.PasswordHash = string(hash)
	}

	m.links[link.ID] = link

	logrus.WithFields(logrus.Fields{
		"id":          link.ID,
		"bucket":      bucketName,
		"key":         objectKey,
		"expiresAt":   link.ExpiresAt,
		"singleUse":   singleUse,
		"hasPassword": password != "",
	}).Info("Created share link")

	return link, nil
}

func (m *ShareLinkManager) GetShareLink(id string) (*ShareLink, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	link, exists := m.links[id]
	if !exists {
		return nil, fmt.Errorf("share link not found")
	}

	if time.Now().After(link.ExpiresAt) {
		return nil, fmt.Errorf("share link expired")
	}

	// Check if link has reached max access count
	if link.MaxAccess > 0 && link.AccessCount >= link.MaxAccess {
		return nil, fmt.Errorf("share link has reached maximum access limit")
	}

	return link, nil
}

func (m *ShareLinkManager) IncrementAccessCount(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if link, exists := m.links[id]; exists {
		link.AccessCount++
	}
}

func (m *ShareLinkManager) cleanupExpiredLinks() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		m.mu.Lock()
		now := time.Now()
		for id, link := range m.links {
			if now.After(link.ExpiresAt) {
				delete(m.links, id)
				logrus.WithField("id", id).Debug("Cleaned up expired share link")
			}
		}
		m.mu.Unlock()
	}
}

type ShareLinkHandler struct {
	manager   *ShareLinkManager
	s3Handler http.Handler
	storage   storage.Backend
}

func NewShareLinkHandler(storageBackend storage.Backend, s3Handler http.Handler) *ShareLinkHandler {
	return &ShareLinkHandler{
		manager:   NewShareLinkManager(),
		s3Handler: s3Handler,
		storage:   storageBackend,
	}
}

func (h *ShareLinkHandler) CreateShareLinkHandler(w http.ResponseWriter, r *http.Request) {
	authenticated, _ := r.Context().Value("authenticated").(bool)
	if !authenticated {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	isAdmin := false
	if adminValue := r.Context().Value("is_admin"); adminValue != nil {
		if admin, ok := adminValue.(bool); ok {
			isAdmin = admin
		}
	}
	if !isAdmin {
		http.Error(w, "Forbidden: admin access required", http.StatusForbidden)
		return
	}

	var req struct {
		BucketName string `json:"bucket_name"`
		ObjectKey  string `json:"object_key"`
		TTLHours   int    `json:"ttl_hours"`
		Password   string `json:"password"`
		SingleUse  bool   `json:"single_use"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Default TTL is 24 hours, max is 7 days
	if req.TTLHours <= 0 {
		req.TTLHours = 24
	} else if req.TTLHours > 168 {
		req.TTLHours = 168
	}

	ttl := time.Duration(req.TTLHours) * time.Hour

	if h.storage == nil {
		logrus.Error("Share link storage backend is not configured")
		http.Error(w, "Storage backend unavailable", http.StatusInternalServerError)
		return
	}

	if err := security.ValidateBucketName(req.BucketName); err != nil {
		http.Error(w, "Invalid bucket name", http.StatusBadRequest)
		return
	}

	if err := security.ValidateObjectKey(req.ObjectKey); err != nil {
		http.Error(w, "Invalid object key", http.StatusBadRequest)
		return
	}

	if _, err := h.storage.HeadObject(r.Context(), req.BucketName, req.ObjectKey); err != nil {
		logrus.WithError(err).WithFields(logrus.Fields{
			"bucket": req.BucketName,
			"key":    req.ObjectKey,
		}).Warn("Share link creation denied: object not accessible")
		http.Error(w, "Access to requested object denied", http.StatusForbidden)
		return
	}

	userSub, _ := r.Context().Value("user_sub").(string)

	createdBy := userSub
	if createdBy == "" {
		createdBy = "authenticated-user"
	}

	if userInfo, ok := r.Context().Value("user").(map[string]interface{}); ok {
		if email, ok := userInfo["email"].(string); ok && email != "" {
			createdBy = email
		}
	}

	link, err := h.manager.CreateShareLink(req.BucketName, req.ObjectKey, createdBy, ttl, req.Password, req.SingleUse)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Build the share URL
	scheme := "http"
	if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https" {
		scheme = "https"
	}

	host := r.Host
	if forwardedHost := r.Header.Get("X-Forwarded-Host"); forwardedHost != "" {
		host = forwardedHost
	}

	shareURL := fmt.Sprintf("%s://%s/api/share/%s", scheme, host, link.ID)

	response := map[string]interface{}{
		"share_url":    shareURL,
		"expires_at":   link.ExpiresAt,
		"ttl_hours":    req.TTLHours,
		"share_id":     link.ID,
		"single_use":   req.SingleUse,
		"has_password": req.Password != "",
		"created_by":   createdBy,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (h *ShareLinkHandler) ServeSharedFile(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	shareID := vars["shareID"]

	link, err := h.manager.GetShareLink(shareID)
	if err != nil {
		http.Error(w, "Invalid or expired share link", http.StatusNotFound)
		return
	}

	// Check password if required
	if link.PasswordHash != "" {
		password := r.URL.Query().Get("password")
		if password == "" {
			// Check Authorization header for password
			auth := r.Header.Get("Authorization")
			if strings.HasPrefix(auth, "Bearer ") {
				password = strings.TrimPrefix(auth, "Bearer ")
			}
		}

		if password == "" {
			w.Header().Set("WWW-Authenticate", "Bearer realm=\"Share Link\"")
			http.Error(w, "Password required", http.StatusUnauthorized)
			return
		}

		// Verify password
		if err := bcrypt.CompareHashAndPassword([]byte(link.PasswordHash), []byte(password)); err != nil {
			http.Error(w, "Invalid password", http.StatusUnauthorized)
			return
		}
	}

	// Increment access count
	h.manager.IncrementAccessCount(shareID)

	// Securely rewrite the request to point to the actual S3 object
	// Validate and sanitize bucket name and object key to prevent path traversal
	safeBucket := h.sanitizePath(link.BucketName)
	safeObjectKey := h.sanitizePath(link.ObjectKey)

	if safeBucket == "" || safeObjectKey == "" {
		logrus.WithFields(logrus.Fields{
			"shareID":    shareID,
			"bucket":     link.BucketName,
			"key":        link.ObjectKey,
			"safeBucket": safeBucket,
			"safeKey":    safeObjectKey,
		}).Warn("Path traversal attempt detected in share link")
		http.Error(w, "Invalid share link path", http.StatusForbidden)
		return
	}

	r.URL.Path = "/" + safeBucket + "/" + safeObjectKey

	// Add headers to indicate this is a shared file
	w.Header().Set("X-Share-Link", "true")
	w.Header().Set("X-Share-Expires", link.ExpiresAt.Format(time.RFC3339))

	// Set Content-Disposition to suggest downloading
	filename := link.ObjectKey
	if idx := strings.LastIndex(filename, "/"); idx >= 0 {
		filename = filename[idx+1:]
	}
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filename))

	logrus.WithFields(logrus.Fields{
		"shareID":     shareID,
		"bucket":      link.BucketName,
		"key":         link.ObjectKey,
		"accessCount": link.AccessCount + 1,
		"singleUse":   link.SingleUse,
	}).Info("Serving shared file")

	// Pass to S3 handler
	h.s3Handler.ServeHTTP(w, r)
}

// sanitizePath prevents directory traversal attacks
func (h *ShareLinkHandler) sanitizePath(inputPath string) string {
	cleanedPath, err := security.SanitizePathAllowlist(inputPath)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"original": inputPath,
			"error":    err.Error(),
		}).Warn("Path sanitization failed")
		return ""
	}

	return cleanedPath
}
