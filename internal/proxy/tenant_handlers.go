package proxy

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"

	"github.com/prysmsh/warp-storage-engine/internal/auth"
	"github.com/prysmsh/warp-storage-engine/internal/database"
	"github.com/prysmsh/warp-storage-engine/internal/middleware"
)

// TenantHandlers provides HTTP handlers for org/user/bucket management
type TenantHandlers struct {
	db            database.UserStore
	vaultProvider *auth.VaultMultiUserProvider
	defaultBucket string
}

// NewTenantHandlers creates a new TenantHandlers instance
func NewTenantHandlers(db database.UserStore, vaultProvider *auth.VaultMultiUserProvider, defaultBucket string) *TenantHandlers {
	return &TenantHandlers{
		db:            db,
		vaultProvider: vaultProvider,
		defaultBucket: defaultBucket,
	}
}

var slugRegex = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*[a-z0-9]$`)

func isValidSlug(s string) bool {
	if len(s) < 2 || len(s) > 100 {
		return false
	}
	return slugRegex.MatchString(s)
}

// CreateOrgHandler creates a new organization
// POST /api/orgs
func (h *TenantHandlers) CreateOrgHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
		Slug string `json:"slug"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid request body"})
		return
	}

	if req.Name == "" || req.Slug == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "name and slug are required"})
		return
	}

	if !isValidSlug(req.Slug) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "slug must be lowercase alphanumeric with hyphens, 2-100 chars"})
		return
	}

	// Check if slug is taken
	existing, err := h.db.GetOrgBySlug(req.Slug)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to check slug"})
		return
	}
	if existing != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "Organization slug already exists"})
		return
	}

	org := &database.Organization{
		Name:     req.Name,
		Slug:     req.Slug,
		Active:   true,
		Settings: "{}",
	}

	if err := h.db.CreateOrganization(org); err != nil {
		logrus.WithError(err).Error("Failed to create organization")
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to create organization"})
		return
	}

	logrus.WithFields(logrus.Fields{
		"org_id":   org.ID,
		"org_slug": org.Slug,
	}).Info("Organization created")

	writeJSON(w, http.StatusCreated, org)
}

// GetOrgHandler returns organization info
// GET /api/orgs/{slug}
func (h *TenantHandlers) GetOrgHandler(w http.ResponseWriter, r *http.Request) {
	slug := mux.Vars(r)["slug"]

	org, err := h.db.GetOrgBySlug(slug)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to fetch organization"})
		return
	}
	if org == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Organization not found"})
		return
	}

	// Check org membership (unless global admin)
	if !isRequestAdmin(r) {
		ctxOrgID := middleware.GetOrgID(r.Context())
		if ctxOrgID != org.ID {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "Access denied"})
			return
		}
	}

	writeJSON(w, http.StatusOK, org)
}

// AddUserToOrgHandler creates a user and adds them to an org
// POST /api/orgs/{slug}/users
func (h *TenantHandlers) AddUserToOrgHandler(w http.ResponseWriter, r *http.Request) {
	slug := mux.Vars(r)["slug"]

	org, err := h.db.GetOrgBySlug(slug)
	if err != nil || org == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Organization not found"})
		return
	}

	// Must be org admin or global admin
	if !h.isOrgAdmin(r, org.ID) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "Org admin access required"})
		return
	}

	var req struct {
		Email string `json:"email"`
		Role  string `json:"role"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid request body"})
		return
	}

	if req.Email == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "email is required"})
		return
	}

	role := req.Role
	if role == "" {
		role = "member"
	}
	if role != "admin" && role != "member" && role != "readonly" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "role must be admin, member, or readonly"})
		return
	}

	// Generate access key and secret key
	accessKey, err := generateAccessKey()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to generate access key"})
		return
	}

	secretKey, err := generateSecretKey()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to generate secret key"})
		return
	}

	// Store secret in Vault
	if h.vaultProvider != nil {
		if err := h.vaultProvider.StoreCredential(accessKey, secretKey, org.ID); err != nil {
			logrus.WithError(err).Error("Failed to store credential in Vault")
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to store credential in Vault"})
			return
		}
	}

	// Store user metadata in DB (secret key stored as placeholder since real secret is in Vault)
	user := &database.User{
		Email:     req.Email,
		AccessKey: accessKey,
		SecretKey: "vault-managed", // Plaintext secret is only in Vault
		Active:    true,
		OrgID:     &org.ID,
		Role:      role,
	}

	if err := h.db.CreateUser(user); err != nil {
		logrus.WithError(err).Error("Failed to create user")
		// Try to clean up Vault credential
		if h.vaultProvider != nil {
			_ = h.vaultProvider.DeleteCredential(accessKey)
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to create user"})
		return
	}

	logrus.WithFields(logrus.Fields{
		"user_id":    user.ID,
		"org_id":     org.ID,
		"access_key": accessKey,
		"role":       role,
	}).Info("User created for organization")

	// Return credentials (only time secret is shown)
	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"access_key": accessKey,
		"secret_key": secretKey,
		"email":      req.Email,
		"role":       role,
		"org_slug":   org.Slug,
	})
}

// ListOrgUsersHandler lists users in an organization
// GET /api/orgs/{slug}/users
func (h *TenantHandlers) ListOrgUsersHandler(w http.ResponseWriter, r *http.Request) {
	slug := mux.Vars(r)["slug"]

	org, err := h.db.GetOrgBySlug(slug)
	if err != nil || org == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Organization not found"})
		return
	}

	if !h.isOrgAdmin(r, org.ID) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "Org admin access required"})
		return
	}

	users, err := h.db.GetUsersByOrgID(org.ID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to list users"})
		return
	}

	// Strip secret keys from response
	type safeUser struct {
		ID        int        `json:"id"`
		AccessKey string     `json:"access_key"`
		Email     string     `json:"email"`
		Role      string     `json:"role"`
		Active    bool       `json:"active"`
		CreatedAt time.Time  `json:"created_at"`
		LastLogin *time.Time `json:"last_login,omitempty"`
	}

	safeUsers := make([]safeUser, 0, len(users))
	for _, u := range users {
		safeUsers = append(safeUsers, safeUser{
			ID:        u.ID,
			AccessKey: u.AccessKey,
			Email:     u.Email,
			Role:      u.Role,
			Active:    u.Active,
			CreatedAt: u.CreatedAt,
			LastLogin: u.LastLogin,
		})
	}

	writeJSON(w, http.StatusOK, safeUsers)
}

// RemoveUserFromOrgHandler removes a user from an organization
// DELETE /api/orgs/{slug}/users/{id}
func (h *TenantHandlers) RemoveUserFromOrgHandler(w http.ResponseWriter, r *http.Request) {
	slug := mux.Vars(r)["slug"]
	userIDStr := mux.Vars(r)["id"]

	org, err := h.db.GetOrgBySlug(slug)
	if err != nil || org == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Organization not found"})
		return
	}

	if !h.isOrgAdmin(r, org.ID) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "Org admin access required"})
		return
	}

	var userID int
	if _, err := fmt.Sscanf(userIDStr, "%d", &userID); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid user ID"})
		return
	}

	if err := h.db.DeleteUserByID(userID); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to remove user"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"message": "User removed"})
}

// CreateBucketMappingHandler creates a virtual bucket mapping
// POST /api/orgs/{slug}/buckets
func (h *TenantHandlers) CreateBucketMappingHandler(w http.ResponseWriter, r *http.Request) {
	slug := mux.Vars(r)["slug"]

	org, err := h.db.GetOrgBySlug(slug)
	if err != nil || org == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Organization not found"})
		return
	}

	if !h.isOrgAdmin(r, org.ID) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "Org admin access required"})
		return
	}

	var req struct {
		VirtualBucket  string `json:"virtual_bucket"`
		PhysicalBucket string `json:"physical_bucket"`
		Prefix         string `json:"prefix"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid request body"})
		return
	}

	if req.VirtualBucket == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "virtual_bucket is required"})
		return
	}

	physicalBucket := req.PhysicalBucket
	if physicalBucket == "" {
		physicalBucket = h.defaultBucket
	}

	prefix := req.Prefix
	if prefix == "" {
		prefix = slug + "/"
	}

	mapping := &database.OrgBucketMapping{
		OrgID:          org.ID,
		VirtualBucket:  req.VirtualBucket,
		PhysicalBucket: physicalBucket,
		Prefix:         prefix,
	}

	if err := h.db.CreateBucketMapping(mapping); err != nil {
		logrus.WithError(err).Error("Failed to create bucket mapping")
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to create bucket mapping"})
		return
	}

	logrus.WithFields(logrus.Fields{
		"org_id":          org.ID,
		"virtual_bucket":  req.VirtualBucket,
		"physical_bucket": physicalBucket,
		"prefix":          prefix,
	}).Info("Bucket mapping created")

	writeJSON(w, http.StatusCreated, mapping)
}

// ListBucketMappingsHandler lists bucket mappings for an org
// GET /api/orgs/{slug}/buckets
func (h *TenantHandlers) ListBucketMappingsHandler(w http.ResponseWriter, r *http.Request) {
	slug := mux.Vars(r)["slug"]

	org, err := h.db.GetOrgBySlug(slug)
	if err != nil || org == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Organization not found"})
		return
	}

	// Members can list buckets, not just admins
	if !isRequestAdmin(r) {
		ctxOrgID := middleware.GetOrgID(r.Context())
		if ctxOrgID != org.ID {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "Access denied"})
			return
		}
	}

	mappings, err := h.db.GetBucketMappingsByOrgID(org.ID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to list bucket mappings"})
		return
	}

	writeJSON(w, http.StatusOK, mappings)
}

// DeleteBucketMappingHandler deletes a virtual bucket mapping
// DELETE /api/orgs/{slug}/buckets/{name}
func (h *TenantHandlers) DeleteBucketMappingHandler(w http.ResponseWriter, r *http.Request) {
	slug := mux.Vars(r)["slug"]
	bucketName := mux.Vars(r)["name"]

	org, err := h.db.GetOrgBySlug(slug)
	if err != nil || org == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Organization not found"})
		return
	}

	if !h.isOrgAdmin(r, org.ID) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "Org admin access required"})
		return
	}

	if err := h.db.DeleteBucketMapping(org.ID, bucketName); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to delete bucket mapping"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"message": "Bucket mapping deleted"})
}

// isOrgAdmin checks if the requesting user is an admin of the specified org
func (h *TenantHandlers) isOrgAdmin(r *http.Request, orgID string) bool {
	if isRequestAdmin(r) {
		return true
	}

	ctxOrgID := middleware.GetOrgID(r.Context())
	ctxRole := middleware.GetUserRole(r.Context())

	return ctxOrgID == orgID && ctxRole == "admin"
}

// isRequestAdmin checks if the request has global admin privileges
func isRequestAdmin(r *http.Request) bool {
	if v := r.Context().Value("is_admin"); v != nil {
		if admin, ok := v.(bool); ok {
			return admin
		}
	}
	return false
}

// writeJSON writes a JSON response
func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		logrus.WithError(err).Error("Failed to encode JSON response")
	}
}

// generateAccessKey creates a random AWS-style access key
func generateAccessKey() (string, error) {
	bytes := make([]byte, 20)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	key := base64.URLEncoding.EncodeToString(bytes)
	key = strings.ReplaceAll(key, "-", "")
	key = strings.ReplaceAll(key, "_", "")
	key = strings.ToUpper(key)
	if len(key) > 20 {
		key = key[:20]
	}
	return key, nil
}

// generateSecretKey creates a random 40-character secret key
func generateSecretKey() (string, error) {
	bytes := make([]byte, 30)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	key := base64.URLEncoding.EncodeToString(bytes)
	if len(key) > 40 {
		key = key[:40]
	}
	return key, nil
}
