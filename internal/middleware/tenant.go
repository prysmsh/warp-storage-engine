package middleware

import (
	"context"
	"net/http"

	"github.com/prysmsh/warp-storage-engine/internal/database"
	"github.com/sirupsen/logrus"
)

type contextKey string

const (
	CtxOrgID           contextKey = "org_id"
	CtxOrgSlug         contextKey = "org_slug"
	CtxUserRole        contextKey = "user_role"
	CtxUserPermissions contextKey = "user_permissions"
)

// TenantContextMiddleware sets org_id, org_slug, user_role in the request context
// after authentication has succeeded. It reads the user's access key from context
// and resolves their org membership from the database.
func TenantContextMiddleware(db database.UserStore) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Only proceed if user is authenticated
			authVal := r.Context().Value("authenticated")
			if authVal == nil {
				next.ServeHTTP(w, r)
				return
			}
			authenticated, ok := authVal.(bool)
			if !ok || !authenticated {
				next.ServeHTTP(w, r)
				return
			}

			// Get user_sub (access key) from context
			userSub, _ := r.Context().Value("user_sub").(string)
			if userSub == "" {
				next.ServeHTTP(w, r)
				return
			}

			// Look up user in DB to get org_id and role
			user, err := db.GetUserByAccessKey(userSub)
			if err != nil || user == nil {
				logrus.WithFields(logrus.Fields{
					"access_key": userSub,
					"error":      err,
				}).Debug("Could not resolve tenant context for user")
				next.ServeHTTP(w, r)
				return
			}

			ctx := r.Context()

			if user.OrgID != nil && *user.OrgID != "" {
				ctx = context.WithValue(ctx, CtxOrgID, *user.OrgID)

				// Resolve org slug
				org, err := db.GetOrgByID(*user.OrgID)
				if err == nil && org != nil {
					ctx = context.WithValue(ctx, CtxOrgSlug, org.Slug)
				}
			}

			if user.Role != "" {
				ctx = context.WithValue(ctx, CtxUserRole, user.Role)
			}

			// Load user permissions
			perms, err := db.GetUserPermissions(user.ID)
			if err == nil && len(perms) > 0 {
				ctx = context.WithValue(ctx, CtxUserPermissions, perms)
			}

			r = r.WithContext(ctx)
			next.ServeHTTP(w, r)
		})
	}
}

// GetOrgID extracts the org_id from request context
func GetOrgID(ctx context.Context) string {
	if v, ok := ctx.Value(CtxOrgID).(string); ok {
		return v
	}
	return ""
}

// GetOrgSlug extracts the org_slug from request context
func GetOrgSlug(ctx context.Context) string {
	if v, ok := ctx.Value(CtxOrgSlug).(string); ok {
		return v
	}
	return ""
}

// GetUserRole extracts the user_role from request context
func GetUserRole(ctx context.Context) string {
	if v, ok := ctx.Value(CtxUserRole).(string); ok {
		return v
	}
	return ""
}
