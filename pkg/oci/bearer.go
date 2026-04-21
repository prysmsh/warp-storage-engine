package oci

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// We roll an HS256 JWT by hand to avoid a direct dependency on golang-jwt
// (which is only transitively pulled in by other modules). The token is
// short-lived (<= 1h by default), single-issuer, single-audience — the
// surface area where a library would matter is tiny.

type tokenClaims struct {
	Iss      string       `json:"iss"`
	Sub      string       `json:"sub"`
	Aud      string       `json:"aud"`
	Iat      int64        `json:"iat"`
	Nbf      int64        `json:"nbf"`
	Exp      int64        `json:"exp"`
	Jti      string       `json:"jti,omitempty"`
	Access   []accessGrant `json:"access,omitempty"`
}

// accessGrant matches the Docker token-server spec:
//
//	{"type": "repository", "name": "myrepo", "actions": ["pull","push"]}
type accessGrant struct {
	Type    string   `json:"type"`
	Name    string   `json:"name"`
	Actions []string `json:"actions"`
}

// handleToken implements a Docker/OCI token server:
//
//	GET/POST /auth/token?service=<svc>&scope=repository:<repo>:pull,push
//	Authorization: Basic base64(user:pass)
//	→ {"token": "<jwt>", "access_token": "<jwt>", "expires_in": 3600, "issued_at": "..."}
//
// Anonymous access is allowed when Username is empty — the returned token
// carries only the requested scopes (and the handler may ignore unauthorized
// scopes when validating later, but for Phase 2b the token grants what was
// asked). Username-set deployments require Basic creds on this endpoint.
func (h *Handler) handleToken(w http.ResponseWriter, r *http.Request) {
	cfg := h.cfg.Bearer
	if !cfg.Enabled {
		writeError(w, http.StatusNotFound, ErrCodeUnsupported, "bearer auth disabled")
		return
	}

	// Authenticate the caller via Basic if credentials are configured.
	sub := "anonymous"
	if h.cfg.Username != "" {
		if !h.basicAuthOK(r) {
			w.Header().Set("WWW-Authenticate", `Basic realm="warp-oci-token"`)
			writeError(w, http.StatusUnauthorized, ErrCodeUnauthorized, "invalid credentials")
			return
		}
		sub = h.cfg.Username
	}

	service := r.URL.Query().Get("service")
	if service == "" {
		service = cfg.Service
	}
	scopes := r.URL.Query()["scope"]

	access := parseScopes(scopes)
	tok, exp, err := h.issueToken(sub, service, access)
	if err != nil {
		writeError(w, http.StatusInternalServerError, ErrCodeUnsupported, "failed to issue token")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"token":        tok,
		"access_token": tok,
		"expires_in":   int(cfg.TokenTTL.Seconds()),
		"issued_at":    time.Now().UTC().Format(time.RFC3339),
		"sub":          sub,
		"service":      service,
		"exp":          exp,
	})
}

// parseScopes turns Docker-style scope strings ("repository:foo/bar:pull,push")
// into accessGrant records. Unknown scope formats are dropped silently; the
// handler does per-request authorization separately.
func parseScopes(scopes []string) []accessGrant {
	out := make([]accessGrant, 0, len(scopes))
	for _, s := range scopes {
		parts := strings.SplitN(s, ":", 3)
		if len(parts) != 3 {
			continue
		}
		out = append(out, accessGrant{
			Type:    parts[0],
			Name:    parts[1],
			Actions: strings.Split(parts[2], ","),
		})
	}
	return out
}

func (h *Handler) issueToken(sub, service string, access []accessGrant) (string, int64, error) {
	cfg := h.cfg.Bearer
	if cfg.Secret == "" {
		return "", 0, fmt.Errorf("bearer secret not configured")
	}
	now := time.Now().Unix()
	exp := now + int64(cfg.TokenTTL.Seconds())

	header := map[string]string{"alg": "HS256", "typ": "JWT"}
	claims := tokenClaims{
		Iss:    service,
		Sub:    sub,
		Aud:    service,
		Iat:    now,
		Nbf:    now,
		Exp:    exp,
		Access: access,
	}

	hb, _ := json.Marshal(header)
	cb, _ := json.Marshal(claims)
	hEnc := base64URL(hb)
	cEnc := base64URL(cb)
	signingInput := hEnc + "." + cEnc
	sig := hmacSign(signingInput, cfg.Secret)
	return signingInput + "." + sig, exp, nil
}

// validateBearer parses and verifies a compact HS256 JWT. Returns the claims
// on success, a spec-shaped error code/message otherwise.
func (h *Handler) validateBearer(raw string) (*tokenClaims, string, string) {
	cfg := h.cfg.Bearer
	parts := strings.Split(raw, ".")
	if len(parts) != 3 {
		return nil, ErrCodeUnauthorized, "malformed token"
	}
	signingInput := parts[0] + "." + parts[1]
	expected := hmacSign(signingInput, cfg.Secret)
	if subtle.ConstantTimeCompare([]byte(expected), []byte(parts[2])) != 1 {
		return nil, ErrCodeUnauthorized, "signature mismatch"
	}

	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, ErrCodeUnauthorized, "unreadable payload"
	}
	var claims tokenClaims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, ErrCodeUnauthorized, "invalid claims"
	}

	now := time.Now().Unix()
	if claims.Exp != 0 && now >= claims.Exp {
		return nil, ErrCodeUnauthorized, "token expired"
	}
	if claims.Nbf != 0 && now < claims.Nbf {
		return nil, ErrCodeUnauthorized, "token not yet valid"
	}
	if claims.Aud != "" && claims.Aud != cfg.Service {
		return nil, ErrCodeUnauthorized, "audience mismatch"
	}
	return &claims, "", ""
}

// bearerChallenge sets the WWW-Authenticate header per the OCI Distribution
// spec for bearer auth. Clients parse this to discover where to fetch a token.
func (h *Handler) bearerChallenge(w http.ResponseWriter, scope string) {
	cfg := h.cfg.Bearer
	realm := cfg.Realm
	if realm == "" {
		// Sensible default if operator didn't set realm — point at our own
		// /auth/token on the same listener. Real deployments should override.
		realm = "/auth/token"
	}
	challenge := fmt.Sprintf(`Bearer realm=%q,service=%q`, realm, cfg.Service)
	if scope != "" {
		challenge += fmt.Sprintf(`,scope=%q`, scope)
	}
	w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
	w.Header().Set("WWW-Authenticate", challenge)
	writeError(w, http.StatusUnauthorized, ErrCodeUnauthorized, "authentication required")
}

func base64URL(b []byte) string {
	return base64.RawURLEncoding.EncodeToString(b)
}

func hmacSign(input, secret string) string {
	m := hmac.New(sha256.New, []byte(secret))
	_, _ = m.Write([]byte(input))
	return base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}
