package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	vaultapi "github.com/hashicorp/vault/api"
	"github.com/sirupsen/logrus"

	"github.com/prysmsh/warp-storage-engine/internal/config"
)

// cachedCredential holds a Vault-fetched credential with TTL tracking
type cachedCredential struct {
	secretKey string
	orgID     string
	fetchedAt time.Time
}

// VaultMultiUserProvider looks up per-user secrets from Vault for AWS Sig V4 verification
type VaultMultiUserProvider struct {
	client    *vaultapi.Client
	cache     *lru.Cache[string, *cachedCredential]
	mountPath string
	basePath  string
	cacheTTL  time.Duration
	logger    *logrus.Entry
}

// NewVaultMultiUserProvider creates a new multi-tenant Vault credential provider
func NewVaultMultiUserProvider(cfg config.AuthConfig) (*VaultMultiUserProvider, error) {
	if cfg.Vault == nil || !cfg.Vault.Enabled {
		return nil, fmt.Errorf("vault authentication must be enabled for vault_multiuser provider")
	}

	clientCfg := vaultapi.DefaultConfig()
	if cfg.Vault.Address != "" {
		clientCfg.Address = cfg.Vault.Address
	}

	client, err := vaultapi.NewClient(clientCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create Vault client: %w", err)
	}

	token, err := resolveVaultToken(cfg.Vault)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve Vault token: %w", err)
	}
	if token != "" {
		client.SetToken(token)
	}

	cacheSize := 10000
	ttl := 5 * time.Minute
	mountPath := "secret"
	basePath := "fse/users"

	// Use multi-tenant vault config from the main auth vault config
	// The mount_path and base_path are shared
	if cfg.Vault.MountPath != "" {
		mountPath = cfg.Vault.MountPath
	}

	cache, err := lru.New[string, *cachedCredential](cacheSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create LRU cache: %w", err)
	}

	provider := &VaultMultiUserProvider{
		client:    client,
		cache:     cache,
		mountPath: mountPath,
		basePath:  basePath,
		cacheTTL:  ttl,
		logger:    logrus.WithField("component", "vault-multiuser"),
	}

	provider.logger.WithFields(logrus.Fields{
		"mount_path": mountPath,
		"base_path":  basePath,
		"cache_size": cacheSize,
		"cache_ttl":  ttl,
	}).Info("Vault multi-user provider initialized")

	return provider, nil
}

// NewVaultMultiUserProviderWithConfig creates a provider using the dedicated MultitenancyConfig
func NewVaultMultiUserProviderWithConfig(authCfg config.AuthConfig, mtCfg config.MultitenancyConfig) (*VaultMultiUserProvider, error) {
	if authCfg.Vault == nil || !authCfg.Vault.Enabled {
		return nil, fmt.Errorf("vault authentication must be enabled for vault_multiuser provider")
	}

	clientCfg := vaultapi.DefaultConfig()
	if authCfg.Vault.Address != "" {
		clientCfg.Address = authCfg.Vault.Address
	}

	client, err := vaultapi.NewClient(clientCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create Vault client: %w", err)
	}

	token, err := resolveVaultToken(authCfg.Vault)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve Vault token: %w", err)
	}
	if token != "" {
		client.SetToken(token)
	}

	cacheSize := 10000
	ttl := 5 * time.Minute
	mountPath := "secret"
	basePath := "fse/users"

	if mtCfg.Vault != nil {
		if mtCfg.Vault.MountPath != "" {
			mountPath = mtCfg.Vault.MountPath
		}
		if mtCfg.Vault.BasePath != "" {
			basePath = mtCfg.Vault.BasePath
		}
		if mtCfg.Vault.CacheSize > 0 {
			cacheSize = mtCfg.Vault.CacheSize
		}
		if mtCfg.Vault.CacheTTL > 0 {
			ttl = mtCfg.Vault.CacheTTL
		}
	}

	cache, err := lru.New[string, *cachedCredential](cacheSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create LRU cache: %w", err)
	}

	provider := &VaultMultiUserProvider{
		client:    client,
		cache:     cache,
		mountPath: mountPath,
		basePath:  basePath,
		cacheTTL:  ttl,
		logger:    logrus.WithField("component", "vault-multiuser"),
	}

	provider.logger.WithFields(logrus.Fields{
		"mount_path": mountPath,
		"base_path":  basePath,
		"cache_size": cacheSize,
		"cache_ttl":  ttl,
	}).Info("Vault multi-user provider initialized")

	return provider, nil
}

// fetchCredential retrieves a user's credential from Vault, using cache when available
func (p *VaultMultiUserProvider) fetchCredential(accessKey string) (*cachedCredential, error) {
	// Check cache
	if cached, ok := p.cache.Get(accessKey); ok {
		if time.Since(cached.fetchedAt) < p.cacheTTL {
			return cached, nil
		}
		// Expired, remove from cache
		p.cache.Remove(accessKey)
	}

	// Fetch from Vault
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mount := strings.Trim(p.mountPath, "/")
	secretPath := strings.Trim(p.basePath, "/") + "/" + accessKey

	secret, err := p.client.KVv2(mount).Get(ctx, secretPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read credential from Vault for %s: %w", maskCredential(accessKey), err)
	}

	secretKeyValue, ok := secret.Data["secret_key"]
	if !ok {
		return nil, fmt.Errorf("vault secret missing 'secret_key' field for %s", maskCredential(accessKey))
	}
	secretKey, ok := secretKeyValue.(string)
	if !ok || secretKey == "" {
		return nil, fmt.Errorf("vault 'secret_key' is empty or not a string for %s", maskCredential(accessKey))
	}

	var orgID string
	if orgIDValue, ok := secret.Data["org_id"]; ok {
		if orgIDStr, ok := orgIDValue.(string); ok {
			orgID = orgIDStr
		}
	}

	cred := &cachedCredential{
		secretKey: secretKey,
		orgID:     orgID,
		fetchedAt: time.Now(),
	}

	p.cache.Add(accessKey, cred)

	p.logger.WithField("access_key", maskCredential(accessKey)).Debug("Fetched credential from Vault")
	return cred, nil
}

// extractAccessKey parses the access key from the Authorization header
func (p *VaultMultiUserProvider) extractAccessKey(r *http.Request) (string, error) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return "", fmt.Errorf("missing authorization header")
	}

	if strings.HasPrefix(authHeader, "AWS4-HMAC-SHA256 ") {
		// AWS Signature Version 4
		credIdx := strings.Index(authHeader, "Credential=")
		if credIdx == -1 {
			return "", fmt.Errorf("missing credential in authorization header")
		}
		credStart := credIdx + 11
		remaining := authHeader[credStart:]
		slashIdx := strings.Index(remaining, "/")
		if slashIdx == -1 {
			return "", fmt.Errorf("invalid credential format")
		}
		return remaining[:slashIdx], nil
	} else if strings.HasPrefix(authHeader, "AWS ") {
		// AWS Signature Version 2
		parts := strings.SplitN(authHeader[4:], ":", 2)
		if len(parts) != 2 {
			return "", fmt.Errorf("invalid authorization header format")
		}
		return parts[0], nil
	}

	return "", fmt.Errorf("unsupported authorization method")
}

// Authenticate validates the AWS signature using the per-user secret from Vault
func (p *VaultMultiUserProvider) Authenticate(r *http.Request) error {
	accessKey, err := p.extractAccessKey(r)
	if err != nil {
		return err
	}

	cred, err := p.fetchCredential(accessKey)
	if err != nil {
		return fmt.Errorf("credential lookup failed: %w", err)
	}

	authHeader := r.Header.Get("Authorization")

	// Delegate signature verification to the appropriate provider
	if strings.HasPrefix(authHeader, "AWS4-HMAC-SHA256 ") {
		tempProvider := &AWSV4Provider{
			identity:   accessKey,
			credential: cred.secretKey,
		}
		return tempProvider.Authenticate(r)
	} else if strings.HasPrefix(authHeader, "AWS ") {
		tempProvider := &AWSV2Provider{
			identity:   accessKey,
			credential: cred.secretKey,
		}
		return tempProvider.Authenticate(r)
	}

	return fmt.Errorf("unsupported authorization method")
}

// GetSecretKey returns the plaintext secret key for a given access key
func (p *VaultMultiUserProvider) GetSecretKey(accessKey string) (string, error) {
	cred, err := p.fetchCredential(accessKey)
	if err != nil {
		return "", err
	}
	return cred.secretKey, nil
}

// GetUserContext returns the tenant context (org_id, role) for a given access key
func (p *VaultMultiUserProvider) GetUserContext(accessKey string) (*UserContext, error) {
	cred, err := p.fetchCredential(accessKey)
	if err != nil {
		return nil, err
	}
	return &UserContext{
		AccessKey: accessKey,
		OrgID:     cred.orgID,
	}, nil
}

// StoreCredential stores a user's secret key in Vault
func (p *VaultMultiUserProvider) StoreCredential(accessKey, secretKey, orgID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mount := strings.Trim(p.mountPath, "/")
	secretPath := strings.Trim(p.basePath, "/") + "/" + accessKey

	data := map[string]interface{}{
		"secret_key": secretKey,
		"org_id":     orgID,
	}

	_, err := p.client.KVv2(mount).Put(ctx, secretPath, data)
	if err != nil {
		return fmt.Errorf("failed to store credential in Vault: %w", err)
	}

	// Update cache
	p.cache.Add(accessKey, &cachedCredential{
		secretKey: secretKey,
		orgID:     orgID,
		fetchedAt: time.Now(),
	})

	p.logger.WithField("access_key", maskCredential(accessKey)).Info("Stored credential in Vault")
	return nil
}

// DeleteCredential removes a user's secret key from Vault
func (p *VaultMultiUserProvider) DeleteCredential(accessKey string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mount := strings.Trim(p.mountPath, "/")
	secretPath := strings.Trim(p.basePath, "/") + "/" + accessKey

	err := p.client.KVv2(mount).Delete(ctx, secretPath)
	if err != nil {
		return fmt.Errorf("failed to delete credential from Vault: %w", err)
	}

	p.cache.Remove(accessKey)

	p.logger.WithField("access_key", maskCredential(accessKey)).Info("Deleted credential from Vault")
	return nil
}

// verifySignatureV4 is a helper for external callers that need to verify a signature
// given a known access key and secret key, without a full http.Request.
func verifySignatureV4(secretKey, dateStr, region, service, stringToSign string) string {
	signingKey := getSigningKey(secretKey, dateStr, region, service)
	h := hmac.New(sha256.New, signingKey)
	h.Write([]byte(stringToSign))
	return hex.EncodeToString(h.Sum(nil))
}
