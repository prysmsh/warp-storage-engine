package storage

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/prysmsh/warp-storage-engine/internal/database"
	"github.com/prysmsh/warp-storage-engine/internal/middleware"
	"github.com/sirupsen/logrus"
)

// TenantAwareBackend wraps a storage Backend and translates virtual buckets
// to physical bucket+prefix based on the org's bucket mappings.
type TenantAwareBackend struct {
	backend Backend
	db      database.UserStore
}

// NewTenantAwareBackend creates a tenant-aware storage wrapper
func NewTenantAwareBackend(backend Backend, db database.UserStore) *TenantAwareBackend {
	return &TenantAwareBackend{
		backend: backend,
		db:      db,
	}
}

// resolveMapping looks up the physical bucket and prefix for a virtual bucket
func (t *TenantAwareBackend) resolveMapping(ctx context.Context, virtualBucket string) (physicalBucket, prefix string, err error) {
	orgID := middleware.GetOrgID(ctx)
	if orgID == "" {
		// No org context — pass through directly (backward compatibility)
		return virtualBucket, "", nil
	}

	mapping, err := t.db.GetBucketMapping(orgID, virtualBucket)
	if err != nil {
		return "", "", fmt.Errorf("failed to resolve bucket mapping: %w", err)
	}
	if mapping == nil {
		return "", "", fmt.Errorf("bucket %q not found for organization", virtualBucket)
	}

	return mapping.PhysicalBucket, mapping.Prefix, nil
}

// prefixKey prepends the org prefix to a key
func prefixKey(prefix, key string) string {
	if prefix == "" {
		return key
	}
	prefix = strings.TrimSuffix(prefix, "/") + "/"
	return prefix + key
}

// stripPrefix removes the org prefix from a key
func stripPrefix(prefix, key string) string {
	if prefix == "" {
		return key
	}
	prefix = strings.TrimSuffix(prefix, "/") + "/"
	return strings.TrimPrefix(key, prefix)
}

// ListBuckets returns only the virtual buckets belonging to the user's org
func (t *TenantAwareBackend) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	orgID := middleware.GetOrgID(ctx)
	if orgID == "" {
		// No org context — fall back to real backend
		return t.backend.ListBuckets(ctx)
	}

	mappings, err := t.db.GetBucketMappingsByOrgID(orgID)
	if err != nil {
		return nil, fmt.Errorf("failed to list bucket mappings: %w", err)
	}

	buckets := make([]BucketInfo, 0, len(mappings))
	for _, m := range mappings {
		buckets = append(buckets, BucketInfo{
			Name:         m.VirtualBucket,
			CreationDate: m.CreatedAt,
		})
	}

	return buckets, nil
}

// CreateBucket creates a virtual bucket mapping (not a physical bucket)
func (t *TenantAwareBackend) CreateBucket(ctx context.Context, bucket string) error {
	orgID := middleware.GetOrgID(ctx)
	if orgID == "" {
		return t.backend.CreateBucket(ctx, bucket)
	}

	// Check if mapping already exists
	existing, err := t.db.GetBucketMapping(orgID, bucket)
	if err != nil {
		return fmt.Errorf("failed to check bucket mapping: %w", err)
	}
	if existing != nil {
		return nil // Already exists
	}

	// Resolve org slug for the prefix
	orgSlug := middleware.GetOrgSlug(ctx)
	if orgSlug == "" {
		org, err := t.db.GetOrgByID(orgID)
		if err != nil || org == nil {
			return fmt.Errorf("failed to resolve org for bucket creation")
		}
		orgSlug = org.Slug
	}

	// For now, we need a default physical bucket — this should come from config
	// The caller can override this by using the tenant API directly
	mapping := &database.OrgBucketMapping{
		OrgID:          orgID,
		VirtualBucket:  bucket,
		PhysicalBucket: bucket, // Default: same name
		Prefix:         orgSlug + "/",
	}

	return t.db.CreateBucketMapping(mapping)
}

// DeleteBucket deletes the virtual bucket mapping
func (t *TenantAwareBackend) DeleteBucket(ctx context.Context, bucket string) error {
	orgID := middleware.GetOrgID(ctx)
	if orgID == "" {
		return t.backend.DeleteBucket(ctx, bucket)
	}

	return t.db.DeleteBucketMapping(orgID, bucket)
}

// BucketExists checks if the virtual bucket mapping exists
func (t *TenantAwareBackend) BucketExists(ctx context.Context, bucket string) (bool, error) {
	orgID := middleware.GetOrgID(ctx)
	if orgID == "" {
		return t.backend.BucketExists(ctx, bucket)
	}

	mapping, err := t.db.GetBucketMapping(orgID, bucket)
	if err != nil {
		return false, err
	}
	return mapping != nil, nil
}

// ListObjects lists objects under the virtual bucket with prefix translation
func (t *TenantAwareBackend) ListObjects(ctx context.Context, bucket, reqPrefix, marker string, maxKeys int) (*ListObjectsResult, error) {
	physBucket, orgPrefix, err := t.resolveMapping(ctx, bucket)
	if err != nil {
		return nil, err
	}

	fullPrefix := prefixKey(orgPrefix, reqPrefix)
	fullMarker := marker
	if marker != "" && orgPrefix != "" {
		fullMarker = prefixKey(orgPrefix, marker)
	}

	result, err := t.backend.ListObjects(ctx, physBucket, fullPrefix, fullMarker, maxKeys)
	if err != nil {
		return nil, err
	}

	// Strip prefix from returned keys
	if orgPrefix != "" {
		for i := range result.Contents {
			result.Contents[i].Key = stripPrefix(orgPrefix, result.Contents[i].Key)
		}
		if result.NextMarker != "" {
			result.NextMarker = stripPrefix(orgPrefix, result.NextMarker)
		}
	}

	return result, nil
}

// ListObjectsWithDelimiter lists objects with delimiter and prefix translation
func (t *TenantAwareBackend) ListObjectsWithDelimiter(ctx context.Context, bucket, reqPrefix, marker, delimiter string, maxKeys int) (*ListObjectsResult, error) {
	physBucket, orgPrefix, err := t.resolveMapping(ctx, bucket)
	if err != nil {
		return nil, err
	}

	fullPrefix := prefixKey(orgPrefix, reqPrefix)
	fullMarker := marker
	if marker != "" && orgPrefix != "" {
		fullMarker = prefixKey(orgPrefix, marker)
	}

	result, err := t.backend.ListObjectsWithDelimiter(ctx, physBucket, fullPrefix, fullMarker, delimiter, maxKeys)
	if err != nil {
		return nil, err
	}

	if orgPrefix != "" {
		for i := range result.Contents {
			result.Contents[i].Key = stripPrefix(orgPrefix, result.Contents[i].Key)
		}
		for i := range result.CommonPrefixes {
			result.CommonPrefixes[i] = stripPrefix(orgPrefix, result.CommonPrefixes[i])
		}
		if result.NextMarker != "" {
			result.NextMarker = stripPrefix(orgPrefix, result.NextMarker)
		}
	}

	return result, nil
}

// ListDeletedObjects lists deleted objects with prefix translation
func (t *TenantAwareBackend) ListDeletedObjects(ctx context.Context, bucket, reqPrefix, marker string, maxKeys int) (*ListObjectsResult, error) {
	physBucket, orgPrefix, err := t.resolveMapping(ctx, bucket)
	if err != nil {
		return nil, err
	}

	fullPrefix := prefixKey(orgPrefix, reqPrefix)
	fullMarker := marker
	if marker != "" && orgPrefix != "" {
		fullMarker = prefixKey(orgPrefix, marker)
	}

	result, err := t.backend.ListDeletedObjects(ctx, physBucket, fullPrefix, fullMarker, maxKeys)
	if err != nil {
		return nil, err
	}

	if orgPrefix != "" {
		for i := range result.Contents {
			result.Contents[i].Key = stripPrefix(orgPrefix, result.Contents[i].Key)
		}
		if result.NextMarker != "" {
			result.NextMarker = stripPrefix(orgPrefix, result.NextMarker)
		}
	}

	return result, nil
}

// GetObject retrieves an object with key prefix translation
func (t *TenantAwareBackend) GetObject(ctx context.Context, bucket, key string) (*Object, error) {
	physBucket, orgPrefix, err := t.resolveMapping(ctx, bucket)
	if err != nil {
		return nil, err
	}
	return t.backend.GetObject(ctx, physBucket, prefixKey(orgPrefix, key))
}

// PutObject stores an object with key prefix translation
func (t *TenantAwareBackend) PutObject(ctx context.Context, bucket, key string, reader io.Reader, size int64, metadata map[string]string) error {
	physBucket, orgPrefix, err := t.resolveMapping(ctx, bucket)
	if err != nil {
		return err
	}

	logrus.WithFields(logrus.Fields{
		"virtual_bucket":  bucket,
		"physical_bucket": physBucket,
		"virtual_key":     key,
		"physical_key":    prefixKey(orgPrefix, key),
	}).Debug("Tenant storage: PutObject")

	return t.backend.PutObject(ctx, physBucket, prefixKey(orgPrefix, key), reader, size, metadata)
}

// DeleteObject deletes an object with key prefix translation
func (t *TenantAwareBackend) DeleteObject(ctx context.Context, bucket, key string) error {
	physBucket, orgPrefix, err := t.resolveMapping(ctx, bucket)
	if err != nil {
		return err
	}
	return t.backend.DeleteObject(ctx, physBucket, prefixKey(orgPrefix, key))
}

// RestoreObject restores an object with key prefix translation
func (t *TenantAwareBackend) RestoreObject(ctx context.Context, bucket, key, versionID string) error {
	physBucket, orgPrefix, err := t.resolveMapping(ctx, bucket)
	if err != nil {
		return err
	}
	return t.backend.RestoreObject(ctx, physBucket, prefixKey(orgPrefix, key), versionID)
}

// HeadObject retrieves object metadata with key prefix translation
func (t *TenantAwareBackend) HeadObject(ctx context.Context, bucket, key string) (*ObjectInfo, error) {
	physBucket, orgPrefix, err := t.resolveMapping(ctx, bucket)
	if err != nil {
		return nil, err
	}
	info, err := t.backend.HeadObject(ctx, physBucket, prefixKey(orgPrefix, key))
	if err != nil {
		return nil, err
	}
	if info != nil && orgPrefix != "" {
		info.Key = stripPrefix(orgPrefix, info.Key)
	}
	return info, nil
}

// GetObjectACL gets ACL with prefix translation
func (t *TenantAwareBackend) GetObjectACL(ctx context.Context, bucket, key string) (*ACL, error) {
	physBucket, orgPrefix, err := t.resolveMapping(ctx, bucket)
	if err != nil {
		return nil, err
	}
	return t.backend.GetObjectACL(ctx, physBucket, prefixKey(orgPrefix, key))
}

// PutObjectACL sets ACL with prefix translation
func (t *TenantAwareBackend) PutObjectACL(ctx context.Context, bucket, key string, acl *ACL) error {
	physBucket, orgPrefix, err := t.resolveMapping(ctx, bucket)
	if err != nil {
		return err
	}
	return t.backend.PutObjectACL(ctx, physBucket, prefixKey(orgPrefix, key), acl)
}

// InitiateMultipartUpload initiates multipart upload with key prefix translation
func (t *TenantAwareBackend) InitiateMultipartUpload(ctx context.Context, bucket, key string, metadata map[string]string) (string, error) {
	physBucket, orgPrefix, err := t.resolveMapping(ctx, bucket)
	if err != nil {
		return "", err
	}
	return t.backend.InitiateMultipartUpload(ctx, physBucket, prefixKey(orgPrefix, key), metadata)
}

// UploadPart uploads a part with key prefix translation
func (t *TenantAwareBackend) UploadPart(ctx context.Context, bucket, key, uploadID string, partNumber int, reader io.Reader, size int64) (string, error) {
	physBucket, orgPrefix, err := t.resolveMapping(ctx, bucket)
	if err != nil {
		return "", err
	}
	return t.backend.UploadPart(ctx, physBucket, prefixKey(orgPrefix, key), uploadID, partNumber, reader, size)
}

// CompleteMultipartUpload completes multipart upload with key prefix translation
func (t *TenantAwareBackend) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []CompletedPart) error {
	physBucket, orgPrefix, err := t.resolveMapping(ctx, bucket)
	if err != nil {
		return err
	}
	return t.backend.CompleteMultipartUpload(ctx, physBucket, prefixKey(orgPrefix, key), uploadID, parts)
}

// AbortMultipartUpload aborts multipart upload with key prefix translation
func (t *TenantAwareBackend) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	physBucket, orgPrefix, err := t.resolveMapping(ctx, bucket)
	if err != nil {
		return err
	}
	return t.backend.AbortMultipartUpload(ctx, physBucket, prefixKey(orgPrefix, key), uploadID)
}

// ListParts lists parts with key prefix translation
func (t *TenantAwareBackend) ListParts(ctx context.Context, bucket, key, uploadID string, maxParts int, partNumberMarker int) (*ListPartsResult, error) {
	physBucket, orgPrefix, err := t.resolveMapping(ctx, bucket)
	if err != nil {
		return nil, err
	}
	return t.backend.ListParts(ctx, physBucket, prefixKey(orgPrefix, key), uploadID, maxParts, partNumberMarker)
}

// Ensure TenantAwareBackend implements Backend
var _ Backend = (*TenantAwareBackend)(nil)
