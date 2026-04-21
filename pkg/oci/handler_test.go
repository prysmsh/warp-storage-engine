package oci

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/einyx/foundation-storage-engine/internal/config"
	"github.com/einyx/foundation-storage-engine/internal/storage"
)

// memoryBackend is a minimal in-memory storage.Backend used solely for
// exercising the OCI handler in unit tests. It implements exactly the methods
// the OCI handler actually calls; the rest return errors to make misuse loud.
type memoryBackend struct {
	mu      sync.Mutex
	objects map[string][]byte
	meta    map[string]map[string]string
}

func newMemoryBackend() *memoryBackend {
	return &memoryBackend{
		objects: make(map[string][]byte),
		meta:    make(map[string]map[string]string),
	}
}

func key(bucket, k string) string { return bucket + "/" + k }

func (m *memoryBackend) PutObject(_ context.Context, bucket, k string, r io.Reader, _ int64, metadata map[string]string) error {
	buf, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[key(bucket, k)] = buf
	if metadata != nil {
		cp := make(map[string]string, len(metadata))
		for kk, vv := range metadata {
			cp[kk] = vv
		}
		m.meta[key(bucket, k)] = cp
	}
	return nil
}

func (m *memoryBackend) GetObject(_ context.Context, bucket, k string) (*storage.Object, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.objects[key(bucket, k)]
	if !ok {
		return nil, fmt.Errorf("not found: %s/%s", bucket, k)
	}
	meta := m.meta[key(bucket, k)]
	contentType := ""
	if meta != nil {
		contentType = meta["Content-Type"]
	}
	return &storage.Object{
		Body:        io.NopCloser(bytes.NewReader(data)),
		Size:        int64(len(data)),
		ContentType: contentType,
		ETag:        fmt.Sprintf("%x", sha256.Sum256(data)),
	}, nil
}

func (m *memoryBackend) HeadObject(_ context.Context, bucket, k string) (*storage.ObjectInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.objects[key(bucket, k)]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return &storage.ObjectInfo{Key: k, Size: int64(len(data))}, nil
}

func (m *memoryBackend) DeleteObject(_ context.Context, bucket, k string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.objects[key(bucket, k)]; !ok {
		return fmt.Errorf("not found")
	}
	delete(m.objects, key(bucket, k))
	delete(m.meta, key(bucket, k))
	return nil
}

func (m *memoryBackend) ListObjects(_ context.Context, bucket, prefix, _ string, _ int) (*storage.ListObjectsResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	pfx := bucket + "/" + prefix
	var contents []storage.ObjectInfo
	for k, v := range m.objects {
		if strings.HasPrefix(k, pfx) {
			contents = append(contents, storage.ObjectInfo{
				Key:  strings.TrimPrefix(k, bucket+"/"),
				Size: int64(len(v)),
			})
		}
	}
	return &storage.ListObjectsResult{Contents: contents}, nil
}

// Unused — satisfy interface only.
func (m *memoryBackend) ListBuckets(_ context.Context) ([]storage.BucketInfo, error) {
	return nil, nil
}
func (m *memoryBackend) CreateBucket(_ context.Context, _ string) error { return nil }
func (m *memoryBackend) DeleteBucket(_ context.Context, _ string) error { return nil }
func (m *memoryBackend) BucketExists(_ context.Context, _ string) (bool, error) {
	return true, nil
}
func (m *memoryBackend) ListObjectsWithDelimiter(_ context.Context, _, _, _, _ string, _ int) (*storage.ListObjectsResult, error) {
	return nil, fmt.Errorf("unused")
}
func (m *memoryBackend) ListDeletedObjects(_ context.Context, _, _, _ string, _ int) (*storage.ListObjectsResult, error) {
	return nil, fmt.Errorf("unused")
}
func (m *memoryBackend) RestoreObject(_ context.Context, _, _, _ string) error {
	return fmt.Errorf("unused")
}
func (m *memoryBackend) GetObjectACL(_ context.Context, _, _ string) (*storage.ACL, error) {
	return nil, fmt.Errorf("unused")
}
func (m *memoryBackend) PutObjectACL(_ context.Context, _, _ string, _ *storage.ACL) error {
	return fmt.Errorf("unused")
}
func (m *memoryBackend) InitiateMultipartUpload(_ context.Context, _, _ string, _ map[string]string) (string, error) {
	return "", fmt.Errorf("unused")
}
func (m *memoryBackend) UploadPart(_ context.Context, _, _, _ string, _ int, _ io.Reader, _ int64) (string, error) {
	return "", fmt.Errorf("unused")
}
func (m *memoryBackend) CompleteMultipartUpload(_ context.Context, _, _, _ string, _ []storage.CompletedPart) error {
	return fmt.Errorf("unused")
}
func (m *memoryBackend) AbortMultipartUpload(_ context.Context, _, _, _ string) error {
	return fmt.Errorf("unused")
}
func (m *memoryBackend) ListParts(_ context.Context, _, _, _ string, _ int, _ int) (*storage.ListPartsResult, error) {
	return nil, fmt.Errorf("unused")
}

// --- tests ---

func newTestServer(t *testing.T, username, password string) (*httptest.Server, *memoryBackend) {
	t.Helper()
	be := newMemoryBackend()
	h, err := NewHandler(be, config.OCIConfig{
		Bucket:   "oci",
		Username: username,
		Password: password,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return httptest.NewServer(h.Router()), be
}

func digestOf(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func TestV2Root_NoAuth(t *testing.T) {
	srv, _ := newTestServer(t, "", "")
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/v2/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("Docker-Distribution-API-Version"); got != "registry/2.0" {
		t.Fatalf("version header: got %q", got)
	}
}

func TestV2Root_ChallengeWhenAuthConfigured(t *testing.T) {
	srv, _ := newTestServer(t, "user", "pw")
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/v2/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status: got %d, want 401", resp.StatusCode)
	}
	if !strings.Contains(resp.Header.Get("WWW-Authenticate"), "Basic") {
		t.Fatalf("missing Basic challenge: %q", resp.Header.Get("WWW-Authenticate"))
	}
}

func TestMonolithicBlobPush_AndPull(t *testing.T) {
	srv, be := newTestServer(t, "", "")
	defer srv.Close()

	data := []byte("hello oci")
	digest := digestOf(data)

	// Monolithic: POST with ?digest=
	req, _ := http.NewRequest(
		http.MethodPost,
		srv.URL+"/v2/myrepo/blobs/uploads/?digest="+digest,
		bytes.NewReader(data),
	)
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusCreated {
		bod, _ := io.ReadAll(resp.Body)
		t.Fatalf("push status: got %d, want 201. body: %s", resp.StatusCode, bod)
	}
	resp.Body.Close()

	// Pull it back.
	resp, err = http.Get(srv.URL + "/v2/myrepo/blobs/" + digest)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("pull status: got %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !bytes.Equal(body, data) {
		t.Fatalf("pulled wrong bytes: got %q, want %q", body, data)
	}
	if resp.Header.Get("Docker-Content-Digest") != digest {
		t.Fatalf("digest header missing")
	}

	// Backend contains exactly one blob under blobs/.
	be.mu.Lock()
	defer be.mu.Unlock()
	if _, ok := be.objects["oci/blobs/"+digestHex(digest)]; !ok {
		t.Fatalf("expected blob to be stored at oci/blobs/%s", digestHex(digest))
	}
}

func TestChunkedBlobPush(t *testing.T) {
	srv, _ := newTestServer(t, "", "")
	defer srv.Close()

	chunk1 := []byte("part-one-")
	chunk2 := []byte("part-two")
	full := append(append([]byte{}, chunk1...), chunk2...)
	digest := digestOf(full)

	// 1. POST to start upload.
	resp, err := http.Post(srv.URL+"/v2/helm/charts/blobs/uploads/", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("start status: got %d, want 202", resp.StatusCode)
	}
	loc := resp.Header.Get("Location")
	resp.Body.Close()
	if loc == "" {
		t.Fatal("missing Location header")
	}

	// 2. PATCH chunk 1.
	patchURL := srv.URL + loc
	doChunkedPatch(t, patchURL, chunk1)

	// 3. PATCH chunk 2.
	doChunkedPatch(t, patchURL, chunk2)

	// 4. PUT to finalize (no body).
	req, _ := http.NewRequest(http.MethodPut, patchURL+"?digest="+digest, nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		bod, _ := io.ReadAll(resp.Body)
		t.Fatalf("finalize status: got %d, want 201. body: %s", resp.StatusCode, bod)
	}

	// 5. HEAD to verify blob is there.
	resp2, err := http.Head(srv.URL + "/v2/helm/charts/blobs/" + digest)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("head status: got %d, want 200", resp2.StatusCode)
	}
}

func doChunkedPatch(t *testing.T, url string, body []byte) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPatch, url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		bod, _ := io.ReadAll(resp.Body)
		t.Fatalf("patch status: got %d, want 202. body: %s", resp.StatusCode, bod)
	}
}

func TestManifestPush_ByTag_Pull_ListTags(t *testing.T) {
	srv, _ := newTestServer(t, "", "")
	defer srv.Close()

	manifest := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{}}`)
	digest := digestOf(manifest)

	// PUT by tag.
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/v2/myapp/manifests/v1", bytes.NewReader(manifest))
	req.Header.Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusCreated {
		bod, _ := io.ReadAll(resp.Body)
		t.Fatalf("put status: got %d, want 201. body: %s", resp.StatusCode, bod)
	}
	if got := resp.Header.Get("Docker-Content-Digest"); got != digest {
		t.Fatalf("digest header: got %q, want %q", got, digest)
	}
	resp.Body.Close()

	// GET by tag.
	resp, err = http.Get(srv.URL + "/v2/myapp/manifests/v1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get status: got %d", resp.StatusCode)
	}
	got, _ := io.ReadAll(resp.Body)
	if !bytes.Equal(got, manifest) {
		t.Fatalf("manifest body mismatch")
	}

	// GET by digest.
	resp2, err := http.Get(srv.URL + "/v2/myapp/manifests/" + digest)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("get-by-digest status: got %d", resp2.StatusCode)
	}

	// Push a second tag pointing at same manifest.
	req, _ = http.NewRequest(http.MethodPut, srv.URL+"/v2/myapp/manifests/latest", bytes.NewReader(manifest))
	req.Header.Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
	resp3, _ := http.DefaultClient.Do(req)
	resp3.Body.Close()

	// List tags.
	resp4, err := http.Get(srv.URL + "/v2/myapp/tags/list")
	if err != nil {
		t.Fatal(err)
	}
	defer resp4.Body.Close()
	var listResp tagListResponse
	if err := json.NewDecoder(resp4.Body).Decode(&listResp); err != nil {
		t.Fatal(err)
	}
	if listResp.Name != "myapp" {
		t.Fatalf("list name: got %q", listResp.Name)
	}
	wantSet := map[string]bool{"v1": true, "latest": true}
	if len(listResp.Tags) != 2 {
		t.Fatalf("expected 2 tags, got %v", listResp.Tags)
	}
	for _, tg := range listResp.Tags {
		if !wantSet[tg] {
			t.Fatalf("unexpected tag %q in %v", tg, listResp.Tags)
		}
	}
}

func TestManifestPutDigestMismatch(t *testing.T) {
	srv, _ := newTestServer(t, "", "")
	defer srv.Close()

	manifest := []byte(`{"schemaVersion":2}`)
	wrong := "sha256:0000000000000000000000000000000000000000000000000000000000000000"
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/v2/x/manifests/"+wrong, bytes.NewReader(manifest))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", resp.StatusCode)
	}
}

func TestDigestMismatchOnMonolithicPush(t *testing.T) {
	srv, _ := newTestServer(t, "", "")
	defer srv.Close()

	body := []byte("hello")
	wrong := digestOf([]byte("something else"))

	req, _ := http.NewRequest(
		http.MethodPost,
		srv.URL+"/v2/foo/blobs/uploads/?digest="+wrong,
		bytes.NewReader(body),
	)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", resp.StatusCode)
	}
}

func TestInvalidRepoName(t *testing.T) {
	srv, _ := newTestServer(t, "", "")
	defer srv.Close()

	// Upper-case not allowed.
	resp, err := http.Get(srv.URL + "/v2/INVALID/tags/list")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", resp.StatusCode)
	}
}

func TestManifestDeleteByTag(t *testing.T) {
	srv, _ := newTestServer(t, "", "")
	defer srv.Close()

	manifest := []byte(`{"schemaVersion":2}`)

	// Push by tag.
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/v2/todel/manifests/t1", bytes.NewReader(manifest))
	req.Header.Set("Content-Type", "application/json")
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	// Delete by tag.
	req, _ = http.NewRequest(http.MethodDelete, srv.URL+"/v2/todel/manifests/t1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status: got %d, want 202", resp.StatusCode)
	}

	// GET by tag now fails.
	resp2, _ := http.Get(srv.URL + "/v2/todel/manifests/t1")
	if resp2.StatusCode != http.StatusNotFound {
		t.Fatalf("status after delete: got %d, want 404", resp2.StatusCode)
	}
	resp2.Body.Close()
}

// smoke-test: full image push dance — blob + config + manifest — and pull back.
func TestFullImagePushPull(t *testing.T) {
	srv, _ := newTestServer(t, "", "")
	defer srv.Close()
	client := &http.Client{Timeout: 5 * time.Second}

	layer := []byte("layer-bytes")
	layerDigest := digestOf(layer)
	config := []byte(`{"architecture":"amd64","os":"linux"}`)
	configDigest := digestOf(config)

	for _, blob := range []struct {
		bytes  []byte
		digest string
	}{{layer, layerDigest}, {config, configDigest}} {
		url := fmt.Sprintf("%s/v2/myapp/blobs/uploads/?digest=%s", srv.URL, blob.digest)
		req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(blob.bytes))
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("blob push: got %d, want 201", resp.StatusCode)
		}
	}

	manifest := fmt.Sprintf(`{
		"schemaVersion": 2,
		"mediaType": "application/vnd.oci.image.manifest.v1+json",
		"config": {"mediaType":"application/vnd.oci.image.config.v1+json","digest":%q,"size":%d},
		"layers": [{"mediaType":"application/vnd.oci.image.layer.v1.tar","digest":%q,"size":%d}]
	}`, configDigest, len(config), layerDigest, len(layer))

	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/v2/myapp/manifests/1.0", strings.NewReader(manifest))
	req.Header.Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("manifest push: got %d, want 201", resp.StatusCode)
	}

	// Pull manifest back.
	resp, err = client.Get(srv.URL + "/v2/myapp/manifests/1.0")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("manifest pull: got %d, want 200", resp.StatusCode)
	}
	got, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(got), configDigest) {
		t.Fatalf("pulled manifest missing config digest")
	}
}
