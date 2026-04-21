package oci

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prysmsh/warp-storage-engine/internal/config"
)

func newBearerServer(t *testing.T, user, pass, secret string) *httptest.Server {
	t.Helper()
	be := newMemoryBackend()
	h, err := NewHandler(be, config.OCIConfig{
		Bucket:   "oci",
		Username: user,
		Password: pass,
		Bearer: config.OCIBearerConfig{
			Enabled:  true,
			Realm:    "",
			Service:  "warp-oci",
			Secret:   secret,
			TokenTTL: 1 * time.Hour,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return httptest.NewServer(h.Router())
}

func TestBearerChallengeAndToken(t *testing.T) {
	srv := newBearerServer(t, "pushuser", "pushpass", "super-secret")
	defer srv.Close()

	// /v2/ without auth → 401 with Bearer challenge.
	resp, err := http.Get(srv.URL + "/v2/")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status: got %d, want 401", resp.StatusCode)
	}
	chall := resp.Header.Get("WWW-Authenticate")
	if !strings.HasPrefix(chall, "Bearer ") {
		t.Fatalf("expected Bearer challenge, got %q", chall)
	}
	resp.Body.Close()

	// Exchange Basic creds for a bearer token.
	req, _ := http.NewRequest(http.MethodGet,
		srv.URL+"/auth/token?service=warp-oci&scope=repository:myrepo:pull,push", nil)
	req.SetBasicAuth("pushuser", "pushpass")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("token fetch: got %d, want 200. body: %s", resp.StatusCode, body)
	}
	var tok struct {
		Token     string `json:"token"`
		ExpiresIn int    `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tok); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if tok.Token == "" || !strings.Contains(tok.Token, ".") {
		t.Fatalf("invalid JWT: %q", tok.Token)
	}

	// Use the bearer on a real request.
	req, _ = http.NewRequest(http.MethodGet, srv.URL+"/v2/", nil)
	req.Header.Set("Authorization", "Bearer "+tok.Token)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status with bearer: got %d, want 200", resp.StatusCode)
	}
}

func TestBearerRejectsBadCredentials(t *testing.T) {
	srv := newBearerServer(t, "u", "p", "s")
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/auth/token", nil)
	req.SetBasicAuth("u", "wrong")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status: got %d, want 401", resp.StatusCode)
	}
}

func TestBearerRejectsTamperedToken(t *testing.T) {
	srv := newBearerServer(t, "", "", "secret")
	defer srv.Close()

	// Fetch a legit token (no basic creds since username is empty).
	resp, _ := http.Get(srv.URL + "/auth/token?scope=repository:r:pull")
	var tok struct {
		Token string `json:"token"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&tok)
	resp.Body.Close()

	// Flip one character of the signature.
	parts := strings.Split(tok.Token, ".")
	if len(parts) != 3 {
		t.Fatalf("want 3 parts, got %d", len(parts))
	}
	bad := parts[0] + "." + parts[1] + "." + flipOneChar(parts[2])

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v2/", nil)
	req.Header.Set("Authorization", "Bearer "+bad)
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusUnauthorized {
		t.Fatalf("tampered token accepted: %d", resp2.StatusCode)
	}
}

func flipOneChar(s string) string {
	if s == "" {
		return s
	}
	b := []byte(s)
	if b[0] == 'A' {
		b[0] = 'B'
	} else {
		b[0] = 'A'
	}
	return string(b)
}

func TestCatalog(t *testing.T) {
	srv, _ := newTestServer(t, "", "")
	defer srv.Close()

	// Push one manifest into two different repos.
	m := []byte(`{"schemaVersion":2}`)
	for _, repo := range []string{"alpha", "beta/sub"} {
		req, _ := http.NewRequest(http.MethodPut,
			srv.URL+"/v2/"+repo+"/manifests/v1", bytes.NewReader(m))
		req.Header.Set("Content-Type", "application/json")
		r, _ := http.DefaultClient.Do(req)
		r.Body.Close()
	}

	resp, err := http.Get(srv.URL + "/v2/_catalog")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d", resp.StatusCode)
	}
	var cat catalogResponse
	if err := json.NewDecoder(resp.Body).Decode(&cat); err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"alpha": true, "beta/sub": true}
	if len(cat.Repositories) != 2 {
		t.Fatalf("got %v", cat.Repositories)
	}
	for _, r := range cat.Repositories {
		if !want[r] {
			t.Fatalf("unexpected repo %q", r)
		}
	}
}

func TestCrossRepoBlobMount(t *testing.T) {
	srv, be := newTestServer(t, "", "")
	defer srv.Close()

	data := []byte("cross-repo bytes")
	digest := digestOf(data)

	// Push to first repo.
	req, _ := http.NewRequest(http.MethodPost,
		srv.URL+"/v2/first/blobs/uploads/?digest="+digest, bytes.NewReader(data))
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	// Mount into second repo — should return 201 without a body.
	mountURL := fmt.Sprintf("%s/v2/second/blobs/uploads/?mount=%s&from=first", srv.URL, digest)
	req, _ = http.NewRequest(http.MethodPost, mountURL, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("mount status: got %d, want 201. body: %s", resp.StatusCode, body)
	}
	if got := resp.Header.Get("Docker-Content-Digest"); got != digest {
		t.Fatalf("digest header: got %q, want %q", got, digest)
	}

	// Pull from the second repo — should still succeed (content is shared).
	resp2, err := http.Get(srv.URL + "/v2/second/blobs/" + digest)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("pull status: got %d", resp2.StatusCode)
	}
	body, _ := io.ReadAll(resp2.Body)
	if !bytes.Equal(body, data) {
		t.Fatalf("mismatched bytes")
	}

	// Backend still has exactly one stored copy of the blob.
	be.mu.Lock()
	defer be.mu.Unlock()
	count := 0
	for k := range be.objects {
		if strings.HasPrefix(k, "oci/blobs/") {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected 1 blob copy, got %d", count)
	}
}

func TestMountOfUnknownDigestFallsThrough(t *testing.T) {
	srv, _ := newTestServer(t, "", "")
	defer srv.Close()

	// Digest that doesn't exist in any repo — mount should fall through to
	// a normal upload-session start (202 + Location).
	unknown := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	req, _ := http.NewRequest(http.MethodPost,
		srv.URL+"/v2/r/blobs/uploads/?mount="+unknown+"&from=other", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status: got %d, want 202 (fall-through)", resp.StatusCode)
	}
}

func TestReferrersApi(t *testing.T) {
	srv, _ := newTestServer(t, "", "")
	defer srv.Close()

	// First: push a subject manifest.
	subject := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json"}`)
	subjectDigest := digestOf(subject)
	req, _ := http.NewRequest(http.MethodPut,
		srv.URL+"/v2/app/manifests/"+subjectDigest, bytes.NewReader(subject))
	req.Header.Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	// Push a signature manifest that references the subject. In cosign's
	// shape this would be an artifactType like "application/vnd.dev.cosign.artifact.sig.v1+json".
	sig := fmt.Sprintf(`{
		"schemaVersion": 2,
		"mediaType": "application/vnd.oci.image.manifest.v1+json",
		"artifactType": "application/vnd.dev.cosign.artifact.sig.v1+json",
		"subject": {
			"mediaType": "application/vnd.oci.image.manifest.v1+json",
			"digest": %q,
			"size": %d
		},
		"annotations": {"signed.by": "alice"}
	}`, subjectDigest, len(subject))

	sigBytes := []byte(sig)
	sigDigest := digestOf(sigBytes)
	req, _ = http.NewRequest(http.MethodPut,
		srv.URL+"/v2/app/manifests/"+sigDigest, bytes.NewReader(sigBytes))
	req.Header.Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("sig push: got %d", resp.StatusCode)
	}

	// Query referrers for the subject.
	resp2, err := http.Get(srv.URL + "/v2/app/referrers/" + subjectDigest)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("referrers status: got %d", resp2.StatusCode)
	}
	var idx referrersIndex
	if err := json.NewDecoder(resp2.Body).Decode(&idx); err != nil {
		t.Fatal(err)
	}
	if idx.SchemaVersion != 2 {
		t.Fatalf("schemaVersion: got %d", idx.SchemaVersion)
	}
	if len(idx.Manifests) != 1 {
		t.Fatalf("manifests: got %d, want 1", len(idx.Manifests))
	}
	got := idx.Manifests[0]
	if got.Digest != sigDigest {
		t.Fatalf("digest: got %q, want %q", got.Digest, sigDigest)
	}
	if got.ArtifactType != "application/vnd.dev.cosign.artifact.sig.v1+json" {
		t.Fatalf("artifactType: got %q", got.ArtifactType)
	}
	if got.Annotations["signed.by"] != "alice" {
		t.Fatalf("annotations: got %v", got.Annotations)
	}

	// Filter by artifactType.
	resp3, err := http.Get(srv.URL + "/v2/app/referrers/" + subjectDigest +
		"?artifactType=application/vnd.dev.cosign.artifact.sig.v1+json")
	if err != nil {
		t.Fatal(err)
	}
	defer resp3.Body.Close()
	if got := resp3.Header.Get("OCI-Filters-Applied"); got != "artifactType" {
		t.Fatalf("OCI-Filters-Applied header: got %q", got)
	}

	// Filter by a non-matching artifactType yields empty list.
	resp4, err := http.Get(srv.URL + "/v2/app/referrers/" + subjectDigest +
		"?artifactType=nonexistent")
	if err != nil {
		t.Fatal(err)
	}
	defer resp4.Body.Close()
	var empty referrersIndex
	_ = json.NewDecoder(resp4.Body).Decode(&empty)
	if len(empty.Manifests) != 0 {
		t.Fatalf("expected 0 filtered manifests, got %d", len(empty.Manifests))
	}
}

func TestManifestWithSubjectSetsHeader(t *testing.T) {
	srv, _ := newTestServer(t, "", "")
	defer srv.Close()

	subjectDigest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	body := fmt.Sprintf(`{"schemaVersion":2,"subject":{"mediaType":"x","digest":%q,"size":0}}`, subjectDigest)

	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/v2/x/manifests/t",
		bytes.NewReader([]byte(body)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("OCI-Subject"); got != subjectDigest {
		t.Fatalf("OCI-Subject: got %q, want %q", got, subjectDigest)
	}
}

func TestTokenEndpointDisabledWhenBearerOff(t *testing.T) {
	srv, _ := newTestServer(t, "", "")
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/auth/token")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status: got %d, want 404", resp.StatusCode)
	}
}
