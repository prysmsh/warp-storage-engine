package oci

import (
	"fmt"
	"regexp"
	"strings"
)

// digestPattern validates OCI content-addressable identifiers.
// Format: <algorithm>:<hex-encoded-hash>. We only support sha256/sha512.
var digestPattern = regexp.MustCompile(`^(sha256|sha512):[a-f0-9]{32,128}$`)

// repoNamePattern validates OCI repository names per the distribution spec:
// lowercase alphanumerics plus '.', '_', '-', separated by '/'.
// Each component must start and end with alphanumeric.
// https://github.com/opencontainers/distribution-spec/blob/main/spec.md#pulling-manifests
var repoNamePattern = regexp.MustCompile(`^[a-z0-9]+([._-][a-z0-9]+)*(/[a-z0-9]+([._-][a-z0-9]+)*)*$`)

// tagPattern validates OCI tag names.
var tagPattern = regexp.MustCompile(`^[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}$`)

func validDigest(d string) bool {
	return digestPattern.MatchString(d)
}

func validRepoName(n string) bool {
	return len(n) > 0 && len(n) <= 255 && repoNamePattern.MatchString(n)
}

func validTagOrDigest(ref string) bool {
	return tagPattern.MatchString(ref) || validDigest(ref)
}

// Storage layout (under the configured OCI bucket):
//
//	blobs/<hex-digest>                  — raw blob bytes (key is the hex portion only)
//	manifests/<repo>/<hex-digest>       — manifest JSON stored by its own digest
//	tags/<repo>/<tag>                   — small text object containing the manifest digest
//	uploads/<uuid>                      — in-progress blob uploads (streaming to backend)
//
// Using only the hex portion of the digest in keys keeps paths short and
// S3-safe. The algorithm prefix is always sha256 (we reject other algorithms
// on write).

func blobKey(digest string) string {
	return "blobs/" + digestHex(digest)
}

func manifestKeyByDigest(repo, digest string) string {
	return fmt.Sprintf("manifests/%s/%s", repo, digestHex(digest))
}

func tagKey(repo, tag string) string {
	return fmt.Sprintf("tags/%s/%s", repo, tag)
}

func tagsPrefix(repo string) string {
	return fmt.Sprintf("tags/%s/", repo)
}

func uploadKey(uuid string) string {
	return "uploads/" + uuid
}

// digestHex returns the hex portion of a digest (the part after the colon).
// Callers should have validated the digest first.
func digestHex(digest string) string {
	i := strings.IndexByte(digest, ':')
	if i < 0 {
		return digest
	}
	return digest[i+1:]
}
