# Foundation bugfix port — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port ~14 bugfix commits from `foundation-storage-engine` into `warp-storage-engine` at code-line granularity (not `git cherry-pick`), without disturbing warp-specific features (multitenancy, Auth0, admin/tenant handlers, chunk-decoder variants) or pulling foundation's observability/OTEL code.

**Architecture:** The two repos share no git history but the same module path (`github.com/einyx/foundation-storage-engine`). Port is organized **by file**, not by commit — because the same warp file often needs changes from multiple foundation fix commits. Each task: read foundation's patch(es) for the target file, locate semantically equivalent code in warp, apply via `Edit`, build the package, run its tests. No commits are created by the assistant.

**Tech Stack:** Go 1.24, stdlib, `aws-sdk-go-v2`, gorilla/mux, project-internal packages.

**Foundation source root (read-only):** `~/code/foundation/foundation-compose/foundation-storage-engine`
**Warp target root:** `/Users/alessio/code/prysm/warp-storage-engine`

**Fix commits to port, by target file:**

| Warp file | Foundation commits | Notes |
|---|---|---|
| `internal/auth/provider.go` | `40ad355`, `21cc59b`, `84670a4`, `e321c5e`, `3cdc27d` | 5 fixes, SigV4 + header parsing + loki |
| `internal/auth/vault_provider.go` | `04d1c51` (vault half) | soften empty vault token |
| `internal/security/path.go` | `04d1c51` (path half) | don't false-block system-path segments |
| `internal/storage/filesystem.go` | `3de78ca`, `3e10ee3` (fs half), `8f0de7f`, `3cdc27d` (fs half) | directory markers, upload-ID validator, loki names |
| `internal/storage/s3.go` | `249edc9` (s3 half) | content-length headers |
| `pkg/s3/validation.go` | `408fc19` (validation half) | `maxObjectSize` → int64 |
| `pkg/s3/bucket_handler.go` | `1074f18` | `?location=` canonical LocationConstraint XML |
| `pkg/s3/bulk_delete.go` | `3e10ee3` (bulk_delete half) | directory markers in bulk delete |
| `pkg/s3/object_handler.go` | `2f51098` | raw-chunk writing on disk |
| `internal/proxy/auth_manager.go` | `31b44f0`, `249edc9` (auth half) | timestamp session validation, content-length |

**Explicitly NOT ported:**
- Foundation-only files: `pkg/s3/put_object_helpers.go` (doesn't exist in warp), `internal/storage/safe_chunk_decoder.go` (foundation-only chunk decoder; warp has its own variants).
- Observability/OTEL (out of scope per user).
- Claude-skill fixes (`d7a0179`, `882534f`) — internal to foundation's tooling.
- CI/workflow fixes (`54fca8a`, `7b5f7c5`, `ee9070c`, `42a8cbd`, `e94a356`, `85ba5f0`, `8a5af7a`) — evaluated but deferred; warp has its own release pipeline.

**Test-file strategy:** When a fix commit includes a test file change, port the test iff (a) the equivalent test file exists in warp and (b) the test exercises the fixed code path. Skip tests that reference foundation-only symbols.

**Verification commands reused throughout:**
- Full build: `cd /Users/alessio/code/prysm/warp-storage-engine && go build ./...`
- Package test: `cd /Users/alessio/code/prysm/warp-storage-engine && go test ./<package>/...`
- Patch fetch: `git -C ~/code/foundation/foundation-compose/foundation-storage-engine show <sha> -- <path>`

---

## Task 0: Baseline — capture starting build/test state

**Files:** none modified.

- [ ] **Step 1: Record baseline build state**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go build ./... 2>&1 | tee /tmp/warp-baseline-build.log`
Expected: record whether build currently passes/fails. If it fails, note the errors — we must not make it worse.

- [ ] **Step 2: Record baseline test state**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go test ./... 2>&1 | tee /tmp/warp-baseline-test.log || true`
Expected: record pass/fail counts per package. We will compare against this at the end to ensure no regression.

---

## Task 1: `internal/auth/provider.go` — SigV4 + header fixes

**Files:**
- Modify: `internal/auth/provider.go`
- Reference patches (foundation): `40ad355`, `21cc59b`, `84670a4`, `e321c5e`, `3cdc27d`

- [ ] **Step 1: Read foundation patches in chronological order**

```bash
cd ~/code/foundation/foundation-compose/foundation-storage-engine
git show 40ad355 -- internal/auth/provider.go
git show 21cc59b -- internal/auth/provider.go
git show 84670a4 -- internal/auth/provider.go
git show e321c5e -- internal/auth/provider.go
git show 3cdc27d -- internal/auth/provider.go
```

- [ ] **Step 2: Read current warp file to locate equivalent code**

Read: `/Users/alessio/code/prysm/warp-storage-engine/internal/auth/provider.go`
Look for: SigV4 signature computation, canonical query string encoding, Authorization header parser, signed-headers handling.

- [ ] **Step 3: Apply `40ad355` — use `RawQuery` for v4 signature validation**

Semantic change: when reconstructing the canonical request for signature verification, use `r.URL.RawQuery` instead of `r.URL.Query().Encode()` (the latter re-encodes and may alter the signed string).

Apply via `Edit` at the verification site in warp's provider.go.

- [ ] **Step 4: Apply `21cc59b` — AWS SigV4-compliant query string encoding**

Semantic change: introduce the SigV4 canonical encoding helper (rfc3986-style, with specific reserved-char handling per AWS spec). Foundation's patch adds an `encodeSigV4QueryString` (or similarly named) helper function.

Port the helper and call it from the signature-validation path.

- [ ] **Step 5: Apply `84670a4` — sort canonical query string per AWS spec**

Semantic change: after splitting `RawQuery` on `&`, sort keys (and stable-order within equal keys) per AWS SigV4 canonical-request rules before re-encoding.

Adjust the helper from Step 4 to sort.

- [ ] **Step 6: Apply `e321c5e` — accept `', '` and `','` separators in Authorization header**

Semantic change: when parsing the `Authorization: AWS4-HMAC-SHA256 …` header, split SignedHeaders/Signature components tolerating both `, ` (with space) and `,` (no space) separators. Some clients emit one, some the other.

Apply to the Authorization-parsing helper.

- [ ] **Step 7: Apply `3cdc27d` — loki upload IDs and spaces**

Semantic change (provider.go half): when handling object keys that contain spaces (Loki writes keys with spaces), URL-decode / re-encode them consistently with what SigV4 computed over. Port the exact substitution foundation does.

- [ ] **Step 8: Build the package**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go build ./internal/auth/...`
Expected: compiles.

- [ ] **Step 9: Run the package tests**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go test ./internal/auth/... -count=1`
Expected: pre-existing tests continue to pass. If a test from foundation's `provider_test.go` covers SigV4 canonical-query-string sorting and is absent from warp, port it (Step 10).

- [ ] **Step 10: Port relevant SigV4 test (if missing in warp)**

Reference: `git show 84670a4 -- internal/auth/provider_test.go` in foundation.
Read warp's `internal/auth/provider_test.go`. If warp lacks the new SigV4 canonical-query-sorting test, append it (omit any lines referencing foundation-only symbols).

- [ ] **Step 11: Re-run package tests**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go test ./internal/auth/... -count=1`
Expected: all pass.

---

## Task 2: `internal/auth/vault_provider.go` — soften empty vault token

**Files:**
- Modify: `internal/auth/vault_provider.go`
- Reference patch: `04d1c51` (vault half)

- [ ] **Step 1: Read the relevant patch hunk**

```bash
cd ~/code/foundation/foundation-compose/foundation-storage-engine
git show 04d1c51 -- internal/auth/vault_provider.go
```

- [ ] **Step 2: Read warp's vault_provider.go**

Read: `/Users/alessio/code/prysm/warp-storage-engine/internal/auth/vault_provider.go`
Note: warp has an additional `vault_multiuser_provider.go` — leave that file untouched; the fix is in the single-user `vault_provider.go`.

- [ ] **Step 3: Apply the fix**

Semantic change: where the code currently treats an empty Vault token as a hard error, change it to a warning/log-and-continue with the existing connection state (one-line condition softening). Foundation's patch is a 1-line behavioral change.

Apply via `Edit`.

- [ ] **Step 4: Build and test**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go build ./internal/auth/... && go test ./internal/auth/... -count=1`
Expected: passes.

---

## Task 3: `internal/security/path.go` — don't false-block system-path segments

**Files:**
- Modify: `internal/security/path.go`
- Potentially modify: `internal/security/path_test.go`
- Reference patch: `04d1c51` (path half)

- [ ] **Step 1: Read the patch**

```bash
cd ~/code/foundation/foundation-compose/foundation-storage-engine
git show 04d1c51 -- internal/security/path.go internal/security/path_test.go
```

- [ ] **Step 2: Read warp's path.go**

Read: `/Users/alessio/code/prysm/warp-storage-engine/internal/security/path.go`

- [ ] **Step 3: Apply the fix**

Semantic change: the current blocklist (or substring match) rejects any S3 key containing segments like `etc`, `proc`, `sys`, `var`, `bin` — which causes false positives for legitimate user content. Foundation narrows the check to actual path-traversal patterns (e.g. `../`, absolute paths starting with `/etc/`, `/proc/`, etc.), not mere substring presence. Port the narrower check.

Apply via `Edit`.

- [ ] **Step 4: Port relevant test cases**

If foundation's `path_test.go` diff adds cases that verify "key containing 'etc' but not path-traversal is allowed", append equivalent cases to warp's test file.

- [ ] **Step 5: Build and test**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go build ./internal/security/... && go test ./internal/security/... -count=1`
Expected: passes.

---

## Task 4: `internal/storage/filesystem.go` — directory markers + loki names + upload-ID validator

**Files:**
- Modify: `internal/storage/filesystem.go`
- Potentially modify: `internal/storage/filesystem_test.go`
- Reference patches (chronological): `3de78ca`, `3e10ee3` (fs portion), `8f0de7f`, `3cdc27d` (fs portion)

- [ ] **Step 1: Read all four patches' fs-scoped hunks**

```bash
cd ~/code/foundation/foundation-compose/foundation-storage-engine
git show 3de78ca -- internal/storage/filesystem.go internal/storage/filesystem_test.go
git show 3e10ee3 -- internal/storage/filesystem.go internal/storage/filesystem_test.go
git show 8f0de7f -- internal/storage/filesystem.go internal/storage/filesystem_test.go
git show 3cdc27d -- internal/storage/filesystem.go
```

- [ ] **Step 2: Read warp's filesystem.go**

Read: `/Users/alessio/code/prysm/warp-storage-engine/internal/storage/filesystem.go`

- [ ] **Step 3: Apply `3de78ca` — local filesystem directory markers**

Semantic change: when writing an object whose key ends with `/` (S3 directory marker), create the corresponding directory on disk rather than writing a zero-byte file. When listing, recognize these as CommonPrefixes. Port the additions from foundation.

- [ ] **Step 4: Apply `3e10ee3` — virtio/flush-restart safety for directory markers**

Semantic change: ensure the directory marker handling survives process restart and virtio-backed filesystems — use atomic rename or explicit `Sync()` after `Close()`, and re-discover directory markers on startup. Port.

- [ ] **Step 5: Apply `8f0de7f` — dedicated upload-ID validator in `secureUploadPath`**

Semantic change: current code calls `validateBucketName` on an upload ID, which is overly strict. Foundation adds a dedicated `validateUploadID` (hex/alphanumeric + dashes, bounded length). Port the new validator and swap the call in `secureUploadPath`.

- [ ] **Step 6: Apply `3cdc27d` — loki spaces in key names**

Semantic change (fs half): filesystem backend must accept spaces in object keys (Loki writes `<tenant> <stream>` keys). Where filesystem code currently escapes/rejects spaces, relax it.

- [ ] **Step 7: Port filesystem_test.go additions**

Foundation's tests cover: directory markers, upload-ID validator edge cases, spaces in names. Append the equivalent cases to warp's `filesystem_test.go` (skip any test referencing foundation-only helpers).

- [ ] **Step 8: Build and test**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go build ./internal/storage/... && go test ./internal/storage/... -count=1 -run 'Filesystem|DirMarker|UploadID'`
Expected: passes. If unrelated failures exist in `internal/storage/`, confirm they predate this task (cross-check `/tmp/warp-baseline-test.log`).

---

## Task 5: `internal/storage/s3.go` — content-length headers

**Files:**
- Modify: `internal/storage/s3.go`
- Reference patch: `249edc9` (s3 half only; skip its `safe_chunk_decoder.go` part — foundation-only file)

- [ ] **Step 1: Read the s3-scoped hunk**

```bash
cd ~/code/foundation/foundation-compose/foundation-storage-engine
git show 249edc9 -- internal/storage/s3.go
```

- [ ] **Step 2: Read warp's s3.go**

Read: `/Users/alessio/code/prysm/warp-storage-engine/internal/storage/s3.go`

- [ ] **Step 3: Apply the fix**

Semantic change: when responding to HEAD/GET, ensure `Content-Length` is set from the object's actual size metadata, not from any transient chunk-transfer value. Foundation's patch adds ~26 lines around the header-setting block. Port only the lines that touch response-header writing; do **not** port any lines that reference `SafeChunkDecoder` (foundation-only symbol).

- [ ] **Step 4: Build and test**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go build ./internal/storage/... && go test ./internal/storage/... -count=1`
Expected: passes.

---

## Task 6: `pkg/s3/validation.go` — `maxObjectSize` as int64

**Files:**
- Modify: `pkg/s3/validation.go`
- Reference patch: `408fc19` (validation.go half only; skip `put_object_helpers.go` — foundation-only file)

- [ ] **Step 1: Read the patch**

```bash
cd ~/code/foundation/foundation-compose/foundation-storage-engine
git show 408fc19 -- pkg/s3/validation.go
```

- [ ] **Step 2: Read warp's validation.go**

Read: `/Users/alessio/code/prysm/warp-storage-engine/pkg/s3/validation.go`

- [ ] **Step 3: Apply the fix**

Semantic change: change `maxObjectSize` constant/variable declaration from untyped or `int` to explicit `int64`, so 32-bit builds don't overflow when comparing against `Content-Length` values >2 GiB.

- [ ] **Step 4: Check call sites for type coherence**

Grep: `grep -rn 'maxObjectSize' /Users/alessio/code/prysm/warp-storage-engine/pkg/s3/`
Ensure no caller does an implicit `int`/`int64` comparison that would break with the type change.

- [ ] **Step 5: Build and test**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go build ./pkg/s3/... && go test ./pkg/s3/... -count=1 -run 'Validation'`
Expected: passes.

---

## Task 7: `pkg/s3/bucket_handler.go` — `?location=` canonical XML

**Files:**
- Modify: `pkg/s3/bucket_handler.go`
- Potentially modify: `pkg/s3/handler_coverage_test.go`
- Reference patch: `1074f18`

- [ ] **Step 1: Read the patch**

```bash
cd ~/code/foundation/foundation-compose/foundation-storage-engine
git show 1074f18 -- pkg/s3/bucket_handler.go pkg/s3/handler_coverage_test.go
```

- [ ] **Step 2: Read warp's bucket_handler.go**

Read: `/Users/alessio/code/prysm/warp-storage-engine/pkg/s3/bucket_handler.go`

- [ ] **Step 3: Apply the fix**

Semantic change: when handling `GET /bucket?location=`, return the canonical AWS response:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>
```
with the correct `Content-Type: application/xml` and omitting the body only when the region is actually `us-east-1` is incorrect — always emit the XML. Port the handler branch verbatim (it's a ~34-line addition).

- [ ] **Step 4: Port the coverage test**

Append the new `Test…Location…` case(s) from foundation's `handler_coverage_test.go` to warp's version if warp doesn't already have them.

- [ ] **Step 5: Build and test**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go build ./pkg/s3/... && go test ./pkg/s3/... -count=1 -run 'Location|Bucket'`
Expected: passes.

---

## Task 8: `pkg/s3/bulk_delete.go` — directory markers in bulk delete

**Files:**
- Modify: `pkg/s3/bulk_delete.go`
- Potentially modify: `pkg/s3/bulk_delete_test.go`
- Reference patch: `3e10ee3` (bulk_delete portion)

- [ ] **Step 1: Read the bulk_delete-scoped hunk**

```bash
cd ~/code/foundation/foundation-compose/foundation-storage-engine
git show 3e10ee3 -- pkg/s3/bulk_delete.go pkg/s3/bulk_delete_test.go
```

- [ ] **Step 2: Read warp's bulk_delete.go**

Read: `/Users/alessio/code/prysm/warp-storage-engine/pkg/s3/bulk_delete.go`

- [ ] **Step 3: Apply the fix**

Semantic change: when bulk-deleting, if a key ends with `/`, treat it as a directory marker and remove the directory (or its marker object) rather than failing or no-op'ing. ~11-line addition in foundation.

- [ ] **Step 4: Port the test**

Append foundation's new `TestBulkDelete…DirectoryMarker…` cases.

- [ ] **Step 5: Build and test**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go build ./pkg/s3/... && go test ./pkg/s3/... -count=1 -run 'BulkDelete'`
Expected: passes.

---

## Task 9: `pkg/s3/object_handler.go` — raw chunk writing

**Files:**
- Modify: `pkg/s3/object_handler.go`
- Reference patch: `2f51098`

- [ ] **Step 1: Read the patch**

```bash
cd ~/code/foundation/foundation-compose/foundation-storage-engine
git show 2f51098 -- pkg/s3/object_handler.go
```

- [ ] **Step 2: Read warp's object_handler.go**

Read: `/Users/alessio/code/prysm/warp-storage-engine/pkg/s3/object_handler.go`

Note: warp has chunk-decoder variants (`smart_chunk_decoder.go`, `fast_chunk_reader.go`, `aws_chunk_decoder_v2.go`) that foundation doesn't. The fix must be applied **only** if warp's object_handler still routes through the same code path foundation's fix targets. If warp has already replaced that code path with a variant decoder, **skip this fix** and document in the task log.

- [ ] **Step 3: Apply the fix (if applicable)**

Semantic change: foundation's fix removes 19 lines and adds 6 — it replaces a custom chunk-writing loop with a simpler direct write. In warp, if that custom loop still exists in `object_handler.go`, apply the simplification; if the loop has been moved into `fast_chunk_reader.go` / etc., leave as-is and note that the fix is subsumed.

- [ ] **Step 4: Build and test**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go build ./pkg/s3/... && go test ./pkg/s3/... -count=1`
Expected: passes.

---

## Task 10: `internal/proxy/auth_manager.go` — timestamp validation + content-length

**Files:**
- Modify: `internal/proxy/auth_manager.go`
- Reference patches: `31b44f0`, `249edc9` (auth_manager half)

- [ ] **Step 1: Read both patches**

```bash
cd ~/code/foundation/foundation-compose/foundation-storage-engine
git show 31b44f0 -- internal/proxy/auth_manager.go
git show 249edc9 -- internal/proxy/auth_manager.go
```

- [ ] **Step 2: Read warp's auth_manager.go**

Read: `/Users/alessio/code/prysm/warp-storage-engine/internal/proxy/auth_manager.go`

Note: warp's `auth_manager.go` differs significantly from foundation's (multitenancy, Auth0). The fixes target (a) a session-timestamp-drift window and (b) content-length header relay. Locate the equivalent code paths; if they're still present in warp, apply.

- [ ] **Step 3: Apply `31b44f0` — timestamp session validation**

Semantic change: expand the allowed clock-skew window when validating session timestamps (foundation switches from an exact match to a bounded tolerance, ~14 lines). Port the widened comparison.

- [ ] **Step 4: Apply `249edc9` — content-length header relay**

Semantic change: when proxying requests, ensure `Content-Length` is preserved on the downstream request (not dropped during header sanitization). ~9-line addition.

- [ ] **Step 5: Build and test**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go build ./internal/proxy/... && go test ./internal/proxy/... -count=1 -run 'AuthManager|Session'`
Expected: passes. Warp-specific proxy tests (Auth0, admin, tenant) must continue to pass.

---

## Task 11: CI / Makefile / Dockerfile judgement pass

**Files:**
- Evaluate (modify only where equivalent file exists and semantic maps): `.gitignore`, `Dockerfile`, `Makefile`, `.github/workflows/docker.yml` (or nearest equivalent)
- Reference patches: `8a5af7a` (gitignore), `249edc9` (Dockerfile/Makefile/docker.yml halves)

- [ ] **Step 1: Read all three candidate patches**

```bash
cd ~/code/foundation/foundation-compose/foundation-storage-engine
git show 8a5af7a
git show 249edc9 -- Dockerfile Makefile .github/workflows/docker.yml
```

- [ ] **Step 2: `.gitignore` — add `CHANGELOG.md`**

Read warp's `.gitignore`. If `CHANGELOG.md` (or `/CHANGELOG.md`) is not already ignored, append it.

- [ ] **Step 3: Dockerfile telemetry info**

Read warp's `Dockerfile`. Foundation's `249edc9` adds build-arg labels for version/commit telemetry. If warp's Dockerfile has an equivalent `LABEL` / `ARG` pattern and is missing these, port them; otherwise skip (warp may have its own scheme).

- [ ] **Step 4: Makefile telemetry info**

Same reasoning — port only if warp's Makefile has the same `build:` target structure foundation's patch modifies.

- [ ] **Step 5: docker.yml workflow**

Inspect warp's `.github/workflows/docker.yml`. Port the telemetry `build-arg` lines if the job structure is similar enough to receive them without breaking warp-specific steps.

- [ ] **Step 6: No build/test run**

CI/infra changes don't affect `go build`/`go test`. Nothing to verify locally beyond YAML syntax (`yamllint` if available, else visual).

---

## Task 12: Full-tree build + test + regression check

**Files:** none modified — verification only.

- [ ] **Step 1: Full build**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go build ./... 2>&1 | tee /tmp/warp-final-build.log`
Expected: no errors. Diff against `/tmp/warp-baseline-build.log` — any new errors are regressions to fix.

- [ ] **Step 2: Full test**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go test ./... -count=1 2>&1 | tee /tmp/warp-final-test.log`
Expected: no new failures vs `/tmp/warp-baseline-test.log`. Pre-existing failures that don't touch the ported files are acceptable; new failures are regressions.

- [ ] **Step 3: `go vet` sanity**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go vet ./...`
Expected: no new diagnostics.

- [ ] **Step 4: `go mod tidy` check**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go mod tidy && git -C /Users/alessio/code/prysm/warp-storage-engine diff go.mod go.sum`
Expected: no diff (we added no new imports). If `go mod tidy` does produce a diff, investigate whether a ported file pulls in a new package — likely unintended, revert.

- [ ] **Step 5: Summarize changes for user**

Produce a final report listing:
- Files modified (from `git -C /Users/alessio/code/prysm/warp-storage-engine status`).
- For each fix commit SHA from the list above: applied / partially-applied / skipped (with reason).
- Any test additions.
- Remaining baseline failures that were not caused by this work.

**No git commit is created.** The user reviews `git diff` and commits themselves.
