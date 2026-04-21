# Foundation bugfix port + OCI backend — Design

**Date:** 2026-04-21
**Author:** Alessio (via Claude)
**Status:** Draft — awaiting review

## Context

`warp-storage-engine` (at `github.com/prysmsh/warp-storage-engine`) is a fork of `foundation-storage-engine` (at `github.com/meshxdata/foundation-storage-engine`). Warp was imported via a squashed "Initial Commit" and has since added one feature commit (multitenancy: vault multi-user auth, tenant middleware, per-tenant storage). The two histories share no git ancestry.

Meanwhile foundation has accumulated ~26 `fix:` / `hotfix:` commits that warp is missing, plus an `internal/observability/` package wired into `server.go` and `main.go`.

## Goal

Port foundation's bugfixes into warp, without disturbing warp-specific additions (multitenancy, Auth0, admin/tenant handlers, web UI, chunk-decoder variants, `pkg/s3/client_profile`, `tcp_stub`). Then, as a **separate** follow-up project, design and build an OCI Distribution storage backend (Docker/Helm artifact registry).

This spec covers **Phase 1 only** — the bugfix port. Phase 2 (OCI backend) gets its own brainstorm once Phase 1 is landed.

## Non-goals

- **Porting observability / OTEL.** Explicitly out of scope per user direction. `internal/observability/` and `internal/middleware/otel.go` stay absent from warp.
- Changing the module path, binary name, or rebranding `foundation-storage-engine` → `warp-storage-engine` in code. Out of scope.
- Syncing foundation's CI workflows wholesale. Warp has its own CI; only the CI fixes that map meaningfully to warp's workflow files will be ported.
- Pulling foundation-only refactor/features that are not bugfixes: `auth/fallback_provider.go`, `storage/safe_chunk_decoder.go`, `pkg/s3/put_object_helpers.go`. These can be handled in a later task if desired.
- Committing the work. The user will stage/commit themselves. Nothing in Phase 1 creates git commits.

## Scope of changes (Phase 1)

### A. Port bugfix commits, code-line level

For each fix commit in foundation, read the patch, find the equivalent location in warp's (divergent) file, and apply the semantic change by hand. This is per-hunk, not per-commit — `git cherry-pick` is not usable because histories are unrelated and warp's files have already diverged (multitenancy in `vault_provider.go`, Auth0 in `proxy/server.go`, warp's own chunk-decoder variants, etc.).

Target commits, grouped by area:

**1. Security / path validation**
- `04d1c51` fix(storage): don't false-block S3 keys with system-path segments (path half)
- `8f0de7f` fix(FOU-2545): dedicated upload-ID validator in `secureUploadPath`
- Files: `internal/security/path.go`, `internal/security/path_test.go`

**2. Auth / vault**
- `04d1c51` fix: soften empty vault token (vault half)
- Files: `internal/auth/vault_provider.go`

**3. SigV4 signature validation**
- `40ad355` fix(auth): use `RawQuery` for AWS v4 signature validation
- `84670a4` fix: sort SigV4 canonical query string per AWS spec
- `21cc59b` fix(auth): SigV4-compliant query string encoding
- `e321c5e` fix(auth): accept both `', '` and `','` separators in Authorization header
- Files: `internal/middleware/auth.go`, `pkg/s3/*` signer code

**4. S3 protocol compatibility**
- `1074f18` fix(s3): `?location=` with canonical `LocationConstraint` XML
- `408fc19` fix(s3): `maxObjectSize` as `int64` for 32-bit builds
- `249edc9` fix content-length headers + build telemetry
- `3cdc27d` fix loki upload IDs and spaces in names
- Files: `pkg/s3/{bucket_handler,object_handler,multipart_handler,validation,utils}.go`

**5. Filesystem backend**
- `3de78ca` fix local filesystem directory markers
- `3e10ee3` fix virtio/filesystem directory markers + flush restart
- `2f51098` fix raw-chunk writing on disk
- Files: `internal/storage/filesystem.go`

**6. Session / timestamp validation**
- `31b44f0` fix timestamp session validation
- Files: `internal/middleware/auth.go` or `internal/proxy/auth_manager.go`

**7. Content-length / misc storage improvements**
- `760727d` Content-Length fixes and various improvements
- Files: `internal/storage/*`, `pkg/s3/*`

**8. Test fixes (apply only where warp's test shape matches)**
- `aee3103`, `fd39898`, `d37741c`

**9. CI/build (per-file judgement)**
- `8a5af7a` gitignore CHANGELOG.md → apply to warp's `.gitignore` if not already.
- `54fca8a`, `7b5f7c5`, `e94a356`, `85ba5f0`, `ee9070c`, `42a8cbd` — evaluate warp's existing workflows; apply the semantic change where the equivalent file exists and behavior maps.
- Claude-skill fixes (`d7a0179`, `882534f`) — skip, internal to foundation's skills.

### B. ~~Port observability / OTEL feature~~ — removed per user direction

Observability is out of scope. No changes to `internal/observability/`, `internal/middleware/otel.go`, or related wiring.

## Approach — step by step

1. **Enumerate patches.** `git -C <foundation> show <sha>` for each target commit, save patch contents for reference.
2. **Port by group (order: security → auth/vault → SigV4 → S3 protocol → filesystem → session → misc → tests → CI).** For each group:
   - Read foundation's diff.
   - Read the corresponding warp file.
   - Apply the semantic change via `Edit` (not `patch`), because context around hunks will differ.
   - `go build ./...` to catch breakage.
   - `go test ./<touched packages>` to catch regressions.
3. **Leave everything uncommitted.** User reviews `git status` / `git diff` and commits themselves.

## Risks and how we handle them

- **Merge-conflict-style divergence:** warp's file has multitenancy additions that shift line numbers; foundation's hunk targets a function that no longer exists by the same name in warp. → Apply by semantic intent, not line-number. If the function is gone, trace the equivalent call site in warp.
- **Breaking warp-specific features:** a bugfix may touch a file that also contains warp's multitenancy / Auth0 code. → After each group, run warp's test suite on the touched package *and* on any package that imports it.
- **Fix touches a file warp has partially rewritten:** e.g. `pkg/s3/object_handler.go` — warp and foundation both modified it for different reasons. → Inspect the fix diff in isolation; if the fix targets a code path warp still uses, apply; if the fix targets a code path warp has already replaced, note and skip.

## Acceptance criteria

- `go build ./...` passes in warp.
- `go test ./...` passes in warp (or any pre-existing failures are unchanged — will verify with a baseline run before starting).
- Warp-specific files are untouched except where a bugfix unambiguously belongs in them.
- No observability/OTEL code introduced.
- No git commits created by the assistant.

## Phase 2 preview — OCI Distribution backend

Out of scope for this spec. To be designed separately. Open questions to decide in that spec:
- Implement OCI Distribution spec from scratch, or embed `distribution/distribution` / `zot` internals?
- Does the OCI backend plug into the existing `storage.Backend` interface, or live alongside it?
- Auth model for `docker login` / `helm registry login` (bearer token vs. basic vs. existing SigV4).
- How artifacts (manifests, blobs, tags) map to the multitenancy model.
- Content addressing, chunked/resumable uploads, GC policy.
- KMS-encrypted blob storage for OCI layers.

No work on Phase 2 happens until Phase 1 is landed and this spec's Phase 2 section is expanded into its own design doc.
