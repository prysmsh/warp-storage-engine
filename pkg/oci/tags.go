package oci

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
)

// tagListResponse matches the OCI spec format for GET /v2/<name>/tags/list.
type tagListResponse struct {
	Name string   `json:"name"`
	Tags []string `json:"tags"`
}

// handleListTags returns all tags for a repository, optionally paginated via
// `n` (limit) and `last` (cursor). We do a prefix-scan on tags/<repo>/ and
// strip the prefix to recover tag names.
func (h *Handler) handleListTags(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]

	if !validRepoName(name) {
		writeError(w, http.StatusBadRequest, ErrCodeNameInvalid, "invalid repository name")
		return
	}

	n := parsePositiveInt(r.URL.Query().Get("n"), 0)
	after := r.URL.Query().Get("last")

	prefix := tagsPrefix(name)
	result, err := h.backend.ListObjects(r.Context(), h.cfg.Bucket, prefix, "", 0)
	if err != nil {
		writeError(w, http.StatusNotFound, ErrCodeNameUnknown, "repository not found")
		return
	}

	tags := make([]string, 0, len(result.Contents))
	for _, obj := range result.Contents {
		tag := strings.TrimPrefix(obj.Key, prefix)
		if tag == "" {
			continue
		}
		if after != "" && tag <= after {
			continue
		}
		tags = append(tags, tag)
	}
	sort.Strings(tags)

	if n > 0 && len(tags) > n {
		tags = tags[:n]
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(tagListResponse{Name: name, Tags: tags})
}

func parsePositiveInt(s string, fallback int) int {
	if s == "" {
		return fallback
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 {
		return fallback
	}
	return n
}
