package oci

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"
)

// catalogResponse is the spec-defined shape for GET /v2/_catalog.
type catalogResponse struct {
	Repositories []string `json:"repositories"`
}

// handleCatalog lists all repositories that have at least one stored
// manifest. We derive the repo set by scanning the manifests/ prefix — each
// object key is manifests/<repo>/<digest> so repo = everything between the
// first segment and the last.
func (h *Handler) handleCatalog(w http.ResponseWriter, r *http.Request) {
	n := parsePositiveInt(r.URL.Query().Get("n"), 0)
	after := r.URL.Query().Get("last")

	result, err := h.backend.ListObjects(r.Context(), h.cfg.Bucket, "manifests/", "", 0)
	if err != nil {
		writeError(w, http.StatusInternalServerError, ErrCodeUnsupported, "failed to list manifests")
		return
	}

	repos := make(map[string]struct{}, len(result.Contents))
	for _, obj := range result.Contents {
		rest := strings.TrimPrefix(obj.Key, "manifests/")
		// rest = "<repo>/<digest>" — take everything before the last '/'.
		slash := strings.LastIndex(rest, "/")
		if slash <= 0 {
			continue
		}
		repos[rest[:slash]] = struct{}{}
	}

	names := make([]string, 0, len(repos))
	for name := range repos {
		if after != "" && name <= after {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)

	if n > 0 && len(names) > n {
		names = names[:n]
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(catalogResponse{Repositories: names})
}
