//go:build vector

package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"

	"github.com/prysmsh/warp-storage-engine/pkg/vector"
)

// VectorHandlers exposes the vector engine over a REST API.
type VectorHandlers struct {
	engine *vector.Engine
}

func NewVectorHandlers(engine *vector.Engine) *VectorHandlers {
	return &VectorHandlers{engine: engine}
}

// --- Collections ---

func (h *VectorHandlers) ListCollections(w http.ResponseWriter, r *http.Request) {
	cols, err := h.engine.ListCollections(r.Context())
	if err != nil {
		writeVectorError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"collections": cols})
}

func (h *VectorHandlers) CreateCollection(w http.ResponseWriter, r *http.Request) {
	var col vector.Collection
	if err := json.NewDecoder(r.Body).Decode(&col); err != nil {
		writeVectorError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if col.Name == "" {
		writeVectorError(w, http.StatusBadRequest, "name is required")
		return
	}
	if col.Dimensions <= 0 {
		writeVectorError(w, http.StatusBadRequest, "dimensions must be > 0")
		return
	}
	if col.Distance == "" {
		col.Distance = vector.Cosine
	}

	if err := h.engine.CreateCollection(r.Context(), col); err != nil {
		writeVectorError(w, http.StatusConflict, err.Error())
		return
	}

	logrus.WithField("collection", col.Name).Info("vector collection created")
	writeJSON(w, http.StatusCreated, map[string]any{"collection": col})
}

func (h *VectorHandlers) GetCollection(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	col, err := h.engine.GetCollection(r.Context(), name)
	if err != nil {
		writeVectorError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"collection": col})
}

func (h *VectorHandlers) DeleteCollection(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	if err := h.engine.DeleteCollection(r.Context(), name); err != nil {
		writeVectorError(w, http.StatusNotFound, err.Error())
		return
	}
	logrus.WithField("collection", name).Info("vector collection deleted")
	writeJSON(w, http.StatusOK, map[string]any{"message": fmt.Sprintf("collection %q deleted", name)})
}

// --- Points ---

func (h *VectorHandlers) InsertPoints(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]

	var req struct {
		Points []vector.Point `json:"points"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeVectorError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if len(req.Points) == 0 {
		writeVectorError(w, http.StatusBadRequest, "at least one point is required")
		return
	}

	if err := h.engine.Insert(r.Context(), name, req.Points); err != nil {
		writeVectorError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"inserted": len(req.Points),
	})
}

func (h *VectorHandlers) GetPoints(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]

	var req struct {
		IDs []uint64 `json:"ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeVectorError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	points, err := h.engine.Get(r.Context(), name, req.IDs)
	if err != nil {
		writeVectorError(w, http.StatusNotFound, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"points": points})
}

func (h *VectorHandlers) DeletePoints(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]

	var req struct {
		IDs []uint64 `json:"ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeVectorError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if err := h.engine.Delete(r.Context(), name, req.IDs); err != nil {
		writeVectorError(w, http.StatusNotFound, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"deleted": len(req.IDs)})
}

// --- Search ---

func (h *VectorHandlers) Search(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]

	var req vector.SearchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeVectorError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	req.Collection = name

	if len(req.Vector) == 0 {
		writeVectorError(w, http.StatusBadRequest, "vector is required")
		return
	}
	if req.TopK <= 0 {
		req.TopK = 10
	}

	resp, err := h.engine.Search(r.Context(), req)
	if err != nil {
		writeVectorError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

// writeVectorError writes a JSON error response for vector endpoints.
func writeVectorError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// RegisterVectorRoutes wires vector API endpoints into the router.
func RegisterVectorRoutes(router *mux.Router, engine *vector.Engine) {
	h := NewVectorHandlers(engine)

	v := router.PathPrefix("/api/v1/vectors").Subrouter()

	// Collections
	v.HandleFunc("/collections", h.ListCollections).Methods("GET")
	v.HandleFunc("/collections", h.CreateCollection).Methods("POST")
	v.HandleFunc("/collections/{name}", h.GetCollection).Methods("GET")
	v.HandleFunc("/collections/{name}", h.DeleteCollection).Methods("DELETE")

	// Points
	v.HandleFunc("/collections/{name}/points", h.InsertPoints).Methods("POST")
	v.HandleFunc("/collections/{name}/points/get", h.GetPoints).Methods("POST")
	v.HandleFunc("/collections/{name}/points/delete", h.DeletePoints).Methods("POST")

	// Search
	v.HandleFunc("/collections/{name}/search", h.Search).Methods("POST")

	logrus.Info("Vector API routes registered at /api/v1/vectors/")

	_ = strconv.Itoa(0) // ensure strconv import
}
