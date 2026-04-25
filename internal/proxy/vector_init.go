//go:build vector

package proxy

import (
	"context"
	"os"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"

	"github.com/prysmsh/warp-storage-engine/internal/storage"
	"github.com/prysmsh/warp-storage-engine/pkg/vector"
)

// InitVectorEngine creates the vector engine and registers HTTP routes.
func InitVectorEngine(router *mux.Router, backend storage.Backend, _ any) any {
	cfg := vector.VectorConfig{}

	if v := os.Getenv("VECTOR_HNSW_M"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.HNSWm = n
		}
	}
	if v := os.Getenv("VECTOR_HNSW_EF_CONSTRUCTION"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.HNSWefConstruct = n
		}
	}
	if v := os.Getenv("VECTOR_HNSW_EF_SEARCH"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.HNSWefSearch = n
		}
	}
	if v := os.Getenv("VECTOR_WAL_FLUSH_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.WALFlushInterval = d
		}
	}

	engine, err := vector.NewEngine(backend, cfg)
	if err != nil {
		logrus.WithError(err).Error("Failed to create vector engine")
		return nil
	}

	if err := engine.LoadCollectionsFromBackend(context.Background()); err != nil {
		logrus.WithError(err).Warn("Failed to load existing vector collections")
	}

	RegisterVectorRoutes(router, engine)
	logrus.Info("Vector engine initialized")
	return engine
}
