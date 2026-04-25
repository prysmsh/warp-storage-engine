//go:build vector

package proxy

import (
	"context"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"

	"github.com/prysmsh/warp-storage-engine/internal/storage"
	"github.com/prysmsh/warp-storage-engine/pkg/vector"
)

// InitVectorEngine creates the vector engine and registers HTTP routes.
// Called from setupRoutes when VECTOR_ENABLED=true.
func InitVectorEngine(router *mux.Router, backend storage.Backend, cfg vector.VectorConfig) *vector.Engine {
	engine, err := vector.NewEngine(backend, cfg)
	if err != nil {
		logrus.WithError(err).Error("Failed to create vector engine")
		return nil
	}

	// Load existing collections from storage
	if err := engine.LoadCollectionsFromBackend(context.Background()); err != nil {
		logrus.WithError(err).Warn("Failed to load existing vector collections")
	}

	RegisterVectorRoutes(router, engine)
	return engine
}
