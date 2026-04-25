//go:build !vector

package proxy

import (
	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"

	"github.com/prysmsh/warp-storage-engine/internal/storage"
)

// InitVectorEngine is a no-op when built without the vector tag.
func InitVectorEngine(router *mux.Router, backend storage.Backend, cfg any) any {
	logrus.Debug("Vector engine not available (build without -tags vector)")
	return nil
}
