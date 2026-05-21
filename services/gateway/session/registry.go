// Package session manages WebSocket connection sessions for the Gateway Service.
// Phase 1: in-memory registry keyed by rider_id.
// Phase 2: Redis Pub/Sub for multi-instance fan-out.
package session

import (
	"fmt"
	"sync"

	"github.com/gorilla/websocket"
)

// Registry is a thread-safe in-memory store of active WebSocket connections,
// keyed by rider_id. Phase 1 implementation — single instance only.
//
// Phase 2 replaces this with a Redis-backed registry using
// HSET gateway:sessions:{rider_id} instance_id connection_id.
type Registry struct {
	mu       sync.RWMutex
	sessions map[string]*websocket.Conn
}

// NewRegistry creates an empty Registry.
func NewRegistry() *Registry {
	return &Registry{
		sessions: make(map[string]*websocket.Conn),
	}
}

// Register stores a WebSocket connection for the given rider_id.
// If a connection already exists for this rider, it is replaced.
func (r *Registry) Register(riderID string, conn *websocket.Conn) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sessions[riderID] = conn
}

// Unregister removes the WebSocket connection for the given rider_id.
// No-op if the rider is not registered.
func (r *Registry) Unregister(riderID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.sessions, riderID)
}

// Send pushes a JSON payload to the connected rider's WebSocket connection.
// Returns an error if the rider is not connected (no-op, not a crash) or if
// the write fails.
func (r *Registry) Send(riderID string, payload []byte) error {
	r.mu.RLock()
	conn, ok := r.sessions[riderID]
	r.mu.RUnlock()

	if !ok {
		// Rider not connected — this is normal (they may have disconnected).
		return fmt.Errorf("rider %q not connected", riderID)
	}

	return conn.WriteMessage(websocket.TextMessage, payload)
}

// Count returns the number of active WebSocket connections.
// Used for the Prometheus active_connections gauge (Requirement 12.2).
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.sessions)
}
