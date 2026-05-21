// Package handler contains HTTP and WebSocket handlers for the Gateway Service.
package handler

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/gorilla/websocket"

	"github.com/realtime-tracking/gateway/session"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	// Allow all origins in Phase 1 (local dev). Phase 2: restrict to known origins.
	CheckOrigin: func(r *http.Request) bool { return true },
}

// WebSocketHandler handles GET /ws?rider_id=<string>.
// Upgrades the HTTP connection to WebSocket, registers the rider's session,
// and runs a read loop for heartbeat/ping-pong until the client disconnects.
type WebSocketHandler struct {
	Registry *session.Registry
}

// ServeHTTP implements http.Handler for GET /ws.
func (h *WebSocketHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	riderID := r.URL.Query().Get("rider_id")
	if riderID == "" {
		http.Error(w, `{"error":"rider_id query parameter is required"}`, http.StatusBadRequest)
		return
	}

	// Upgrade HTTP → WebSocket.
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("websocket upgrade failed", "rider_id", riderID, "error", err)
		return
	}

	// Register the session.
	h.Registry.Register(riderID, conn)
	slog.Info("websocket connected", "rider_id", riderID)

	// Configure ping-pong heartbeat.
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	// Read loop — keeps the connection alive and handles client-initiated close.
	// The Gateway only pushes to clients; it doesn't process incoming messages.
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err,
				websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				slog.Warn("websocket read error", "rider_id", riderID, "error", err)
			}
			break
		}
	}

	// Unregister on disconnect.
	h.Registry.Unregister(riderID)
	conn.Close()
	slog.Info("websocket disconnected", "rider_id", riderID)
}

// HealthHandler handles GET /health.
func HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"}) //nolint:errcheck
}
