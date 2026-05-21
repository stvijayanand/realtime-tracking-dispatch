import React, { useState } from 'react'
import { MapContainer, TileLayer, Marker, Popup } from 'react-leaflet'
import { DEFAULT_CENTER, DEFAULT_ZOOM } from './config'
import { requestRide } from './api'
import './App.css'

type AppState =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; tripId: string }
  | { status: 'error'; message: string }

export default function App() {
  const [state, setState] = useState<AppState>({ status: 'idle' })

  const handleRequestRide = async () => {
    setState({ status: 'loading' })
    try {
      const result = await requestRide(DEFAULT_CENTER[0], DEFAULT_CENTER[1])
      setState({ status: 'success', tripId: result.tripId })
    } catch (err) {
      const message = err instanceof Error ? err.message : 'An unexpected error occurred'
      setState({ status: 'error', message })
    }
  }

  return (
    <div className="app">
      <header className="app-header">
        <h1>Rider UI</h1>
        <p className="app-subtitle">Real-Time Ride Dispatch</p>
      </header>

      <main className="app-main">
        <div className="map-container">
          <MapContainer
            center={DEFAULT_CENTER}
            zoom={DEFAULT_ZOOM}
            style={{ height: '100%', width: '100%' }}
          >
            <TileLayer
              attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
              url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
            />
            <Marker position={DEFAULT_CENTER}>
              <Popup>Pickup location</Popup>
            </Marker>
          </MapContainer>
        </div>

        <div className="controls">
          <button
            className="request-ride-btn"
            onClick={handleRequestRide}
            disabled={state.status === 'loading'}
            aria-busy={state.status === 'loading'}
          >
            {state.status === 'loading' ? 'Requesting…' : 'Request Ride'}
          </button>

          {state.status === 'success' && (
            <div className="status-message status-success" role="status" aria-live="polite">
              <strong>Ride requested!</strong>
              <br />
              Trip ID: <code>{state.tripId}</code>
            </div>
          )}

          {state.status === 'error' && (
            <div className="status-message status-error" role="alert" aria-live="assertive">
              <strong>Error:</strong> {state.message}
            </div>
          )}
        </div>
      </main>
    </div>
  )
}
