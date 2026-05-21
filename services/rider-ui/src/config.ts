// Build-time environment variable for the Dispatch Service URL.
// Set VITE_DISPATCH_URL in .env or docker-compose environment.
export const DISPATCH_URL: string =
  import.meta.env.VITE_DISPATCH_URL ?? 'http://localhost:8080'

// Default map centre — central London.
export const DEFAULT_CENTER: [number, number] = [51.5074, -0.1278]
export const DEFAULT_ZOOM = 13

// Hardcoded rider ID for Phase 1 (Requirement 6.3).
export const RIDER_ID = 'rider-001'
