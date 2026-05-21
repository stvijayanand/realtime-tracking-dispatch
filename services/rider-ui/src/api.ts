import { DISPATCH_URL, RIDER_ID } from './config'

export interface RequestRideResponse {
  tripId: string
}

export interface RequestRideError {
  message: string
}

/**
 * Sends a POST /request-ride to the Dispatch Service.
 * Returns the trip_id on success, or throws with a human-readable message on failure.
 */
export async function requestRide(
  pickupLat: number,
  pickupLng: number,
): Promise<RequestRideResponse> {
  const response = await fetch(`${DISPATCH_URL}/request-ride`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      riderId: RIDER_ID,
      pickupLocation: { latitude: pickupLat, longitude: pickupLng },
    }),
  })

  if (!response.ok) {
    let errorMessage = `Request failed with status ${response.status}`
    try {
      const body = await response.json()
      if (body.errors && Array.isArray(body.errors)) {
        errorMessage = body.errors.map((e: { field: string; message: string }) =>
          `${e.field}: ${e.message}`
        ).join(', ')
      } else if (body.error) {
        errorMessage = body.error
      }
    } catch {
      // ignore JSON parse errors — use the status-based message
    }
    throw new Error(errorMessage)
  }

  const data = await response.json()
  return { tripId: data.tripId ?? data.trip_id }
}
