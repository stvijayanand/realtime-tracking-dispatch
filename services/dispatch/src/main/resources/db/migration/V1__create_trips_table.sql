-- Trips table: source of truth for the Trip aggregate lifecycle.
-- Owned exclusively by the Dispatch Service.
-- Requirement 3.16: persists Trip state machine (REQUESTED, ASSIGNED, CANCELLED).

CREATE TABLE trips (
    trip_id      UUID         NOT NULL,
    rider_id     VARCHAR(128) NOT NULL,
    driver_id    VARCHAR(128),
    status       VARCHAR(20)  NOT NULL,
    pickup_lat   DOUBLE PRECISION,
    pickup_lng   DOUBLE PRECISION,
    requested_at TIMESTAMPTZ  NOT NULL,
    assigned_at  TIMESTAMPTZ,
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT pk_trips PRIMARY KEY (trip_id)
);

-- Index for filtering trips by status (e.g. finding all REQUESTED trips).
CREATE INDEX idx_trips_status ON trips (status);

-- Index for time-based queries (e.g. Saga State Monitor finding stuck trips).
CREATE INDEX idx_trips_updated_at ON trips (updated_at);
