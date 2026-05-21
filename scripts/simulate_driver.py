#!/usr/bin/env python3
"""
Driver Simulator — emits GPS pings to the Ingest Service at a configurable rate.

Usage:
    python simulate_driver.py \
        --driver-id driver-001 \
        --route-file scripts/sample_route.geojson \
        --rate 10 \
        --ingest-url http://localhost:8001

Requirements: 5.1–5.7
"""

import argparse
import json
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from typing import List, Tuple


def load_route(route_file: str) -> List[Tuple[float, float]]:
    """Load a GeoJSON LineString and return a list of (longitude, latitude) pairs."""
    with open(route_file, "r", encoding="utf-8") as f:
        geojson = json.load(f)

    # Support both a bare LineString and a FeatureCollection/Feature wrapper.
    if geojson.get("type") == "FeatureCollection":
        features = geojson.get("features", [])
        if not features:
            raise ValueError("FeatureCollection has no features")
        geometry = features[0].get("geometry", {})
    elif geojson.get("type") == "Feature":
        geometry = geojson.get("geometry", {})
    else:
        geometry = geojson

    if geometry.get("type") != "LineString":
        raise ValueError(f"Expected GeoJSON LineString, got: {geometry.get('type')}")

    coords = geometry.get("coordinates", [])
    if len(coords) < 2:
        raise ValueError(f"LineString must have at least 2 coordinate pairs, got {len(coords)}")

    # GeoJSON coordinates are [longitude, latitude].
    return [(c[0], c[1]) for c in coords]


def interpolate_position(
    coords: List[Tuple[float, float]], index: int
) -> Tuple[float, float]:
    """Return the coordinate at the given index, wrapping around at the end."""
    return coords[index % len(coords)]


def send_ping(
    ingest_url: str,
    driver_id: str,
    latitude: float,
    longitude: float,
    timestamp: str,
) -> None:
    """POST a GPS ping to the Ingest Service. Logs errors to stderr and continues."""
    payload = json.dumps(
        {
            "driver_id": driver_id,
            "latitude": latitude,
            "longitude": longitude,
            "timestamp": timestamp,
        }
    ).encode("utf-8")

    url = ingest_url.rstrip("/") + "/location"
    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            # Success — 202 Accepted.
            _ = resp.read()
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        print(
            f"[ERROR] HTTP {e.code} from ingest service: {body}",
            file=sys.stderr,
        )
    except urllib.error.URLError as e:
        print(
            f"[ERROR] Failed to reach ingest service at {url}: {e.reason}",
            file=sys.stderr,
        )
    except Exception as e:  # noqa: BLE001
        print(f"[ERROR] Unexpected error sending ping: {e}", file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Simulate a driver emitting GPS pings to the Ingest Service."
    )
    parser.add_argument(
        "--driver-id",
        required=True,
        help="Unique identifier for the simulated driver (max 128 chars)",
    )
    parser.add_argument(
        "--route-file",
        required=True,
        help="Path to a GeoJSON LineString file defining the driver's route",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=10.0,
        help="GPS pings per second (default: 10)",
    )
    parser.add_argument(
        "--ingest-url",
        default="http://localhost:8001",
        help="Base URL of the Ingest Service (default: http://localhost:8001)",
    )
    args = parser.parse_args()

    if len(args.driver_id) > 128:
        print(
            f"[ERROR] --driver-id must be at most 128 characters, got {len(args.driver_id)}",
            file=sys.stderr,
        )
        sys.exit(1)

    if args.rate <= 0:
        print("[ERROR] --rate must be a positive number", file=sys.stderr)
        sys.exit(1)

    try:
        coords = load_route(args.route_file)
    except (FileNotFoundError, ValueError, json.JSONDecodeError) as e:
        print(f"[ERROR] Failed to load route file: {e}", file=sys.stderr)
        sys.exit(1)

    interval = 1.0 / args.rate
    index = 0

    print(
        f"[INFO] Starting driver simulator: driver_id={args.driver_id}, "
        f"route={args.route_file} ({len(coords)} points), "
        f"rate={args.rate} pings/sec, ingest={args.ingest_url}"
    )

    try:
        while True:
            lng, lat = interpolate_position(coords, index)
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

            send_ping(
                ingest_url=args.ingest_url,
                driver_id=args.driver_id,
                latitude=lat,
                longitude=lng,
                timestamp=timestamp,
            )

            index += 1
            # Loop back to start when route end is reached (Requirement 5.6).
            if index >= len(coords):
                index = 0

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\n[INFO] Driver simulator stopped.")


if __name__ == "__main__":
    main()
