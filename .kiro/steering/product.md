# Product Overview

**Real-Time Ride/Delivery Tracking & Dispatch Platform**

A high-throughput backend platform that:
- Ingests live GPS pings from millions of drivers/couriers at scale
- Matches drivers/couriers to nearby riders/orders in real time
- Streams live ETAs to customer-facing apps
- Sends push notifications on order/ride state changes

## Core Concepts

- **Driver/Courier**: A mobile entity that emits GPS pings and is assigned to jobs
- **Rider/Customer**: The end user waiting for a pickup or delivery
- **Order/Ride**: A job that gets matched to a driver and tracked through its lifecycle
- **Dispatch**: The matching logic that assigns the best available driver to an incoming request
- **ETA**: Continuously updated estimated time of arrival streamed to customers

## Key Non-Functional Requirements

- Must handle millions of concurrent GPS ping producers
- Low-latency matching (real-time, not batch)
- Reliable push notification delivery on state transitions
- High availability — downtime directly impacts active rides/deliveries
