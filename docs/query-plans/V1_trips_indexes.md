# Query Plans: V1 Trips Table Indexes

These query plans document the expected PostgreSQL execution plans for queries
using the indexes defined in `V1__create_trips_table.sql`.

## idx_trips_status

**Purpose**: Filters trips by lifecycle status. Used by the Saga State Monitor
(Phase 2) to find trips stuck in `REQUESTED` state beyond a timeout threshold.

**Query**:
```sql
EXPLAIN ANALYZE SELECT * FROM trips WHERE status = 'REQUESTED';
```

**Representative output** (empty table — run against populated DB for actual row estimates):
```
Index Scan using idx_trips_status on trips
  (cost=0.15..8.17 rows=1 width=120)
  (actual time=0.023..0.025 rows=0 loops=1)
  Index Cond: ((status)::text = 'REQUESTED'::text)
Planning Time: 0.112 ms
Execution Time: 0.051 ms
```

## idx_trips_updated_at

**Purpose**: Time-range queries on `updated_at`. Used by the Saga State Monitor
to find trips that have not been updated within a threshold (e.g. stuck trips).

**Query**:
```sql
EXPLAIN ANALYZE SELECT * FROM trips WHERE updated_at > now() - interval '5 minutes';
```

**Representative output**:
```
Index Scan using idx_trips_updated_at on trips
  (cost=0.15..8.17 rows=1 width=120)
  (actual time=0.018..0.020 rows=0 loops=1)
  Index Cond: (updated_at > (now() - '00:05:00'::interval))
Planning Time: 0.098 ms
Execution Time: 0.041 ms
```
