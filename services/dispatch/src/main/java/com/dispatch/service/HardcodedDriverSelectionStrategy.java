package com.dispatch.service;

import com.dispatch.web.dto.PickupLocation;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Phase 1 driver selection strategy: round-robin from a static in-memory list.
 *
 * <p>Ignores the {@code pickup} location — all three drivers are treated as
 * equally available regardless of position. Phase 2 replaces this with a
 * Redis GEORADIUS query against the CQRS local read model (see ADR 005).
 *
 * <p>Thread-safe: {@link AtomicInteger} ensures correct round-robin behaviour
 * under concurrent requests without synchronisation overhead.
 */
@Component
public class HardcodedDriverSelectionStrategy implements DriverSelectionStrategy {

    private static final List<String> DRIVERS =
        List.of("driver-001", "driver-002", "driver-003");

    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public String selectDriver(PickupLocation pickup) {
        // Round-robin: counter increments atomically; modulo wraps around the list.
        // pickup is intentionally ignored in Phase 1.
        int index = Math.abs(counter.getAndIncrement() % DRIVERS.size());
        return DRIVERS.get(index);
    }
}
