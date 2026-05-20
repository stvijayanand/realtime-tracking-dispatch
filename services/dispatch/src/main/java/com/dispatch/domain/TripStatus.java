package com.dispatch.domain;

import java.util.Map;
import java.util.Set;

public enum TripStatus {
    REQUESTED, ASSIGNED, CANCELLED;

    private static final Map<TripStatus, Set<TripStatus>> VALID_TRANSITIONS = Map.of(
        REQUESTED, Set.of(ASSIGNED, CANCELLED),
        ASSIGNED,  Set.of(CANCELLED),
        CANCELLED, Set.of()
    );

    public void assertCanTransitionTo(TripStatus next) {
        if (!VALID_TRANSITIONS.get(this).contains(next)) {
            throw new IllegalStateTransitionException(this, next);
        }
    }
}
