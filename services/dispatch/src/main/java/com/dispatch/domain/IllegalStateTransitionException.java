package com.dispatch.domain;

public class IllegalStateTransitionException extends RuntimeException {
    private final TripStatus from;
    private final TripStatus to;

    public IllegalStateTransitionException(TripStatus from, TripStatus to) {
        super(String.format("Invalid trip state transition: %s -> %s", from, to));
        this.from = from;
        this.to = to;
    }

    public TripStatus getFrom() { return from; }
    public TripStatus getTo() { return to; }
}
