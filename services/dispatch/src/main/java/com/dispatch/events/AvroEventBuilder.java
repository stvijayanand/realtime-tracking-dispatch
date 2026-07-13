package com.dispatch.events;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Builds Avro {@link GenericRecord} instances from {@link DomainEventEnvelope} objects.
 *
 * <p>The Avro schemas are defined inline to match the contracts in {@code shared/avro/}.
 * This approach avoids compile-time code generation while still producing valid Avro
 * records that the Schema Registry can validate and register.
 *
 * <p>Each domain event schema has a nested payload record. The builder constructs
 * both the outer envelope and the inner payload as {@link GenericRecord} instances.
 */
public final class AvroEventBuilder {

    private AvroEventBuilder() {
        // Utility class — not instantiable.
    }

    // ── TripRequested schema ──────────────────────────────────────────────────

    private static final String TRIP_REQUESTED_SCHEMA_JSON = """
        {
          "type": "record",
          "namespace": "com.dispatch.events",
          "name": "TripRequested",
          "fields": [
            {"name": "event_id", "type": "string"},
            {"name": "event_type", "type": "string", "default": "TripRequested"},
            {"name": "occurred_at", "type": "string"},
            {"name": "payload", "type": {
              "type": "record",
              "name": "TripRequestedPayload",
              "fields": [
                {"name": "trip_id", "type": "string"},
                {"name": "rider_id", "type": "string"},
                {"name": "pickup_location", "type": {
                  "type": "record",
                  "name": "PickupLocation",
                  "fields": [
                    {"name": "latitude", "type": "double"},
                    {"name": "longitude", "type": "double"}
                  ]
                }},
                {"name": "requested_at", "type": "string"}
              ]
            }}
          ]
        }
        """;

    private static final Schema TRIP_REQUESTED_SCHEMA =
        new Schema.Parser().parse(TRIP_REQUESTED_SCHEMA_JSON);

    // ── TripAssigned schema ───────────────────────────────────────────────────

    private static final String TRIP_ASSIGNED_SCHEMA_JSON = """
        {
          "type": "record",
          "namespace": "com.dispatch.events",
          "name": "TripAssigned",
          "fields": [
            {"name": "event_id", "type": "string"},
            {"name": "event_type", "type": "string", "default": "TripAssigned"},
            {"name": "occurred_at", "type": "string"},
            {"name": "payload", "type": {
              "type": "record",
              "name": "TripAssignedPayload",
              "fields": [
                {"name": "trip_id", "type": "string"},
                {"name": "driver_id", "type": "string"},
                {"name": "rider_id", "type": "string"},
                {"name": "assigned_at", "type": "string"}
              ]
            }}
          ]
        }
        """;

    private static final Schema TRIP_ASSIGNED_SCHEMA =
        new Schema.Parser().parse(TRIP_ASSIGNED_SCHEMA_JSON);

    /**
     * Converts a {@code TripRequested} {@link DomainEventEnvelope} into an Avro
     * {@link GenericRecord} suitable for publishing via {@code KafkaAvroSerializer}.
     */
    public static GenericRecord buildTripRequestedRecord(DomainEventEnvelope envelope) {
        var payload = envelope.payload();

        // Build nested PickupLocation record.
        Schema pickupSchema = TRIP_REQUESTED_SCHEMA
            .getField("payload").schema()
            .getField("pickup_location").schema();
        GenericRecord pickupRecord = new GenericData.Record(pickupSchema);

        @SuppressWarnings("unchecked")
        var pickupMap = (java.util.Map<String, Object>) payload.get("pickup_location");
        pickupRecord.put("latitude", ((Number) pickupMap.get("latitude")).doubleValue());
        pickupRecord.put("longitude", ((Number) pickupMap.get("longitude")).doubleValue());

        // Build payload record.
        Schema payloadSchema = TRIP_REQUESTED_SCHEMA.getField("payload").schema();
        GenericRecord payloadRecord = new GenericData.Record(payloadSchema);
        payloadRecord.put("trip_id", payload.get("trip_id"));
        payloadRecord.put("rider_id", payload.get("rider_id"));
        payloadRecord.put("pickup_location", pickupRecord);
        payloadRecord.put("requested_at", payload.get("requested_at"));

        // Build top-level envelope record.
        GenericRecord record = new GenericData.Record(TRIP_REQUESTED_SCHEMA);
        record.put("event_id", envelope.eventId());
        record.put("event_type", envelope.eventType());
        record.put("occurred_at", envelope.occurredAt());
        record.put("payload", payloadRecord);

        return record;
    }

    /**
     * Converts a {@code TripAssigned} {@link DomainEventEnvelope} into an Avro
     * {@link GenericRecord} suitable for publishing via {@code KafkaAvroSerializer}.
     */
    public static GenericRecord buildTripAssignedRecord(DomainEventEnvelope envelope) {
        var payload = envelope.payload();

        // Build payload record.
        Schema payloadSchema = TRIP_ASSIGNED_SCHEMA.getField("payload").schema();
        GenericRecord payloadRecord = new GenericData.Record(payloadSchema);
        payloadRecord.put("trip_id", payload.get("trip_id"));
        payloadRecord.put("driver_id", payload.get("driver_id"));
        payloadRecord.put("rider_id", payload.get("rider_id"));
        payloadRecord.put("assigned_at", payload.get("assigned_at"));

        // Build top-level envelope record.
        GenericRecord record = new GenericData.Record(TRIP_ASSIGNED_SCHEMA);
        record.put("event_id", envelope.eventId());
        record.put("event_type", envelope.eventType());
        record.put("occurred_at", envelope.occurredAt());
        record.put("payload", payloadRecord);

        return record;
    }
}
