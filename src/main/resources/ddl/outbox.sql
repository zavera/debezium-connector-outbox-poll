-- debezium_outbox: single source of truth for the trigger-less detection mode.
-- Schema as specified in DDD-53, "Trigger-less Detection Mode" section.
-- (row_id, new_checksum) is unique so concurrent resyncs from multiple
-- replicas collapse without coordination.

CREATE TABLE debezium_outbox (
    id              BIGINT PRIMARY KEY,
    row_id          BIGINT NOT NULL,
    new_checksum    BIGINT NOT NULL,
    event_type      TEXT NOT NULL,
    payload         TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    detected_at     TIMESTAMP NOT NULL,
    consumed_at     TIMESTAMP,
    UNIQUE (row_id, new_checksum)
);
