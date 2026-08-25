-- debezium_outbox: single source of truth for the trigger-less detection mode.
-- Schema as specified in DDD-53, "Trigger-less Detection Mode" section.
--
-- generation is the number of DELETE events already recorded for row_id at
-- the time this row was written. event_type alone cannot distinguish an
-- insert from a row's first appearance versus an insert after that same
-- row_id was deleted and reinserted with byte-identical content -- both
-- would be (row_id, new_checksum, INSERT). generation breaks that tie: it
-- only advances when a real delete happens in the source table, so two
-- replicas racing to write the *same* real occurrence always compute the
-- same generation (dedupe preserved), while two occurrences separated by a
-- genuine delete compute different generations (no longer collide).
-- event_type is VARCHAR, not TEXT: it only ever holds INSERT/UPDATE/DELETE,
-- and it participates in the UNIQUE constraint below. MySQL/MariaDB reject a
-- BLOB/TEXT column in a key without an explicit prefix length; Postgres and
-- H2 silently allow it, which is exactly the kind of divergence that only
-- shows up by actually testing against each database.
CREATE TABLE debezium_outbox (
    id              BIGINT PRIMARY KEY,
    row_id          BIGINT NOT NULL,
    new_checksum    BIGINT NOT NULL,
    event_type      VARCHAR(20) NOT NULL,
    generation      BIGINT NOT NULL,
    payload         TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    detected_at     TIMESTAMP NOT NULL,
    consumed_at     TIMESTAMP,
    UNIQUE (row_id, new_checksum, event_type, generation)
);
