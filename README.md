# debezium-connector-outbox-poll

A database-agnostic outbox polling connector for Kafka Connect.

Detects row changes (inserts, updates, deletes) by periodically diffing a
watched table's current state against an in-memory baseline, using a
sort-merge diff over sorted `(id, checksum)` pairs. Requires no replication
slots, no WAL access, and no triggers on the watched table.

Design doc: [DDD-53](https://github.com/debezium/debezium-design-documents/blob/main/DDD-53.md),
trigger-less detection mode.

## Status

Early scaffold, under active development. Not yet published to Maven Central.

## Why

Logical replication (the mechanism behind most Debezium connectors) is not
available or not desirable in every deployment — managed databases (RDS,
Cloud SQL) that restrict replication slots being the primary case. This
connector trades JVM heap for the operational risk of an unconsumed
replication slot growing a database's WAL unboundedly.

This is not a CDC replacement. For full CDC with DDL capture and exactly-once
semantics, use the existing Debezium connectors backed by logical replication.

## License

Apache License 2.0. See [LICENSE](LICENSE).
