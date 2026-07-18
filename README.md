# debezium-connector-outbox-poll

A zero-broker, database-agnostic outbox polling agent.

Detects row changes (inserts, updates, deletes) by periodically diffing a
watched table's current state against an in-memory baseline, using a
sort-merge diff over sorted `(id, checksum)` pairs. Requires no replication
slots, no WAL access, and no triggers on the watched table.

This is a standalone JVM process, not a Kafka Connect connector. It has no
Kafka dependency at all: it writes detected changes to `debezium_outbox` and
stops there. The outbox table is the entire interface — downstream consumers
read it however they choose to (direct polling, their own scheduled job,
etc.). Nothing here assumes or requires a broker.

Design doc: [DDD-53](https://github.com/debezium/debezium-design-documents/blob/main/DDD-53.md),
trigger-less detection mode.

## Status

Early scaffold, under active development. Not yet published to Maven Central.

## Running

```
java -jar debezium-connector-outbox-poll.jar config.properties
```

```properties
jdbc.url=jdbc:postgresql://localhost:5432/mydb
jdbc.user=outbox_poll
jdbc.password=...
table.name=orders
table.id.column=id
poll.interval.ms=5000
scan.fetch.size=1000
outbox.retention.days=7
```

## Why

Logical replication (the mechanism behind most Debezium connectors) is not
available or not desirable in every deployment — managed databases (RDS,
Cloud SQL) that restrict replication slots being the primary case. This
agent trades JVM heap for the operational risk of an unconsumed replication
slot growing a database's WAL unboundedly.

This is not a CDC replacement. For full CDC with DDL capture and exactly-once
semantics, use the existing Debezium connectors backed by logical replication.

## License

Apache License 2.0. See [LICENSE](LICENSE).
