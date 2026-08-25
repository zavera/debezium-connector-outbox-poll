package io.debezium.connector.outboxpoll;

/**
 * A single row read from the watched table during a sweep's table scan,
 * in ascending id order. {@code payload} is the row serialised as JSON at
 * scan time -- the only row content this design retains, since the baseline
 * itself stores checksums only (see Baseline).
 */
public record RowScan(long id, long checksum, String payload) {
}
