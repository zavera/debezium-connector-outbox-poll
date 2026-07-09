package io.debezium.connector.outboxpoll;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Orchestrates a single sweep: recovery check, table scan, sort-merge diff,
 * outbox write. Drives both the notify-triggered sweep and the fixed-interval
 * poll from the same code path, per DDD-53's "unified detection core".
 *
 * Instance restart and outbox loss intentionally share this same path: both
 * start (or resume) from an empty/discarded baseline, and the resulting
 * all-rows-look-new diff repopulates the outbox, deduplicated against
 * whatever survived via (row_id, new_checksum).
 */
public final class SweepEngine {

    private final Connection connection;
    private final String tableName;
    private final String idColumn;
    private final int fetchSize;
    private final OutboxRecovery recovery;

    private volatile Baseline baseline = Baseline.empty();

    public SweepEngine(Connection connection, String tableName, String idColumn, int fetchSize) {
        this.connection = connection;
        this.tableName = tableName;
        this.idColumn = idColumn;
        this.fetchSize = fetchSize;
        this.recovery = new OutboxRecovery(connection);
    }

    public synchronized void sweep() {
        if (!recovery.outboxExists()) {
            recovery.recreateOutbox();
            baseline = Baseline.empty();
        }

        OutboxWriter writer = new OutboxWriter(connection);
        try (TableScanner scanner = new TableScanner(connection, tableName, idColumn, fetchSize)) {
            baseline = DiffEngine.merge(baseline, scanner, writer);
        }
        catch (SQLException e) {
            throw new IllegalStateException("Sweep failed for table " + tableName, e);
        }
    }

    public Baseline currentBaseline() {
        return baseline;
    }
}
