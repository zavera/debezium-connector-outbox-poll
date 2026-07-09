package io.debezium.connector.outboxpoll;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Claims pending outbox rows and marks them consumed, mirroring the
 * claim-then-mark pattern from DDD-53's OutboxSweeper. FOR UPDATE SKIP LOCKED
 * is the horizontal-scaling primitive: concurrent task instances each claim
 * a disjoint batch with no coordination between them.
 */
public final class OutboxConsumer implements AutoCloseable {

    public record OutboxRow(long id, long rowId, String eventType, String payload, String idempotencyKey, Timestamp detectedAt) {
    }

    private static final String CLAIM_SQL = """
            SELECT id, row_id, event_type, payload, idempotency_key, detected_at
            FROM debezium_outbox
            WHERE consumed_at IS NULL
            ORDER BY detected_at ASC
            LIMIT ?
            FOR UPDATE SKIP LOCKED
            """;

    private final Connection connection;

    public OutboxConsumer(Connection connection) throws SQLException {
        this.connection = connection;
        this.connection.setAutoCommit(false);
    }

    public List<OutboxRow> claimBatch(int batchSize) throws SQLException {
        List<OutboxRow> claimed = new ArrayList<>(batchSize);
        try (PreparedStatement select = connection.prepareStatement(CLAIM_SQL)) {
            select.setInt(1, batchSize);
            try (ResultSet rs = select.executeQuery()) {
                while (rs.next()) {
                    claimed.add(new OutboxRow(
                            rs.getLong("id"),
                            rs.getLong("row_id"),
                            rs.getString("event_type"),
                            rs.getString("payload"),
                            rs.getString("idempotency_key"),
                            rs.getTimestamp("detected_at")));
                }
            }
        }

        if (!claimed.isEmpty()) {
            try (PreparedStatement update = connection.prepareStatement(
                    "UPDATE debezium_outbox SET consumed_at = ? WHERE id = ?")) {
                Timestamp now = new Timestamp(System.currentTimeMillis());
                for (OutboxRow row : claimed) {
                    update.setTimestamp(1, now);
                    update.setLong(2, row.id());
                    update.addBatch();
                }
                update.executeBatch();
            }
        }

        connection.commit();
        return claimed;
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }
}
