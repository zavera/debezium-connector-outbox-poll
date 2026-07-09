package io.debezium.connector.outboxpoll;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;

/**
 * Writes detected changes to debezium_outbox. Relies on the (row_id,
 * new_checksum) unique constraint for dedupe: concurrent resyncs from
 * multiple replicas racing to write the same detection collapse to a single
 * row without any coordination between instances.
 */
public final class OutboxWriter implements DiffEngine.OutboxSink {

    private static final String INSERT_SQL = """
            INSERT INTO debezium_outbox
                (id, row_id, new_checksum, event_type, payload, idempotency_key, detected_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """;

    private final Connection connection;

    public OutboxWriter(Connection connection) {
        this.connection = connection;
    }

    /**
     * id is derived deterministically from (row_id, new_checksum, type) rather
     * than a centrally-issued sequence. A DB sequence or MAX(id)+1 counter would
     * reintroduce exactly the cross-instance coordination this design avoids
     * everywhere else. Two replicas detecting the identical change compute the
     * identical id, so a PK collision on retry is the same event, not a false
     * conflict -- consistent with the (row_id, new_checksum) dedupe contract.
     */
    private static long deriveId(long rowId, long newChecksum, ChangeType type) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update((rowId + ":" + newChecksum + ":" + type).getBytes(StandardCharsets.UTF_8));
            byte[] hash = digest.digest();
            long id = 0;
            for (int i = 0; i < 8; i++) {
                id = (id << 8) | (hash[i] & 0xFF);
            }
            return id & Long.MAX_VALUE;
        }
        catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    @Override
    public void onChange(long rowId, long newChecksum, ChangeType type, String payload) {
        String idempotencyKey = rowId + ":" + newChecksum + ":" + type;
        // DELETE carries no recoverable row content -- the baseline stores checksums
        // only, so the deleted row's field values were never retained. The DDL marks
        // payload NOT NULL, so a tombstone marker fills that column for deletes.
        String effectivePayload = payload != null ? payload : "{\"row_id\":" + rowId + ",\"content_available\":false}";

        try (PreparedStatement statement = connection.prepareStatement(INSERT_SQL)) {
            statement.setLong(1, deriveId(rowId, newChecksum, type));
            statement.setLong(2, rowId);
            statement.setLong(3, newChecksum);
            statement.setString(4, type.name());
            statement.setString(5, effectivePayload);
            statement.setString(6, idempotencyKey);
            statement.setTimestamp(7, Timestamp.from(Instant.now()));
            statement.executeUpdate();
        }
        catch (SQLException e) {
            if (isUniqueConstraintViolation(e)) {
                // another replica already recorded this exact (row_id, new_checksum) detection
                return;
            }
            throw new IllegalStateException("Failed to write outbox row for row_id=" + rowId, e);
        }
    }

    private static boolean isUniqueConstraintViolation(SQLException e) {
        // SQLState class "23" (integrity constraint violation) is standard across
        // PostgreSQL, MySQL/MariaDB, H2, and most JDBC-compliant databases.
        String sqlState = e.getSQLState();
        return sqlState != null && sqlState.startsWith("23");
    }
}
