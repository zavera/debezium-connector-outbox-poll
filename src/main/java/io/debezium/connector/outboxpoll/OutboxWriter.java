package io.debezium.connector.outboxpoll;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;

/**
 * Writes detected changes to debezium_outbox. Relies on the (row_id,
 * new_checksum, event_type, generation) unique constraint for dedupe:
 * concurrent resyncs from multiple replicas racing to write the same
 * detection collapse to a single row without any coordination between
 * instances.
 */
public final class OutboxWriter implements DiffEngine.OutboxSink {

    private static final String GENERATION_SQL = """
            SELECT COUNT(*) FROM debezium_outbox WHERE row_id = ? AND event_type = 'DELETE'
            """;

    private static final String INSERT_SQL = """
            INSERT INTO debezium_outbox
                (id, row_id, new_checksum, event_type, generation, payload, idempotency_key, detected_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private final Connection connection;

    public OutboxWriter(Connection connection) {
        this.connection = connection;
    }

    /**
     * id is derived deterministically from (row_id, new_checksum, type,
     * generation) rather than a centrally-issued sequence. A DB sequence or
     * MAX(id)+1 counter would reintroduce exactly the cross-instance
     * coordination this design avoids everywhere else. Two replicas racing on
     * the *same* real occurrence compute the same generation (see
     * queryGeneration) and therefore the same id, so a PK collision on retry
     * is the same event, not a false conflict.
     */
    private static long deriveId(long rowId, long newChecksum, ChangeType type, long generation) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update((rowId + ":" + newChecksum + ":" + type + ":" + generation).getBytes(StandardCharsets.UTF_8));
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

    /**
     * Number of DELETE events already recorded for rowId. Deliberately counts
     * only deletes, not matches on the full (row_id, checksum, event_type)
     * triple: a query scoped to the exact triple would give two replicas
     * racing on the *same* insert different answers depending on which one's
     * write commits first, breaking the very dedupe this is meant to
     * preserve. Counting deletes only changes this number when a real delete
     * happens in the source table, not when a duplicate write attempt does.
     */
    private long queryGeneration(long rowId) {
        try (PreparedStatement statement = connection.prepareStatement(GENERATION_SQL)) {
            statement.setLong(1, rowId);
            try (ResultSet rs = statement.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
        catch (SQLException e) {
            throw new IllegalStateException("Failed to compute generation for row_id=" + rowId, e);
        }
    }

    @Override
    public void onChange(long rowId, long newChecksum, ChangeType type, String payload) {
        long generation = queryGeneration(rowId);
        String idempotencyKey = rowId + ":" + newChecksum + ":" + type + ":" + generation;
        // DELETE carries no recoverable row content -- the baseline stores checksums
        // only, so the deleted row's field values were never retained. The DDL marks
        // payload NOT NULL, so a tombstone marker fills that column for deletes.
        String effectivePayload = payload != null ? payload : "{\"row_id\":" + rowId + ",\"content_available\":false}";

        try (PreparedStatement statement = connection.prepareStatement(INSERT_SQL)) {
            statement.setLong(1, deriveId(rowId, newChecksum, type, generation));
            statement.setLong(2, rowId);
            statement.setLong(3, newChecksum);
            statement.setString(4, type.name());
            statement.setLong(5, generation);
            statement.setString(6, effectivePayload);
            statement.setString(7, idempotencyKey);
            statement.setTimestamp(8, Timestamp.from(Instant.now()));
            statement.executeUpdate();
        }
        catch (SQLException e) {
            if (isUniqueConstraintViolation(e)) {
                // another replica already recorded this exact (row_id, new_checksum,
                // event_type, generation) detection
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
