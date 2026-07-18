package io.debezium.connector.outboxpoll;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Purges consumed outbox rows past the configured retention window, per
 * DDD-53's "Outbox Maintenance" section. The cutoff is computed in Java
 * rather than via a database-specific interval function (Postgres'
 * make_interval, in the doc's example query), keeping the query portable
 * across the JDBC-accessible databases this design targets.
 */
public final class OutboxMaintenance {

    private static final String DELETE_SQL = """
            DELETE FROM debezium_outbox
            WHERE consumed_at IS NOT NULL
              AND consumed_at < ?
            """;

    private final Connection connection;

    public OutboxMaintenance(Connection connection) {
        this.connection = connection;
    }

    /** Deletes consumed rows older than {@code retentionDays}; returns the number removed. */
    public int purge(int retentionDays) {
        Timestamp cutoff = Timestamp.from(Instant.now().minus(retentionDays, ChronoUnit.DAYS));
        try (PreparedStatement statement = connection.prepareStatement(DELETE_SQL)) {
            statement.setTimestamp(1, cutoff);
            return statement.executeUpdate();
        }
        catch (SQLException e) {
            throw new IllegalStateException("Failed to purge consumed outbox rows", e);
        }
    }
}
