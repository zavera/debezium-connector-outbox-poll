package io.debezium.connector.outboxpoll;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Handles the "outbox dropped or truncated out from under the connector"
 * failure mode described in DDD-53. Durability of the table itself is
 * delegated to whatever backup/replication policy the operator already
 * applies to the database; this class only detects the gap and rebuilds.
 */
public final class OutboxRecovery {

    private static final String DDL = loadDdl();

    private final Connection connection;

    public OutboxRecovery(Connection connection) {
        this.connection = connection;
    }

    /** Lightweight existence check, per DDD-53's "existence check" option. */
    public boolean outboxExists() {
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SELECT 1 FROM debezium_outbox WHERE 1 = 0")) {
            return true;
        }
        catch (SQLException e) {
            if (isTableMissing(e)) {
                return false;
            }
            // an unrelated error (connectivity, permissions, ...) must not be
            // misread as "table missing" -- that would trigger a spurious
            // CREATE TABLE attempt and mask the real failure
            throw new IllegalStateException("Failed to check debezium_outbox existence", e);
        }
    }

    private static boolean isTableMissing(SQLException e) {
        // 42P01 (PostgreSQL "undefined_table", verified against a live
        // instance), 42S02 (MySQL/MariaDB "table not found"), and 42S04 (H2
        // "table not found", verified empirically -- H2 does not use 42S02
        // despite modelling much of its error catalog on MySQL).
        String sqlState = e.getSQLState();
        return "42P01".equals(sqlState) || "42S02".equals(sqlState) || "42S04".equals(sqlState);
    }

    public void recreateOutbox() {
        try (Statement statement = connection.createStatement()) {
            statement.execute(DDL);
        }
        catch (SQLException e) {
            throw new IllegalStateException("Failed to recreate debezium_outbox", e);
        }
    }

    private static String loadDdl() {
        try (InputStream in = OutboxRecovery.class.getResourceAsStream("/ddl/outbox.sql")) {
            if (in == null) {
                throw new IllegalStateException("ddl/outbox.sql not found on classpath");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        catch (IOException e) {
            throw new IllegalStateException("Failed to load outbox DDL", e);
        }
    }
}
