package io.debezium.connector.outboxpoll;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OutboxMaintenanceTest {

    private static final String INSERT_SQL = """
            INSERT INTO debezium_outbox
                (id, row_id, new_checksum, event_type, payload, idempotency_key, detected_at, consumed_at)
            VALUES (?, ?, ?, 'INSERT', '{}', 'k', ?, ?)
            """;

    private Connection connection;

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:h2:mem:outbox-maintenance-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        try (Statement statement = connection.createStatement()) {
            statement.execute(TestDdl.outboxDdl());
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        connection.close();
    }

    @Test
    void purgesConsumedRowsOlderThanRetention() throws SQLException {
        insertRow(1L, Instant.now().minus(10, ChronoUnit.DAYS), Instant.now().minus(9, ChronoUnit.DAYS));

        int purged = new OutboxMaintenance(connection).purge(7);

        assertThat(purged).isEqualTo(1);
        assertThat(countRows()).isEqualTo(0);
    }

    @Test
    void keepsConsumedRowsWithinRetentionWindow() throws SQLException {
        insertRow(1L, Instant.now().minus(1, ChronoUnit.DAYS), Instant.now().minus(1, ChronoUnit.DAYS));

        int purged = new OutboxMaintenance(connection).purge(7);

        assertThat(purged).isEqualTo(0);
        assertThat(countRows()).isEqualTo(1);
    }

    @Test
    void neverPurgesUnconsumedRowsRegardlessOfAge() throws SQLException {
        insertRow(1L, Instant.now().minus(30, ChronoUnit.DAYS), null);

        int purged = new OutboxMaintenance(connection).purge(7);

        assertThat(purged).isEqualTo(0);
        assertThat(countRows()).isEqualTo(1);
    }

    private void insertRow(long id, Instant detectedAt, Instant consumedAt) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(INSERT_SQL)) {
            statement.setLong(1, id);
            statement.setLong(2, id);
            statement.setLong(3, id * 100);
            statement.setTimestamp(4, Timestamp.from(detectedAt));
            if (consumedAt != null) {
                statement.setTimestamp(5, Timestamp.from(consumedAt));
            }
            else {
                statement.setNull(5, java.sql.Types.TIMESTAMP);
            }
            statement.executeUpdate();
        }
    }

    private int countRows() throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM debezium_outbox")) {
            rs.next();
            return rs.getInt(1);
        }
    }
}
