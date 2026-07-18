package io.debezium.connector.outboxpoll;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OutboxRecoveryTest {

    private Connection connection;

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:h2:mem:outbox-recovery-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
    }

    @AfterEach
    void tearDown() throws SQLException {
        connection.close();
    }

    @Test
    void reportsMissingWhenTableWasNeverCreated() {
        OutboxRecovery recovery = new OutboxRecovery(connection);
        assertThat(recovery.outboxExists()).isFalse();
    }

    @Test
    void reportsPresentAfterCreation() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(TestDdl.outboxDdl());
        }
        OutboxRecovery recovery = new OutboxRecovery(connection);
        assertThat(recovery.outboxExists()).isTrue();
    }

    @Test
    void recreateOutboxMakesTableUsableAgain() {
        OutboxRecovery recovery = new OutboxRecovery(connection);
        assertThat(recovery.outboxExists()).isFalse();

        recovery.recreateOutbox();

        assertThat(recovery.outboxExists()).isTrue();
        // table is genuinely usable, not just present -- the writer path works
        new OutboxWriter(connection).onChange(1L, 10L, ChangeType.INSERT, "{\"id\":1}");
    }

    @Test
    void sweepEngineRebuildsBaselineFromScratchWhenOutboxIsGone() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(TestDdl.outboxDdl());
            statement.execute("CREATE TABLE watched (id BIGINT PRIMARY KEY, v INT)");
            statement.execute("INSERT INTO watched VALUES (1, 10), (2, 20)");
        }

        SweepEngine sweepEngine = new SweepEngine(connection, "watched", "id", 100, 7);
        sweepEngine.sweep();
        assertThat(sweepEngine.currentBaseline().size()).isEqualTo(2);

        // simulate outbox loss: drop the table entirely
        try (Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE debezium_outbox");
        }

        // next sweep must detect the loss, recreate the table, and repopulate
        // both the baseline and the outbox from current state -- same code
        // path as a fresh startup, per DDD-53
        sweepEngine.sweep();

        assertThat(sweepEngine.currentBaseline().size()).isEqualTo(2);
        assertThat(new OutboxRecovery(connection).outboxExists()).isTrue();

        try (Statement statement = connection.createStatement();
                var rs = statement.executeQuery("SELECT COUNT(*) FROM debezium_outbox")) {
            rs.next();
            assertThat(rs.getInt(1)).isEqualTo(2);
        }
    }
}
