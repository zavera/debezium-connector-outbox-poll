package io.debezium.connector.outboxpoll;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OutboxWriterTest {

    private Connection connection;

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:h2:mem:outbox-writer-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        try (Statement statement = connection.createStatement()) {
            statement.execute(TestDdl.outboxDdl());
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        connection.close();
    }

    @Test
    void writesNewChangeToOutbox() {
        OutboxWriter writer = new OutboxWriter(connection);
        writer.onChange(1L, 100L, ChangeType.INSERT, "{\"id\":1}");

        assertThat(countRows()).isEqualTo(1);
    }

    @Test
    void dedupesIdenticalRowIdAndChecksum() {
        OutboxWriter writer = new OutboxWriter(connection);
        writer.onChange(1L, 100L, ChangeType.INSERT, "{\"id\":1}");
        // simulates a second replica independently detecting and writing the
        // identical (row_id, new_checksum) change -- must collapse to one row
        writer.onChange(1L, 100L, ChangeType.INSERT, "{\"id\":1}");

        assertThat(countRows()).isEqualTo(1);
    }

    @Test
    void distinctChecksumsForSameRowAreNotDeduped() {
        OutboxWriter writer = new OutboxWriter(connection);
        writer.onChange(1L, 100L, ChangeType.INSERT, "{\"id\":1}");
        writer.onChange(1L, 200L, ChangeType.UPDATE, "{\"id\":1,\"v\":2}");

        assertThat(countRows()).isEqualTo(2);
    }

    @Test
    void deleteWithNoPayloadStillSatisfiesNotNullConstraint() {
        OutboxWriter writer = new OutboxWriter(connection);
        writer.onChange(1L, 100L, ChangeType.DELETE, null);

        assertThat(countRows()).isEqualTo(1);
    }

    @Test
    void reinsertWithSameRowIdAndChecksumAsAPriorDeleteIsNotDroppedAsADuplicate() {
        // a delete tombstone stores the row's last known checksum under
        // (row_id, new_checksum) -- if that row_id is later reused for a new
        // row whose content happens to serialize to the identical checksum,
        // the INSERT must not collide with the earlier DELETE's row
        OutboxWriter writer = new OutboxWriter(connection);
        writer.onChange(1L, 100L, ChangeType.DELETE, null);
        writer.onChange(1L, 100L, ChangeType.INSERT, "{\"id\":1}");

        assertThat(countRows()).isEqualTo(2);
    }

    @Test
    void reinsertWithSameChecksumAsTheOriginalInsertIsNotDroppedAsADuplicate() {
        // event_type alone can't distinguish this insert from the original one --
        // both are INSERT with the same checksum. generation is what tells them
        // apart: 0 deletes had happened when row 1 first appeared, 1 delete had
        // happened by the time it reappeared with identical content.
        OutboxWriter writer = new OutboxWriter(connection);
        writer.onChange(1L, 100L, ChangeType.INSERT, "{\"id\":1}"); // t1: first appearance
        writer.onChange(1L, 100L, ChangeType.DELETE, null); // t2: deleted
        writer.onChange(1L, 100L, ChangeType.INSERT, "{\"id\":1}"); // t3: reappears, identical content

        assertThat(countRows()).isEqualTo(3);
    }

    @Test
    void replicaRaceStillDedupesAfterADeleteHasAlreadyHappened() {
        // confirms generation doesn't reopen the race-dedup hole it's meant to
        // close: two replicas racing on the SAME post-delete reinsert must
        // still collapse to one row, since neither write changes the delete
        // count they both compute generation from
        OutboxWriter writer = new OutboxWriter(connection);
        writer.onChange(1L, 100L, ChangeType.INSERT, "{\"id\":1}");
        writer.onChange(1L, 100L, ChangeType.DELETE, null);
        writer.onChange(1L, 100L, ChangeType.INSERT, "{\"id\":1}");
        // a second replica independently detects the same t3 reinsert
        writer.onChange(1L, 100L, ChangeType.INSERT, "{\"id\":1}");

        assertThat(countRows()).isEqualTo(3);
    }

    private int countRows() {
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM debezium_outbox")) {
            rs.next();
            return rs.getInt(1);
        }
        catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
