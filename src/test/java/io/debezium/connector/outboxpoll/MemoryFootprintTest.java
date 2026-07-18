package io.debezium.connector.outboxpoll;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Proves the memory-retention shape from DDD-53's "streaming-merge-batches"
 * diagram: only the resident baseline (primitive id/checksum longs) and the
 * row/batch currently being merged are ever reachable -- everything else
 * must become GC-eligible immediately, not accumulate for the sweep's
 * duration. WeakReference + System.gc() is the standard way to prove
 * non-retention in a unit test; it is used here in place of an actual heap
 * measurement, which would be flaky across JVMs/CI.
 */
class MemoryFootprintTest {

    private Connection connection;

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:h2:mem:memory-footprint-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
    }

    @AfterEach
    void tearDown() throws SQLException {
        connection.close();
    }

    @Test
    void tableScannerDoesNotRetainConsumedRows() throws SQLException {
        int rowCount = 5_000;
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE watched (id BIGINT PRIMARY KEY, payload VARCHAR(500))");
            for (long id = 1; id <= rowCount; id++) {
                statement.execute("INSERT INTO watched VALUES (" + id + ", '" + "x".repeat(500) + "')");
            }
        }

        List<WeakReference<RowScan>> refs = new ArrayList<>(rowCount);
        try (TableScanner scanner = new TableScanner(connection, "watched", "id", 100)) {
            while (scanner.hasNext()) {
                RowScan row = scanner.next();
                refs.add(new WeakReference<>(row));
                // no strong reference to `row` survives past this iteration --
                // the scanner itself must not be holding one either
            }
        }

        assertNoneReachable(refs, "TableScanner");
    }

    @Test
    void diffEngineMergeDoesNotRetainRowScansAfterBuildingBaseline() {
        int rowCount = 5_000;
        List<RowScan> rows = new ArrayList<>(rowCount);
        for (long id = 1; id <= rowCount; id++) {
            rows.add(new RowScan(id, id * 31, "{\"id\":" + id + ",\"padding\":\"" + "x".repeat(200) + "\"}"));
        }

        List<WeakReference<RowScan>> refs = new ArrayList<>(rowCount);
        Iterator<RowScan> tracking = new Iterator<>() {
            private final Iterator<RowScan> delegate = rows.iterator();

            @Override
            public boolean hasNext() {
                return delegate.hasNext();
            }

            @Override
            public RowScan next() {
                RowScan row = delegate.next();
                refs.add(new WeakReference<>(row));
                return row;
            }
        };

        Baseline result = DiffEngine.merge(Baseline.empty(), tracking, (rowId, checksum, type, payload) -> {
            // simulates OutboxWriter: consumes the row's data immediately, retains nothing
        });

        // drop the only remaining strong references before checking reachability
        rows.clear();

        assertThat(result.size()).isEqualTo(rowCount);
        assertNoneReachable(refs, "DiffEngine.merge / Baseline");
    }

    /** Forces GC and asserts every tracked reference has been cleared, retrying to absorb GC timing variance. */
    private static void assertNoneReachable(List<WeakReference<RowScan>> refs, String label) {
        long stillAlive = refs.size();
        for (int attempt = 0; attempt < 5 && stillAlive > 0; attempt++) {
            System.gc();
            stillAlive = refs.stream().filter(ref -> ref.get() != null).count();
        }
        assertThat(stillAlive)
                .withFailMessage("%s retained %d/%d rows after merge/scan -- expected all to be GC-eligible", label, stillAlive, refs.size())
                .isZero();
    }
}
