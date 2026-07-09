package io.debezium.connector.outboxpoll;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.Test;

class DiffEngineTest {

    private record RecordedChange(long rowId, long checksum, ChangeType type, String payload) {
    }

    private static final class RecordingSink implements DiffEngine.OutboxSink {
        final List<RecordedChange> changes = new ArrayList<>();

        @Override
        public void onChange(long rowId, long newChecksum, ChangeType type, String payload) {
            changes.add(new RecordedChange(rowId, newChecksum, type, payload));
        }
    }

    private static Baseline baselineOf(long... idChecksumPairs) {
        Baseline.Builder builder = Baseline.builder(idChecksumPairs.length / 2);
        for (int i = 0; i < idChecksumPairs.length; i += 2) {
            builder.add(idChecksumPairs[i], idChecksumPairs[i + 1]);
        }
        return builder.build();
    }

    private static Iterator<RowScan> scans(RowScan... rows) {
        return List.of(rows).iterator();
    }

    @Test
    void detectsInsertOnEmptyBaseline() {
        RecordingSink sink = new RecordingSink();
        Baseline result = DiffEngine.merge(Baseline.empty(), scans(new RowScan(1L, 100L, "{\"id\":1}")), sink);

        assertThat(sink.changes).containsExactly(new RecordedChange(1L, 100L, ChangeType.INSERT, "{\"id\":1}"));
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.id(0)).isEqualTo(1L);
        assertThat(result.checksum(0)).isEqualTo(100L);
    }

    @Test
    void detectsUpdateWhenChecksumDiffers() {
        Baseline oldBaseline = baselineOf(1L, 100L);
        RecordingSink sink = new RecordingSink();

        Baseline result = DiffEngine.merge(oldBaseline, scans(new RowScan(1L, 200L, "{\"id\":1,\"v\":2}")), sink);

        assertThat(sink.changes).containsExactly(new RecordedChange(1L, 200L, ChangeType.UPDATE, "{\"id\":1,\"v\":2}"));
        assertThat(result.checksum(0)).isEqualTo(200L);
    }

    @Test
    void emitsNothingWhenChecksumUnchanged() {
        Baseline oldBaseline = baselineOf(1L, 100L);
        RecordingSink sink = new RecordingSink();

        Baseline result = DiffEngine.merge(oldBaseline, scans(new RowScan(1L, 100L, "{\"id\":1}")), sink);

        assertThat(sink.changes).isEmpty();
        assertThat(result.checksum(0)).isEqualTo(100L);
    }

    @Test
    void detectsDeleteWhenRowMissingFromScan() {
        Baseline oldBaseline = baselineOf(1L, 100L, 2L, 200L);
        RecordingSink sink = new RecordingSink();

        Baseline result = DiffEngine.merge(oldBaseline, scans(new RowScan(1L, 100L, "{\"id\":1}")), sink);

        assertThat(sink.changes).containsExactly(new RecordedChange(2L, 200L, ChangeType.DELETE, null));
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.id(0)).isEqualTo(1L);
    }

    @Test
    void handlesInterleavedInsertUpdateDeleteInSinglePass() {
        // baseline: 1 (unchanged), 2 (will be deleted), 4 (will be updated)
        Baseline oldBaseline = baselineOf(1L, 10L, 2L, 20L, 4L, 40L);
        RecordingSink sink = new RecordingSink();

        // current: 1 (unchanged), 3 (new insert), 4 (updated)
        Baseline result = DiffEngine.merge(oldBaseline, scans(
                new RowScan(1L, 10L, "{\"id\":1}"),
                new RowScan(3L, 30L, "{\"id\":3}"),
                new RowScan(4L, 44L, "{\"id\":4}")), sink);

        // emission order follows the merge's single left-to-right pass over both
        // sorted inputs, not insert/update/delete grouping: row 2 (id 2) is
        // discovered missing before the scan pointer reaches id 3 or id 4.
        assertThat(sink.changes).containsExactly(
                new RecordedChange(2L, 20L, ChangeType.DELETE, null),
                new RecordedChange(3L, 30L, ChangeType.INSERT, "{\"id\":3}"),
                new RecordedChange(4L, 44L, ChangeType.UPDATE, "{\"id\":4}"));

        assertThat(result.size()).isEqualTo(3);
        assertThat(result.id(0)).isEqualTo(1L);
        assertThat(result.id(1)).isEqualTo(3L);
        assertThat(result.id(2)).isEqualTo(4L);
        assertThat(result.checksum(2)).isEqualTo(44L);
    }
}
