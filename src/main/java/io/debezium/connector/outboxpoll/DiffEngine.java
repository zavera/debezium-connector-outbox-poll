package io.debezium.connector.outboxpoll;

import java.util.Iterator;

/**
 * Sort-merge diff between the resident baseline and a freshly scanned,
 * ascending-id-ordered stream of the watched table's current state.
 *
 * This is the "unified detection core" from DDD-53: the same engine drives
 * both the notify-triggered sweep and continuous polling. Batches are
 * consumed from {@code currentSorted} one row at a time and merged
 * immediately, so only the resident baseline and the current row are held
 * in memory -- a scanned batch never needs to be retained whole.
 */
public final class DiffEngine {

    public interface OutboxSink {
        void onChange(long rowId, long newChecksum, ChangeType type, String payload);
    }

    private DiffEngine() {
    }

    /**
     * Merges {@code currentSorted} against {@code oldBaseline}, reporting
     * every changed row to {@code sink} and returning the new baseline.
     *
     * Delete payloads carry {@code null}: the baseline stores checksums
     * only, never row content, so a row missing from the current scan has
     * no recoverable content here. Downstream consumers of DELETE events
     * get identity (row id) but not the deleted row's prior field values --
     * a direct consequence of not requiring a trigger to capture a true
     * before-image.
     */
    public static Baseline merge(Baseline oldBaseline, Iterator<RowScan> currentSorted, OutboxSink sink) {
        Baseline.Builder builder = Baseline.builder(oldBaseline.size());
        int i = 0;
        RowScan current = currentSorted.hasNext() ? currentSorted.next() : null;

        while (current != null && i < oldBaseline.size()) {
            long baselineId = oldBaseline.id(i);

            if (current.id() < baselineId) {
                // present now, absent from baseline -> new row
                sink.onChange(current.id(), current.checksum(), ChangeType.INSERT, current.payload());
                builder.add(current.id(), current.checksum());
                current = currentSorted.hasNext() ? currentSorted.next() : null;
            }
            else if (current.id() > baselineId) {
                // present in baseline, absent now -> row is gone
                sink.onChange(baselineId, oldBaseline.checksum(i), ChangeType.DELETE, null);
                i++;
            }
            else {
                // same id in both -> compare checksums
                if (current.checksum() != oldBaseline.checksum(i)) {
                    sink.onChange(current.id(), current.checksum(), ChangeType.UPDATE, current.payload());
                }
                builder.add(current.id(), current.checksum());
                i++;
                current = currentSorted.hasNext() ? currentSorted.next() : null;
            }
        }

        // remaining current rows have no baseline counterpart -> inserts
        while (current != null) {
            sink.onChange(current.id(), current.checksum(), ChangeType.INSERT, current.payload());
            builder.add(current.id(), current.checksum());
            current = currentSorted.hasNext() ? currentSorted.next() : null;
        }

        // remaining baseline rows have no current counterpart -> deletes
        while (i < oldBaseline.size()) {
            sink.onChange(oldBaseline.id(i), oldBaseline.checksum(i), ChangeType.DELETE, null);
            i++;
        }

        return builder.build();
    }
}
