package io.debezium.connector.outboxpoll;

import java.util.Arrays;

/**
 * In-memory baseline of the watched table: sorted (id, checksum) pairs held as
 * primitive arrays rather than a stored payload, per DDD-53's memory-footprint
 * trade-off (~15 MB/million rows steady state, ~30 MB peak mid-sweep).
 *
 * Entries are sorted ascending by id so the sort-merge diff can advance both
 * inputs with a single pass.
 */
public final class Baseline {

    private static final Baseline EMPTY = new Baseline(new long[0], new long[0], 0);

    private final long[] ids;
    private final long[] checksums;
    private final int size;

    private Baseline(long[] ids, long[] checksums, int size) {
        this.ids = ids;
        this.checksums = checksums;
        this.size = size;
    }

    public static Baseline empty() {
        return EMPTY;
    }

    public int size() {
        return size;
    }

    public long id(int index) {
        return ids[index];
    }

    public long checksum(int index) {
        return checksums[index];
    }

    /** Binary search by id; returns -1 if not present. */
    public int indexOf(long id) {
        int idx = Arrays.binarySearch(ids, 0, size, id);
        return idx >= 0 ? idx : -1;
    }

    public static Builder builder(int expectedSize) {
        return new Builder(expectedSize);
    }

    /**
     * Appends entries in ascending id order. The sort-merge diff writes its
     * merged output directly into a Builder, so the new baseline is produced
     * in the same pass that detects changes -- no separate re-sort step.
     */
    public static final class Builder {
        private long[] ids;
        private long[] checksums;
        private int size;
        private long lastId = Long.MIN_VALUE;
        private boolean hasLast;

        private Builder(int expectedSize) {
            int initialCapacity = Math.max(expectedSize, 16);
            this.ids = new long[initialCapacity];
            this.checksums = new long[initialCapacity];
        }

        public Builder add(long id, long checksum) {
            if (hasLast && id <= lastId) {
                throw new IllegalArgumentException(
                        "Baseline entries must be appended in strictly ascending id order: " + id + " <= " + lastId);
            }
            ensureCapacity(size + 1);
            ids[size] = id;
            checksums[size] = checksum;
            size++;
            lastId = id;
            hasLast = true;
            return this;
        }

        private void ensureCapacity(int minCapacity) {
            if (minCapacity > ids.length) {
                int newCapacity = Math.max(ids.length * 2, minCapacity);
                ids = Arrays.copyOf(ids, newCapacity);
                checksums = Arrays.copyOf(checksums, newCapacity);
            }
        }

        public Baseline build() {
            return new Baseline(Arrays.copyOf(ids, size), Arrays.copyOf(checksums, size), size);
        }
    }
}
