package io.debezium.connector.outboxpoll;

import java.nio.charset.StandardCharsets;

/**
 * 64-bit FNV-1a hash used as the row checksum. Chosen over a cryptographic
 * hash to keep the per-row baseline entry cheap: this is a change-detection
 * checksum, not a security boundary, so collision resistance beyond
 * accidental collisions is not required.
 */
final class ChecksumUtil {

    private static final long FNV_OFFSET_BASIS = 0xcbf29ce484222325L;
    private static final long FNV_PRIME = 0x100000001b3L;

    private ChecksumUtil() {
    }

    static long checksum(String content) {
        long hash = FNV_OFFSET_BASIS;
        for (byte b : content.getBytes(StandardCharsets.UTF_8)) {
            hash ^= (b & 0xFF);
            hash *= FNV_PRIME;
        }
        return hash;
    }
}
