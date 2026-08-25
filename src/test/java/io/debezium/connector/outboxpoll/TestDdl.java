package io.debezium.connector.outboxpoll;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

final class TestDdl {

    private TestDdl() {
    }

    static String outboxDdl() {
        try (InputStream in = TestDdl.class.getResourceAsStream("/ddl/outbox.sql")) {
            if (in == null) {
                throw new IllegalStateException("ddl/outbox.sql not found on classpath");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
