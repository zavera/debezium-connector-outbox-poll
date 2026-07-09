package io.debezium.connector.outboxpoll;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

public final class OutboxPollConnectorConfig extends AbstractConfig {

    public static final String JDBC_URL = "jdbc.url";
    public static final String JDBC_USER = "jdbc.user";
    public static final String JDBC_PASSWORD = "jdbc.password";
    public static final String TABLE_NAME = "table.name";
    public static final String TABLE_ID_COLUMN = "table.id.column";
    public static final String TOPIC = "topic";
    public static final String POLL_INTERVAL_MS = "poll.interval.ms";
    public static final String SCAN_FETCH_SIZE = "scan.fetch.size";
    public static final String CONSUME_BATCH_SIZE = "consume.batch.size";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(JDBC_URL, Type.STRING, Importance.HIGH, "JDBC connection URL for the watched database")
            .define(JDBC_USER, Type.STRING, Importance.HIGH, "Database user")
            .define(JDBC_PASSWORD, Type.PASSWORD, Importance.HIGH, "Database password")
            .define(TABLE_NAME, Type.STRING, Importance.HIGH, "Watched table name")
            .define(TABLE_ID_COLUMN, Type.STRING, "id", Importance.MEDIUM, "Primary key column used to sort and diff the table")
            .define(TOPIC, Type.STRING, Importance.HIGH, "Destination Kafka topic for detected change events")
            .define(POLL_INTERVAL_MS, Type.LONG, 5000L, Importance.MEDIUM, "Interval between sweeps of the watched table")
            .define(SCAN_FETCH_SIZE, Type.INT, 1000, Importance.LOW, "JDBC fetch size used when streaming the table scan")
            .define(CONSUME_BATCH_SIZE, Type.INT, 100, Importance.LOW, "Max outbox rows claimed per poll() call");

    public OutboxPollConnectorConfig(Map<String, String> props) {
        super(CONFIG_DEF, props);
    }

    public String jdbcUrl() {
        return getString(JDBC_URL);
    }

    public String jdbcUser() {
        return getString(JDBC_USER);
    }

    public String jdbcPassword() {
        return getPassword(JDBC_PASSWORD).value();
    }

    public String tableName() {
        return getString(TABLE_NAME);
    }

    public String tableIdColumn() {
        return getString(TABLE_ID_COLUMN);
    }

    public String topic() {
        return getString(TOPIC);
    }

    public long pollIntervalMs() {
        return getLong(POLL_INTERVAL_MS);
    }

    public int scanFetchSize() {
        return getInt(SCAN_FETCH_SIZE);
    }

    public int consumeBatchSize() {
        return getInt(CONSUME_BATCH_SIZE);
    }
}
