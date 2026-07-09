package io.debezium.connector.outboxpoll;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Each task instance is a stateless replica: it owns its own connections, its
 * own resident baseline (inside {@link SweepEngine}), and independently
 * claims outbox rows. Nothing here is shared or coordinated across tasks.
 */
public final class OutboxPollSourceTask extends SourceTask {

    private static final Logger LOG = LoggerFactory.getLogger(OutboxPollSourceTask.class);
    private static final long EMPTY_POLL_BACKOFF_MS = 200L;

    private Connection sweepConnection;
    private Connection consumeConnection;
    private PollingScheduler pollingScheduler;
    private OutboxConsumer consumer;
    private String topic;
    private int consumeBatchSize;

    @Override
    public String version() {
        return "0.1.0-SNAPSHOT";
    }

    @Override
    public void start(Map<String, String> props) {
        OutboxPollConnectorConfig config = new OutboxPollConnectorConfig(props);
        this.topic = config.topic();
        this.consumeBatchSize = config.consumeBatchSize();

        try {
            this.sweepConnection = DriverManager.getConnection(config.jdbcUrl(), config.jdbcUser(), config.jdbcPassword());
            this.consumeConnection = DriverManager.getConnection(config.jdbcUrl(), config.jdbcUser(), config.jdbcPassword());
            this.consumer = new OutboxConsumer(consumeConnection);
        }
        catch (SQLException e) {
            throw new IllegalStateException("Failed to connect to " + config.jdbcUrl(), e);
        }

        SweepEngine sweepEngine = new SweepEngine(sweepConnection, config.tableName(), config.tableIdColumn(), config.scanFetchSize());
        this.pollingScheduler = new PollingScheduler(sweepEngine, config.pollIntervalMs());
        this.pollingScheduler.start();
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<OutboxConsumer.OutboxRow> claimed;
        try {
            claimed = consumer.claimBatch(consumeBatchSize);
        }
        catch (SQLException e) {
            throw new IllegalStateException("Failed to claim outbox rows", e);
        }

        if (claimed.isEmpty()) {
            Thread.sleep(EMPTY_POLL_BACKOFF_MS);
            return List.of();
        }

        List<SourceRecord> records = new ArrayList<>(claimed.size());
        for (OutboxConsumer.OutboxRow row : claimed) {
            Map<String, ?> sourcePartition = Map.of("table", topic);
            Map<String, ?> sourceOffset = Map.of("outbox_id", row.id());
            SourceRecord record = new SourceRecord(
                    sourcePartition, sourceOffset, topic,
                    Schema.INT64_SCHEMA, row.rowId(),
                    Schema.STRING_SCHEMA, row.payload());
            record.headers().addString("event_type", row.eventType());
            record.headers().addString("idempotency_key", row.idempotencyKey());
            records.add(record);
        }
        return records;
    }

    @Override
    public void stop() {
        if (pollingScheduler != null) {
            pollingScheduler.close();
        }
        closeQuietly(sweepConnection);
        closeQuietly(consumeConnection);
    }

    private static void closeQuietly(Connection connection) {
        if (connection == null) {
            return;
        }
        try {
            connection.close();
        }
        catch (SQLException e) {
            LOG.warn("Failed to close connection cleanly", e);
        }
    }
}
