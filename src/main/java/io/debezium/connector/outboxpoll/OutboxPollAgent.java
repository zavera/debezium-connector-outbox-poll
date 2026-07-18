package io.debezium.connector.outboxpoll;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standalone JVM process. No Kafka, no broker, no Connect framework -- it
 * runs the sweep loop directly against the database and writes detected
 * changes to debezium_outbox. That table is the entire interface: downstream
 * consumers read it on their own terms, however they choose to.
 */
public final class OutboxPollAgent {

    private static final Logger LOG = LoggerFactory.getLogger(OutboxPollAgent.class);

    private final Connection connection;
    private final PollingScheduler scheduler;

    public OutboxPollAgent(AgentConfig config) throws SQLException {
        this.connection = DriverManager.getConnection(config.jdbcUrl(), config.jdbcUser(), config.jdbcPassword());
        SweepEngine sweepEngine = new SweepEngine(connection, config.tableName(), config.tableIdColumn(), config.scanFetchSize(), config.outboxRetentionDays());
        this.scheduler = new PollingScheduler(sweepEngine, config.pollIntervalMs());
    }

    public void start() {
        scheduler.start();
    }

    public void stop() {
        scheduler.close();
        try {
            connection.close();
        }
        catch (SQLException e) {
            LOG.warn("Failed to close connection cleanly", e);
        }
    }

    public static void main(String[] args) throws IOException, SQLException, InterruptedException {
        if (args.length != 1) {
            System.err.println("Usage: OutboxPollAgent <config.properties>");
            System.exit(1);
        }
        Properties props = new Properties();
        try (var in = Files.newInputStream(Path.of(args[0]))) {
            props.load(in);
        }

        OutboxPollAgent agent = new OutboxPollAgent(AgentConfig.fromProperties(props));
        agent.start();

        CountDownLatch shutdownLatch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            agent.stop();
            shutdownLatch.countDown();
        }));
        shutdownLatch.await();
    }
}
