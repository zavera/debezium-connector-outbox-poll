package io.debezium.connector.outboxpoll;

import java.util.Properties;

/** Plain config for the standalone agent -- no Kafka Connect ConfigDef involved. */
public record AgentConfig(
        String jdbcUrl,
        String jdbcUser,
        String jdbcPassword,
        String tableName,
        String tableIdColumn,
        long pollIntervalMs,
        int scanFetchSize) {

    public static AgentConfig fromProperties(Properties props) {
        return new AgentConfig(
                require(props, "jdbc.url"),
                require(props, "jdbc.user"),
                props.getProperty("jdbc.password", ""),
                require(props, "table.name"),
                props.getProperty("table.id.column", "id"),
                Long.parseLong(props.getProperty("poll.interval.ms", "5000")),
                Integer.parseInt(props.getProperty("scan.fetch.size", "1000")));
    }

    private static String require(Properties props, String key) {
        String value = props.getProperty(key);
        if (value == null) {
            throw new IllegalArgumentException("Missing required property: " + key);
        }
        return value;
    }
}
