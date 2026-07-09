package io.debezium.connector.outboxpoll;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

/**
 * Every task is an identical, stateless replica of the same config -- there
 * is no partitioning of the watched table between tasks. Each task
 * independently scans, diffs, and races the others for outbox rows via
 * FOR UPDATE SKIP LOCKED, per DDD-53's horizontal-scaling model.
 */
public final class OutboxPollSourceConnector extends SourceConnector {

    private Map<String, String> configProps;

    @Override
    public String version() {
        return "0.1.0-SNAPSHOT";
    }

    @Override
    public void start(Map<String, String> props) {
        this.configProps = props;
        new OutboxPollConnectorConfig(props); // validates eagerly
    }

    @Override
    public Class<? extends Task> taskClass() {
        return OutboxPollSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            configs.add(configProps);
        }
        return configs;
    }

    @Override
    public void stop() {
        // no connector-level resources held -- each task owns its own connection
    }

    @Override
    public ConfigDef config() {
        return OutboxPollConnectorConfig.CONFIG_DEF;
    }
}
