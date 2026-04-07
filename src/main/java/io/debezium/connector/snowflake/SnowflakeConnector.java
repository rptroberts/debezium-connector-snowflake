/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.snowflake;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

import io.debezium.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowflakeConnector extends SourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeConnector.class);

    private Map<String, String> configProperties;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(Map<String, String> props) {
        Configuration config = Configuration.from(props);
        List<String> errors = SnowflakeConnectorConfig.validateConfig(config);
        if (!errors.isEmpty()) {
            throw new ConnectException("Invalid configuration: " + String.join("; ", errors));
        }
        this.configProperties = Collections.unmodifiableMap(props);
        LOGGER.info("Starting Snowflake connector");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SnowflakeConnectorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        // Single-task only: Snowflake CDC streams cannot be parallelized across tasks
        return Collections.singletonList(configProperties);
    }

    @Override
    public void stop() {
        LOGGER.info("Stopping Snowflake connector");
    }

    @Override
    public ConfigDef config() {
        return SnowflakeConnectorConfig.configDef();
    }
}
