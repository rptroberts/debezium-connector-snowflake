/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.snowflake;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;

public class SnowflakeConnectorTask extends SourceTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeConnectorTask.class);

    private SnowflakeConnectorConfig connectorConfig;
    private SnowflakeConnection connection;
    private SnowflakeChangeEventSourceCoordinator coordinator;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(Map<String, String> props) {
        LOGGER.info("Starting Snowflake connector task");
        Configuration config = Configuration.from(props);
        this.connectorConfig = new SnowflakeConnectorConfig(config);

        this.connection = new SnowflakeConnection(config);
        try {
            connection.connect();

            this.coordinator = new SnowflakeChangeEventSourceCoordinator(
                    connectorConfig,
                    connection,
                    context);
            coordinator.start();
        }
        catch (Exception e) {
            // Clean up connection if initialization fails partway through
            if (connection != null) {
                connection.close();
                connection = null;
            }
            throw new ConnectException("Failed to start Snowflake connector task", e);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        return coordinator.poll();
    }

    @Override
    public void stop() {
        LOGGER.info("Stopping Snowflake connector task");
        if (coordinator != null) {
            coordinator.stop();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
