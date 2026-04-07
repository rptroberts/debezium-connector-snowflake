/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.snowflake;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

class SnowflakeConnectorTest {

    @Test
    void shouldReturnCorrectTaskClass() {
        SnowflakeConnector connector = new SnowflakeConnector();
        assertThat(connector.taskClass()).isEqualTo(SnowflakeConnectorTask.class);
    }

    @Test
    void shouldReturnNonNullConfigDef() {
        SnowflakeConnector connector = new SnowflakeConnector();
        assertThat(connector.config()).isNotNull();
    }

    @Test
    void shouldReturnSingleTaskConfig() {
        SnowflakeConnector connector = new SnowflakeConnector();
        Map<String, String> props = validProps();
        connector.start(props);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(10);
        assertThat(taskConfigs).hasSize(1);
        assertThat(taskConfigs.get(0)).containsEntry("snowflake.url", "https://test.snowflakecomputing.com");
    }

    @Test
    void shouldRejectMissingUrl() {
        SnowflakeConnector connector = new SnowflakeConnector();
        Map<String, String> props = validProps();
        props.remove("snowflake.url");
        assertThatThrownBy(() -> connector.start(props))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("snowflake.url");
    }

    @Test
    void shouldRejectMissingAuth() {
        SnowflakeConnector connector = new SnowflakeConnector();
        Map<String, String> props = validProps();
        props.remove("snowflake.password");
        assertThatThrownBy(() -> connector.start(props))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("authentication");
    }

    @Test
    void shouldRejectConflictingIncludeExcludeLists() {
        SnowflakeConnector connector = new SnowflakeConnector();
        Map<String, String> props = validProps();
        props.put("table.include.list", "TABLE1");
        props.put("table.exclude.list", "TABLE2");
        assertThatThrownBy(() -> connector.start(props))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("table.include.list and table.exclude.list");
    }

    @Test
    void shouldAcceptPrivateKeyAsAuth() {
        SnowflakeConnector connector = new SnowflakeConnector();
        Map<String, String> props = validProps();
        props.remove("snowflake.password");
        props.put("snowflake.private.key", "MIIEvgIBADANBg...");
        connector.start(props); // should not throw
        connector.stop();
    }

    @Test
    void shouldReturnVersion() {
        SnowflakeConnector connector = new SnowflakeConnector();
        assertThat(connector.version()).isNotNull();
    }

    private Map<String, String> validProps() {
        Map<String, String> props = new HashMap<>();
        props.put("topic.prefix", "test");
        props.put("snowflake.url", "https://test.snowflakecomputing.com");
        props.put("snowflake.user", "testuser");
        props.put("snowflake.password", "testpass");
        props.put("snowflake.database", "testdb");
        return props;
    }
}
