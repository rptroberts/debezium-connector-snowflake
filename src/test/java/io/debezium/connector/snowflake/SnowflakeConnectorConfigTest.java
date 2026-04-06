/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.snowflake;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;

class SnowflakeConnectorConfigTest {

    @Test
    void shouldHaveDefaultCdcMode() {
        Configuration config = Configuration.create()
                .with("topic.prefix", "test")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_URL, "https://test.snowflakecomputing.com")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_USER, "user")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_PASSWORD, "pass")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_DATABASE, "testdb")
                .build();

        SnowflakeConnectorConfig connectorConfig = new SnowflakeConnectorConfig(config);
        assertThat(connectorConfig.getCdcMode()).isEqualTo(SnowflakeConnectorConfig.CdcMode.STREAM);
    }

    @Test
    void shouldParseChangesCdcMode() {
        Configuration config = Configuration.create()
                .with("topic.prefix", "test")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_URL, "https://test.snowflakecomputing.com")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_USER, "user")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_PASSWORD, "pass")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_DATABASE, "testdb")
                .with(SnowflakeConnectorConfig.CDC_MODE, "changes")
                .build();

        SnowflakeConnectorConfig connectorConfig = new SnowflakeConnectorConfig(config);
        assertThat(connectorConfig.getCdcMode()).isEqualTo(SnowflakeConnectorConfig.CdcMode.CHANGES);
    }

    @Test
    void shouldHaveDefaultSnapshotMode() {
        Configuration config = Configuration.create()
                .with("topic.prefix", "test")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_URL, "https://test.snowflakecomputing.com")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_USER, "user")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_PASSWORD, "pass")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_DATABASE, "testdb")
                .build();

        SnowflakeConnectorConfig connectorConfig = new SnowflakeConnectorConfig(config);
        assertThat(connectorConfig.getSnapshotModeConfig()).isEqualTo(SnowflakeConnectorConfig.SnapshotMode.INITIAL);
    }

    @Test
    void shouldReturnDefaultSchema() {
        Configuration config = Configuration.create()
                .with("topic.prefix", "test")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_URL, "https://test.snowflakecomputing.com")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_USER, "user")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_PASSWORD, "pass")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_DATABASE, "testdb")
                .build();

        SnowflakeConnectorConfig connectorConfig = new SnowflakeConnectorConfig(config);
        assertThat(connectorConfig.getSnowflakeSchema()).isEqualTo("PUBLIC");
    }

    @Test
    void shouldReturnConnectionProperties() {
        Configuration config = Configuration.create()
                .with("topic.prefix", "test")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_URL, "https://myaccount.snowflakecomputing.com")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_USER, "testuser")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_PASSWORD, "testpass")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_DATABASE, "mydb")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_SCHEMA, "MYSCHEMA")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_WAREHOUSE, "COMPUTE_WH")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_ROLE, "SYSADMIN")
                .build();

        SnowflakeConnectorConfig connectorConfig = new SnowflakeConnectorConfig(config);
        assertThat(connectorConfig.getSnowflakeUrl()).isEqualTo("https://myaccount.snowflakecomputing.com");
        assertThat(connectorConfig.getSnowflakeUser()).isEqualTo("testuser");
        assertThat(connectorConfig.getSnowflakePassword()).isEqualTo("testpass");
        assertThat(connectorConfig.getSnowflakeDatabase()).isEqualTo("mydb");
        assertThat(connectorConfig.getSnowflakeSchema()).isEqualTo("MYSCHEMA");
        assertThat(connectorConfig.getSnowflakeWarehouse()).isEqualTo("COMPUTE_WH");
        assertThat(connectorConfig.getSnowflakeRole()).isEqualTo("SYSADMIN");
    }

    @Test
    void shouldReturnDefaultStreamSettings() {
        Configuration config = Configuration.create()
                .with("topic.prefix", "test")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_URL, "https://test.snowflakecomputing.com")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_USER, "user")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_PASSWORD, "pass")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_DATABASE, "testdb")
                .build();

        SnowflakeConnectorConfig connectorConfig = new SnowflakeConnectorConfig(config);
        assertThat(connectorConfig.getStreamPrefix()).isEqualTo("_DBZ_STREAM_");
        assertThat(connectorConfig.getStreamType()).isEqualTo("STANDARD");
        assertThat(connectorConfig.getStreamMaxRowsPerPoll()).isEqualTo(10000);
        assertThat(connectorConfig.getStaleHandlingMode()).isEqualTo(SnowflakeConnectorConfig.StaleHandlingMode.FAIL);
        assertThat(connectorConfig.getStaleWarningDays()).isEqualTo(2);
    }

    @Test
    void shouldParseStaleHandlingMode() {
        assertThat(SnowflakeConnectorConfig.StaleHandlingMode.parse("fail"))
                .isEqualTo(SnowflakeConnectorConfig.StaleHandlingMode.FAIL);
        assertThat(SnowflakeConnectorConfig.StaleHandlingMode.parse("recreate"))
                .isEqualTo(SnowflakeConnectorConfig.StaleHandlingMode.RECREATE);
        assertThat(SnowflakeConnectorConfig.StaleHandlingMode.parse("skip"))
                .isEqualTo(SnowflakeConnectorConfig.StaleHandlingMode.SKIP);
        assertThat(SnowflakeConnectorConfig.StaleHandlingMode.parse("unknown"))
                .isEqualTo(SnowflakeConnectorConfig.StaleHandlingMode.FAIL);
    }
}
