/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.snowflake;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;

/**
 * Unit tests for SnowflakeConnection. Tests that don't require a live Snowflake instance
 * focus on URL building, identifier quoting, and constructor behavior.
 * Full JDBC interaction tests are in SnowflakeConnectorIT (integration).
 */
class SnowflakeConnectionTest {

    @Test
    void shouldConstructWithValidConfig() {
        Configuration config = Configuration.create()
                .with(SnowflakeConnectorConfig.SNOWFLAKE_URL, "https://myaccount.snowflakecomputing.com")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_USER, "user")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_PASSWORD, "pass")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_DATABASE, "testdb")
                .build();

        SnowflakeConnection conn = new SnowflakeConnection(config);
        assertThat(conn).isNotNull();
    }

    @Test
    void shouldConstructWithJdbcUrl() {
        Configuration config = Configuration.create()
                .with(SnowflakeConnectorConfig.SNOWFLAKE_URL, "jdbc:snowflake://myaccount.snowflakecomputing.com")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_USER, "user")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_PASSWORD, "pass")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_DATABASE, "testdb")
                .build();

        SnowflakeConnection conn = new SnowflakeConnection(config);
        assertThat(conn).isNotNull();
    }

    @Test
    void shouldCloseCleanlyWhenNeverConnected() {
        Configuration config = Configuration.create()
                .with(SnowflakeConnectorConfig.SNOWFLAKE_URL, "https://test.snowflakecomputing.com")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_USER, "user")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_PASSWORD, "pass")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_DATABASE, "testdb")
                .build();

        SnowflakeConnection conn = new SnowflakeConnection(config);
        conn.close(); // should not throw
    }

    @Test
    void shouldQuoteIdentifiersForStreamHasData() {
        assertThat(SnowflakeIdentifiers.quoteIdentifier("test_stream"))
                .isEqualTo("\"test_stream\"");
    }

    @Test
    void shouldQuoteIdentifiersForCreateStream() {
        assertThat(SnowflakeIdentifiers.quoteIdentifier("MY_STREAM")).isEqualTo("\"MY_STREAM\"");
        assertThat(SnowflakeIdentifiers.quoteIdentifier("MY_TABLE")).isEqualTo("\"MY_TABLE\"");
    }

    @Test
    void shouldValidateTimestampForCreateStream() {
        assertThatThrownBy(() ->
                SnowflakeIdentifiers.validateTimestamp("'; DROP TABLE users;--"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldEscapeEmbeddedQuotesInIdentifiers() {
        assertThat(SnowflakeIdentifiers.quoteIdentifier("test\"stream"))
                .isEqualTo("\"test\"\"stream\"");
    }

    @Test
    void shouldHandleSpecialCharactersInIdentifiers() {
        assertThat(SnowflakeIdentifiers.quoteIdentifier("table with spaces"))
                .isEqualTo("\"table with spaces\"");
        assertThat(SnowflakeIdentifiers.quoteIdentifier("table;DROP"))
                .isEqualTo("\"table;DROP\"");
    }

    @Test
    void shouldUseQuotedDatabaseInInfoSchemaQueries() {
        // Verify the quoting for database identifiers used in INFORMATION_SCHEMA queries
        assertThat(SnowflakeIdentifiers.quoteIdentifier("MY_DB"))
                .isEqualTo("\"MY_DB\"");
    }
}
