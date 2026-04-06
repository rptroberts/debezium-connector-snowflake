/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.snowflake;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;

/**
 * Integration tests requiring a real Snowflake account.
 *
 * Required environment variables:
 * - SNOWFLAKE_URL: Snowflake account URL
 * - SNOWFLAKE_USER: Snowflake username
 * - SNOWFLAKE_PASSWORD: Snowflake password
 * - SNOWFLAKE_DATABASE: Test database name
 * - SNOWFLAKE_SCHEMA: Test schema name (default: PUBLIC)
 * - SNOWFLAKE_WAREHOUSE: Snowflake warehouse
 */
@Tag("integration")
class SnowflakeConnectorIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeConnectorIT.class);

    private static final String TEST_TABLE = "DBZ_TEST_CDC";

    private static String snowflakeUrl;
    private static String snowflakeUser;
    private static String snowflakePassword;
    private static String snowflakeDatabase;
    private static String snowflakeSchema;
    private static String snowflakeWarehouse;

    private Connection directConnection;
    private SnowflakeConnection connectorConnection;

    @BeforeAll
    static void checkEnvironment() {
        snowflakeUrl = System.getenv("SNOWFLAKE_URL");
        snowflakeUser = System.getenv("SNOWFLAKE_USER");
        snowflakePassword = System.getenv("SNOWFLAKE_PASSWORD");
        snowflakeDatabase = System.getenv("SNOWFLAKE_DATABASE");
        snowflakeSchema = System.getenv().getOrDefault("SNOWFLAKE_SCHEMA", "PUBLIC");
        snowflakeWarehouse = System.getenv("SNOWFLAKE_WAREHOUSE");

        org.junit.jupiter.api.Assumptions.assumeTrue(
                snowflakeUrl != null && !snowflakeUrl.isEmpty(),
                "Snowflake connection details not provided. Set SNOWFLAKE_URL environment variable.");
    }

    @BeforeEach
    void setUp() throws Exception {
        // Direct JDBC connection for test setup
        Properties props = new Properties();
        props.put("user", snowflakeUser);
        props.put("password", snowflakePassword);
        props.put("db", snowflakeDatabase);
        props.put("schema", snowflakeSchema);
        if (snowflakeWarehouse != null) {
            props.put("warehouse", snowflakeWarehouse);
        }

        String jdbcUrl = snowflakeUrl.startsWith("jdbc:") ? snowflakeUrl
                : "jdbc:snowflake://" + snowflakeUrl.replace("https://", "").replace("http://", "");
        directConnection = DriverManager.getConnection(jdbcUrl, props);

        // Create test table
        try (Statement stmt = directConnection.createStatement()) {
            stmt.execute("CREATE OR REPLACE TABLE " + TEST_TABLE + " (" +
                    "ID NUMBER(10,0) NOT NULL, " +
                    "NAME VARCHAR(100), " +
                    "VALUE NUMBER(10,2), " +
                    "ACTIVE BOOLEAN, " +
                    "PRIMARY KEY (ID))");
        }

        // Create connector connection
        Configuration config = buildConfig();
        connectorConnection = new SnowflakeConnection(config);
        connectorConnection.connect();
    }

    @AfterEach
    void tearDown() throws Exception {
        // Clean up streams
        try (Statement stmt = directConnection.createStatement()) {
            stmt.execute("DROP STREAM IF EXISTS _DBZ_STREAM_" + TEST_TABLE);
            stmt.execute("DROP TABLE IF EXISTS " + TEST_TABLE);
        }
        catch (Exception e) {
            LOGGER.warn("Cleanup failed", e);
        }

        if (connectorConnection != null) {
            connectorConnection.close();
        }
        if (directConnection != null) {
            directConnection.close();
        }
    }

    @Test
    void shouldConnectToSnowflake() throws Exception {
        assertThat(connectorConnection.getJdbcConnection().isClosed()).isFalse();
    }

    @Test
    void shouldDiscoverTableColumns() throws Exception {
        List<java.util.Map<String, Object>> columns = connectorConnection.getTableColumns(
                snowflakeDatabase, snowflakeSchema, TEST_TABLE);

        assertThat(columns).isNotEmpty();
        assertThat(columns.size()).isGreaterThanOrEqualTo(4);

        List<String> columnNames = new ArrayList<>();
        for (java.util.Map<String, Object> col : columns) {
            columnNames.add(String.valueOf(col.get("COLUMN_NAME")));
        }
        assertThat(columnNames).contains("ID", "NAME", "VALUE", "ACTIVE");
    }

    @Test
    void shouldCreateAndCheckStream() throws Exception {
        String streamName = "_DBZ_STREAM_" + TEST_TABLE;
        connectorConnection.createStream(streamName,
                snowflakeSchema + "." + TEST_TABLE, "STANDARD", null);

        // Insert data
        try (Statement stmt = directConnection.createStatement()) {
            stmt.execute("INSERT INTO " + TEST_TABLE + " VALUES (1, 'Alice', 100.50, TRUE)");
        }

        // Check stream has data
        boolean hasData = connectorConnection.streamHasData(streamName);
        assertThat(hasData).isTrue();
    }

    @Test
    void shouldConsumeStreamChanges() throws Exception {
        String streamName = "_DBZ_STREAM_" + TEST_TABLE;
        connectorConnection.createStream(streamName,
                snowflakeSchema + "." + TEST_TABLE, "STANDARD", null);

        // Insert test data
        try (Statement stmt = directConnection.createStatement()) {
            stmt.execute("INSERT INTO " + TEST_TABLE + " VALUES (1, 'Alice', 100.50, TRUE)");
            stmt.execute("INSERT INTO " + TEST_TABLE + " VALUES (2, 'Bob', 200.75, FALSE)");
        }

        // Consume via temp table
        List<java.util.Map<String, Object>> changes = connectorConnection.consumeStreamViaTemp(
                streamName, "_DBZ_CHANGES_TEST_" + System.currentTimeMillis(), 1000);

        assertThat(changes).hasSize(2);

        // Verify metadata columns present
        java.util.Map<String, Object> firstRow = changes.get(0);
        assertThat(firstRow).containsKey("METADATA$ACTION");
        assertThat(String.valueOf(firstRow.get("METADATA$ACTION"))).isEqualTo("INSERT");
    }

    @Test
    void shouldDetectUpdatesAsDeleteInsertPairs() throws Exception {
        String streamName = "_DBZ_STREAM_" + TEST_TABLE;
        connectorConnection.createStream(streamName,
                snowflakeSchema + "." + TEST_TABLE, "STANDARD", null);

        // Insert then update
        try (Statement stmt = directConnection.createStatement()) {
            stmt.execute("INSERT INTO " + TEST_TABLE + " VALUES (1, 'Alice', 100.50, TRUE)");
        }

        // Consume the insert first
        connectorConnection.consumeStreamViaTemp(streamName,
                "_DBZ_CHANGES_CONSUME_" + System.currentTimeMillis(), 1000);

        // Now update
        try (Statement stmt = directConnection.createStatement()) {
            stmt.execute("UPDATE " + TEST_TABLE + " SET NAME = 'Alice Updated', VALUE = 150.00 WHERE ID = 1");
        }

        // Consume the update
        List<java.util.Map<String, Object>> changes = connectorConnection.consumeStreamViaTemp(
                streamName, "_DBZ_CHANGES_UPD_" + System.currentTimeMillis(), 1000);

        assertThat(changes).hasSize(2); // DELETE + INSERT pair

        long deleteCount = changes.stream()
                .filter(r -> "DELETE".equalsIgnoreCase(String.valueOf(r.get("METADATA$ACTION"))))
                .count();
        long insertCount = changes.stream()
                .filter(r -> "INSERT".equalsIgnoreCase(String.valueOf(r.get("METADATA$ACTION"))))
                .count();

        assertThat(deleteCount).isEqualTo(1);
        assertThat(insertCount).isEqualTo(1);

        // Both should have ISUPDATE = true
        for (java.util.Map<String, Object> row : changes) {
            assertThat(String.valueOf(row.get("METADATA$ISUPDATE")).toLowerCase()).isIn("true", "1");
        }
    }

    @Test
    void shouldGetCurrentTimestamp() throws Exception {
        String timestamp = connectorConnection.getCurrentTimestamp();
        assertThat(timestamp).isNotNull();
        assertThat(timestamp).isNotEmpty();
        LOGGER.info("Current Snowflake timestamp: {}", timestamp);
    }

    private Configuration buildConfig() {
        return Configuration.create()
                .with("topic.prefix", "test-it")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_URL, snowflakeUrl)
                .with(SnowflakeConnectorConfig.SNOWFLAKE_USER, snowflakeUser)
                .with(SnowflakeConnectorConfig.SNOWFLAKE_PASSWORD, snowflakePassword)
                .with(SnowflakeConnectorConfig.SNOWFLAKE_DATABASE, snowflakeDatabase)
                .with(SnowflakeConnectorConfig.SNOWFLAKE_SCHEMA, snowflakeSchema)
                .with(SnowflakeConnectorConfig.SNOWFLAKE_WAREHOUSE, snowflakeWarehouse != null ? snowflakeWarehouse : "")
                .build();
    }
}
