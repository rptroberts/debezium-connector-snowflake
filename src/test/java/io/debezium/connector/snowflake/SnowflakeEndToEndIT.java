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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;

/**
 * End-to-end integration tests that exercise the full connector pipeline:
 * sample data creation, snapshot, streaming CDC (inserts, updates, deletes),
 * Debezium envelope validation, and both CDC modes.
 */
@Tag("integration")
class SnowflakeEndToEndIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeEndToEndIT.class);

    private static final String CUSTOMERS_TABLE = "DBZ_E2E_CUSTOMERS";
    private static final String ORDERS_TABLE = "DBZ_E2E_ORDERS";

    private static String snowflakeUrl;
    private static String snowflakeUser;
    private static String snowflakePassword;
    private static String snowflakeDatabase;
    private static String snowflakeSchema;
    private static String snowflakeWarehouse;

    private Connection directConnection;

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
        Properties props = new Properties();
        props.put("user", snowflakeUser);
        props.put("password", snowflakePassword);
        props.put("db", snowflakeDatabase);
        props.put("schema", snowflakeSchema);
        if (snowflakeWarehouse != null) {
            props.put("warehouse", snowflakeWarehouse);
        }
        props.put("JDBC_QUERY_RESULT_FORMAT", "JSON");

        String jdbcUrl = snowflakeUrl.startsWith("jdbc:") ? snowflakeUrl
                : "jdbc:snowflake://" + snowflakeUrl.replace("https://", "").replace("http://", "");
        directConnection = DriverManager.getConnection(jdbcUrl, props);

        // Create sample tables with varied data types
        try (Statement stmt = directConnection.createStatement()) {
            stmt.execute("CREATE OR REPLACE TABLE " + CUSTOMERS_TABLE + " (" +
                    "ID NUMBER(10,0) NOT NULL, " +
                    "NAME VARCHAR(200), " +
                    "EMAIL VARCHAR(200), " +
                    "BALANCE NUMBER(12,2), " +
                    "ACTIVE BOOLEAN, " +
                    "METADATA VARIANT, " +
                    "PRIMARY KEY (ID))");

            stmt.execute("CREATE OR REPLACE TABLE " + ORDERS_TABLE + " (" +
                    "ORDER_ID NUMBER(10,0) NOT NULL, " +
                    "CUSTOMER_ID NUMBER(10,0), " +
                    "PRODUCT VARCHAR(200), " +
                    "QUANTITY NUMBER(10,0), " +
                    "PRICE NUMBER(10,2), " +
                    "STATUS VARCHAR(50), " +
                    "PRIMARY KEY (ORDER_ID))");

            // Insert sample data for snapshot (PARSE_JSON not allowed in VALUES clause)
            stmt.execute("INSERT INTO " + CUSTOMERS_TABLE +
                    " (ID, NAME, EMAIL, BALANCE, ACTIVE, METADATA) " +
                    "SELECT 1, 'Alice Johnson', 'alice@example.com', 1500.50, TRUE, PARSE_JSON('{\"tier\": \"gold\"}') " +
                    "UNION ALL SELECT 2, 'Bob Smith', 'bob@example.com', 2300.75, TRUE, PARSE_JSON('{\"tier\": \"silver\"}') " +
                    "UNION ALL SELECT 3, 'Charlie Brown', 'charlie@example.com', 500.00, FALSE, NULL");

            stmt.execute("INSERT INTO " + ORDERS_TABLE +
                    " (ORDER_ID, CUSTOMER_ID, PRODUCT, QUANTITY, PRICE, STATUS) VALUES " +
                    "(101, 1, 'Widget A', 3, 29.99, 'SHIPPED'), " +
                    "(102, 2, 'Gadget X', 2, 99.50, 'PENDING')");

            // Clean up old streams
            stmt.execute("DROP STREAM IF EXISTS _DBZ_STREAM_" + CUSTOMERS_TABLE);
            stmt.execute("DROP STREAM IF EXISTS _DBZ_STREAM_" + ORDERS_TABLE);
        }

        LOGGER.info("Sample data created: 3 customers, 2 orders");
    }

    @AfterEach
    void tearDown() throws Exception {
        try (Statement stmt = directConnection.createStatement()) {
            stmt.execute("DROP STREAM IF EXISTS _DBZ_STREAM_" + CUSTOMERS_TABLE);
            stmt.execute("DROP STREAM IF EXISTS _DBZ_STREAM_" + ORDERS_TABLE);
            stmt.execute("DROP TABLE IF EXISTS " + CUSTOMERS_TABLE);
            stmt.execute("DROP TABLE IF EXISTS " + ORDERS_TABLE);
        }
        catch (Exception e) {
            LOGGER.warn("Cleanup failed", e);
        }
        if (directConnection != null) {
            directConnection.close();
        }
    }

    /**
     * Tests the full snapshot → streaming pipeline in stream mode.
     * 1. Connector starts, snapshots existing rows (3 customers, 2 orders)
     * 2. DML operations happen (insert, update, delete)
     * 3. Connector captures streaming changes
     * 4. Validates Debezium envelope format for all event types
     */
    @Test
    void shouldSnapshotAndStreamChangesInStreamMode() throws Exception {
        SnowflakeConnectorTask task = new SnowflakeConnectorTask();

        Map<String, String> props = buildTaskProps("stream");
        props.put("table.include.list", CUSTOMERS_TABLE + "," + ORDERS_TABLE);

        // Use a mock SourceTaskContext with empty offsets (fresh start)
        task.initialize(createMockTaskContext());
        task.start(props);

        try {
            // --- Phase 1: Snapshot ---
            List<SourceRecord> allRecords = pollUntilCount(task, 5, 30);
            LOGGER.info("Snapshot produced {} records", allRecords.size());
            assertThat(allRecords).hasSize(5); // 3 customers + 2 orders

            // Validate snapshot records
            List<SourceRecord> customerSnapshots = filterByTopic(allRecords, CUSTOMERS_TABLE);
            List<SourceRecord> orderSnapshots = filterByTopic(allRecords, ORDERS_TABLE);
            assertThat(customerSnapshots).hasSize(3);
            assertThat(orderSnapshots).hasSize(2);

            // Validate Debezium envelope of first snapshot record
            for (SourceRecord record : customerSnapshots) {
                Struct envelope = (Struct) record.value();
                assertThat(envelope.getString("op")).isEqualTo("r");
                assertThat(envelope.getStruct("after")).isNotNull();
                assertThat(envelope.getStruct("before")).isNull();
                assertThat(envelope.getStruct("source")).isNotNull();
                assertThat(envelope.getInt64("ts_ms")).isGreaterThan(0);

                // Validate source block
                Struct source = envelope.getStruct("source");
                assertThat(source.getString("connector")).isEqualTo("snowflake");
                assertThat(source.getString("db")).isEqualTo(snowflakeDatabase);
                assertThat(source.getString("schema")).isEqualTo(snowflakeSchema);
            }

            // Validate one snapshot record has correct data
            Struct aliceEnvelope = findRecordByFieldValue(customerSnapshots, "ID", 1);
            assertThat(aliceEnvelope).isNotNull();
            Struct aliceAfter = aliceEnvelope.getStruct("after");
            assertThat(aliceAfter.get("NAME")).isEqualTo("Alice Johnson");
            assertThat(aliceAfter.get("EMAIL")).isEqualTo("alice@example.com");
            assertThat(aliceAfter.get("ACTIVE")).isEqualTo(true);

            LOGGER.info("Snapshot validation passed");

            // --- Phase 2: Streaming CDC ---
            // Wait a moment for streams to be ready
            Thread.sleep(2000);

            // INSERT a new customer
            try (Statement stmt = directConnection.createStatement()) {
                stmt.execute("INSERT INTO " + CUSTOMERS_TABLE +
                        " (ID, NAME, EMAIL, BALANCE, ACTIVE) VALUES " +
                        "(4, 'Diana Prince', 'diana@example.com', 8900.25, TRUE)");
            }
            LOGGER.info("Inserted new customer (ID=4)");

            List<SourceRecord> insertRecords = pollUntilCount(task, 1, 30);
            assertThat(insertRecords).hasSize(1);
            Struct insertEnvelope = (Struct) insertRecords.get(0).value();
            assertThat(insertEnvelope.getString("op")).isEqualTo("c");
            assertThat(insertEnvelope.getStruct("after")).isNotNull();
            assertThat(insertEnvelope.getStruct("before")).isNull();
            assertThat(insertEnvelope.getStruct("after").get("NAME")).isEqualTo("Diana Prince");
            LOGGER.info("INSERT CDC validation passed");

            // UPDATE a customer
            try (Statement stmt = directConnection.createStatement()) {
                stmt.execute("UPDATE " + CUSTOMERS_TABLE +
                        " SET NAME = 'Alice Updated', BALANCE = 2000.00 WHERE ID = 1");
            }
            LOGGER.info("Updated customer (ID=1)");

            List<SourceRecord> updateRecords = pollUntilCount(task, 1, 30);
            assertThat(updateRecords).hasSize(1);
            Struct updateEnvelope = (Struct) updateRecords.get(0).value();
            assertThat(updateEnvelope.getString("op")).isEqualTo("u");
            assertThat(updateEnvelope.getStruct("before")).isNotNull();
            assertThat(updateEnvelope.getStruct("after")).isNotNull();
            // before should have old name
            assertThat(updateEnvelope.getStruct("before").get("NAME")).isEqualTo("Alice Johnson");
            // after should have new name
            assertThat(updateEnvelope.getStruct("after").get("NAME")).isEqualTo("Alice Updated");
            LOGGER.info("UPDATE CDC validation passed (before/after reconstruction)");

            // DELETE a customer
            try (Statement stmt = directConnection.createStatement()) {
                stmt.execute("DELETE FROM " + CUSTOMERS_TABLE + " WHERE ID = 3");
            }
            LOGGER.info("Deleted customer (ID=3)");

            List<SourceRecord> deleteRecords = pollUntilCount(task, 1, 30);
            assertThat(deleteRecords).hasSize(1);
            Struct deleteEnvelope = (Struct) deleteRecords.get(0).value();
            assertThat(deleteEnvelope.getString("op")).isEqualTo("d");
            assertThat(deleteEnvelope.getStruct("before")).isNotNull();
            assertThat(deleteEnvelope.getStruct("after")).isNull();
            assertThat(deleteEnvelope.getStruct("before").get("NAME")).isEqualTo("Charlie Brown");
            LOGGER.info("DELETE CDC validation passed");

        }
        finally {
            task.stop();
        }
    }

    /**
     * Tests the CHANGES clause CDC mode end-to-end.
     * Verifies snapshot + streaming with timestamp-based change tracking.
     */
    @Test
    void shouldSnapshotAndStreamChangesInChangesMode() throws Exception {
        SnowflakeConnectorTask task = new SnowflakeConnectorTask();

        Map<String, String> props = buildTaskProps("changes");
        props.put("table.include.list", CUSTOMERS_TABLE);

        task.initialize(createMockTaskContext());
        task.start(props);

        try {
            // Snapshot: 3 customers
            List<SourceRecord> snapshots = pollUntilCount(task, 3, 30);
            assertThat(snapshots).hasSize(3);
            for (SourceRecord rec : snapshots) {
                assertThat(((Struct) rec.value()).getString("op")).isEqualTo("r");
            }
            LOGGER.info("Changes mode snapshot: {} records", snapshots.size());

            // Wait for changes mode to initialize timestamps
            Thread.sleep(3000);

            // INSERT
            try (Statement stmt = directConnection.createStatement()) {
                stmt.execute("INSERT INTO " + CUSTOMERS_TABLE +
                        " (ID, NAME, EMAIL, BALANCE, ACTIVE) VALUES " +
                        "(5, 'Eve Wilson', 'eve@example.com', 150.00, TRUE)");
            }

            List<SourceRecord> inserts = pollUntilCount(task, 1, 45);
            assertThat(inserts).hasSize(1);
            Struct insertVal = (Struct) inserts.get(0).value();
            assertThat(insertVal.getString("op")).isEqualTo("c");
            assertThat(insertVal.getStruct("after").get("NAME")).isEqualTo("Eve Wilson");
            LOGGER.info("Changes mode INSERT validation passed");
        }
        finally {
            task.stop();
        }
    }

    /**
     * Tests multi-table streaming: DML on different tables produces
     * correctly routed topic records.
     */
    @Test
    void shouldStreamMultiTableChanges() throws Exception {
        SnowflakeConnectorTask task = new SnowflakeConnectorTask();

        Map<String, String> props = buildTaskProps("stream");
        props.put("table.include.list", CUSTOMERS_TABLE + "," + ORDERS_TABLE);

        task.initialize(createMockTaskContext());
        task.start(props);

        try {
            // Consume snapshot
            List<SourceRecord> snapshots = pollUntilCount(task, 5, 30);
            assertThat(snapshots).hasSize(5);

            Thread.sleep(2000);

            // Insert into both tables
            try (Statement stmt = directConnection.createStatement()) {
                stmt.execute("INSERT INTO " + CUSTOMERS_TABLE +
                        " (ID, NAME, EMAIL, BALANCE, ACTIVE) VALUES " +
                        "(6, 'Frank Castle', 'frank@example.com', 999.99, TRUE)");
                stmt.execute("INSERT INTO " + ORDERS_TABLE +
                        " (ORDER_ID, CUSTOMER_ID, PRODUCT, QUANTITY, PRICE, STATUS) VALUES " +
                        "(103, 6, 'Gadget Z', 1, 199.00, 'NEW')");
            }

            List<SourceRecord> changes = pollUntilCount(task, 2, 30);
            assertThat(changes).hasSize(2);

            List<SourceRecord> custChanges = filterByTopic(changes, CUSTOMERS_TABLE);
            List<SourceRecord> orderChanges = filterByTopic(changes, ORDERS_TABLE);
            assertThat(custChanges).hasSize(1);
            assertThat(orderChanges).hasSize(1);

            assertThat(((Struct) custChanges.get(0).value()).getStruct("after").get("NAME"))
                    .isEqualTo("Frank Castle");
            assertThat(((Struct) orderChanges.get(0).value()).getStruct("after").get("PRODUCT"))
                    .isEqualTo("Gadget Z");

            LOGGER.info("Multi-table streaming validation passed");
        }
        finally {
            task.stop();
        }
    }

    /**
     * Tests that data types are correctly converted in the Debezium envelope:
     * NUMBER → INT32/INT64/Decimal, VARCHAR → STRING, BOOLEAN → BOOLEAN,
     * VARIANT → STRING (JSON).
     */
    @Test
    void shouldConvertDataTypesCorrectly() throws Exception {
        SnowflakeConnectorTask task = new SnowflakeConnectorTask();

        Map<String, String> props = buildTaskProps("stream");
        props.put("table.include.list", CUSTOMERS_TABLE);

        task.initialize(createMockTaskContext());
        task.start(props);

        try {
            List<SourceRecord> snapshots = pollUntilCount(task, 3, 30);

            // Find Alice's record (has VARIANT data)
            Struct aliceEnvelope = findRecordByFieldValue(snapshots, "ID", 1);
            assertThat(aliceEnvelope).isNotNull();
            Struct after = aliceEnvelope.getStruct("after");

            // NUMBER(10,0) → integer type
            Object idValue = after.get("ID");
            assertThat(idValue).isNotNull();
            LOGGER.info("ID type: {} value: {}", idValue.getClass().getSimpleName(), idValue);

            // VARCHAR → String
            assertThat(after.get("NAME")).isInstanceOf(String.class);
            assertThat(after.get("EMAIL")).isInstanceOf(String.class);

            // BOOLEAN → Boolean
            assertThat(after.get("ACTIVE")).isInstanceOf(Boolean.class);
            assertThat((Boolean) after.get("ACTIVE")).isTrue();

            // VARIANT → String (JSON)
            Object metadata = after.get("METADATA");
            if (metadata != null) {
                assertThat(metadata).isInstanceOf(String.class);
                assertThat((String) metadata).contains("gold");
                LOGGER.info("VARIANT value: {}", metadata);
            }

            // NUMBER(12,2) → numeric
            Object balance = after.get("BALANCE");
            assertThat(balance).isNotNull();
            LOGGER.info("BALANCE type: {} value: {}", balance.getClass().getSimpleName(), balance);

            // Check null handling (Charlie has null METADATA)
            Struct charlieEnvelope = findRecordByFieldValue(snapshots, "ID", 3);
            assertThat(charlieEnvelope).isNotNull();
            assertThat(charlieEnvelope.getStruct("after").get("METADATA")).isNull();

            LOGGER.info("Data type conversion validation passed");
        }
        finally {
            task.stop();
        }
    }

    /**
     * Tests that the source partition and offset are correctly populated,
     * enabling Kafka Connect to track progress.
     */
    @Test
    void shouldPopulateSourcePartitionAndOffset() throws Exception {
        SnowflakeConnectorTask task = new SnowflakeConnectorTask();

        Map<String, String> props = buildTaskProps("stream");
        props.put("table.include.list", CUSTOMERS_TABLE);

        task.initialize(createMockTaskContext());
        task.start(props);

        try {
            List<SourceRecord> snapshots = pollUntilCount(task, 3, 30);

            for (SourceRecord record : snapshots) {
                // Source partition should contain server name
                Map<String, ?> partition = record.sourcePartition();
                assertThat(partition).isNotNull();
                assertThat(partition).containsKey("server");
                assertThat(partition.get("server")).isEqualTo("e2e-test");

                // Source offset should contain snapshot state
                Map<String, ?> offset = record.sourceOffset();
                assertThat(offset).isNotNull();
                assertThat(offset).containsKey("snapshot_completed");

                // Topic should follow naming convention
                assertThat(record.topic()).contains("e2e-test");
                assertThat(record.topic()).contains(snowflakeDatabase);
                assertThat(record.topic()).contains(CUSTOMERS_TABLE);
            }

            LOGGER.info("Source partition and offset validation passed");
        }
        finally {
            task.stop();
        }
    }

    /**
     * Tests that boolean values are correctly converted (not corrupted by parseBoolean).
     * Verifies the fix for numeric boolean conversion (Bug 1D).
     */
    @Test
    void shouldProduceCorrectBooleanValues() throws Exception {
        SnowflakeConnectorTask task = new SnowflakeConnectorTask();

        Map<String, String> props = buildTaskProps("stream");
        props.put("table.include.list", CUSTOMERS_TABLE);

        task.initialize(createMockTaskContext());
        task.start(props);

        try {
            List<SourceRecord> snapshots = pollUntilCount(task, 3, 30);
            assertThat(snapshots).hasSize(3);

            // Alice (ID=1) has ACTIVE=TRUE
            Struct alice = findRecordByFieldValue(snapshots, "ID", 1);
            assertThat(alice).isNotNull();
            assertThat(alice.getStruct("after").get("ACTIVE")).isEqualTo(true);

            // Charlie (ID=3) has ACTIVE=FALSE
            Struct charlie = findRecordByFieldValue(snapshots, "ID", 3);
            assertThat(charlie).isNotNull();
            assertThat(charlie.getStruct("after").get("ACTIVE")).isEqualTo(false);

            LOGGER.info("Boolean value conversion validation passed");
        }
        finally {
            task.stop();
        }
    }

    /**
     * Tests that NUMBER(12,2) decimal values are preserved with exact precision.
     * Verifies the fix for nullable schema parameter loss (Bug 1A).
     */
    @Test
    void shouldProduceCorrectDecimalValues() throws Exception {
        SnowflakeConnectorTask task = new SnowflakeConnectorTask();

        Map<String, String> props = buildTaskProps("stream");
        props.put("table.include.list", CUSTOMERS_TABLE);

        task.initialize(createMockTaskContext());
        task.start(props);

        try {
            List<SourceRecord> snapshots = pollUntilCount(task, 3, 30);
            assertThat(snapshots).hasSize(3);

            // Alice (ID=1) has BALANCE=1500.50
            Struct alice = findRecordByFieldValue(snapshots, "ID", 1);
            assertThat(alice).isNotNull();
            Object balance = alice.getStruct("after").get("BALANCE");
            assertThat(balance).isNotNull();
            // Should be a BigDecimal or convertible to one with correct value
            LOGGER.info("BALANCE type: {}, value: {}", balance.getClass().getSimpleName(), balance);
            assertThat(new java.math.BigDecimal(balance.toString()))
                    .isEqualByComparingTo(new java.math.BigDecimal("1500.50"));

            LOGGER.info("Decimal value precision validation passed");
        }
        finally {
            task.stop();
        }
    }

    /**
     * Tests that the connector shuts down gracefully while actively streaming.
     * Verifies the fix for safe connection shutdown (Bug 3E).
     */
    @Test
    void shouldShutdownGracefullyUnderLoad() throws Exception {
        SnowflakeConnectorTask task = new SnowflakeConnectorTask();

        Map<String, String> props = buildTaskProps("stream");
        props.put("table.include.list", CUSTOMERS_TABLE + "," + ORDERS_TABLE);

        task.initialize(createMockTaskContext());
        task.start(props);

        try {
            // Consume snapshot
            pollUntilCount(task, 5, 30);

            // Insert some data to keep streams active
            try (Statement stmt = directConnection.createStatement()) {
                for (int i = 10; i < 20; i++) {
                    stmt.execute("INSERT INTO " + CUSTOMERS_TABLE +
                            " (ID, NAME, EMAIL, BALANCE, ACTIVE) VALUES " +
                            "(" + i + ", 'User " + i + "', 'user" + i + "@example.com', " + (i * 100) + ".00, TRUE)");
                }
            }

            Thread.sleep(1000);
        }
        finally {
            // This should not throw or hang
            task.stop();
            LOGGER.info("Graceful shutdown under load validation passed");
        }
    }

    /**
     * Tests that multi-table streaming doesn't lose data when tables have
     * different change rates. Verifies offset consistency (Bug 3C).
     */
    @Test
    void shouldHandleMultiTableOffsetConsistency() throws Exception {
        SnowflakeConnectorTask task = new SnowflakeConnectorTask();

        Map<String, String> props = buildTaskProps("stream");
        props.put("table.include.list", CUSTOMERS_TABLE + "," + ORDERS_TABLE);

        task.initialize(createMockTaskContext());
        task.start(props);

        try {
            // Consume snapshot (3 customers + 2 orders = 5)
            List<SourceRecord> snapshots = pollUntilCount(task, 5, 30);
            assertThat(snapshots).hasSize(5);

            Thread.sleep(2000);

            // Insert different amounts into each table
            try (Statement stmt = directConnection.createStatement()) {
                // 3 customers
                stmt.execute("INSERT INTO " + CUSTOMERS_TABLE +
                        " (ID, NAME, EMAIL, BALANCE, ACTIVE) VALUES " +
                        "(21, 'A1', 'a1@test.com', 100.00, TRUE)");
                stmt.execute("INSERT INTO " + CUSTOMERS_TABLE +
                        " (ID, NAME, EMAIL, BALANCE, ACTIVE) VALUES " +
                        "(22, 'A2', 'a2@test.com', 200.00, TRUE)");
                stmt.execute("INSERT INTO " + CUSTOMERS_TABLE +
                        " (ID, NAME, EMAIL, BALANCE, ACTIVE) VALUES " +
                        "(23, 'A3', 'a3@test.com', 300.00, FALSE)");
                // 1 order
                stmt.execute("INSERT INTO " + ORDERS_TABLE +
                        " (ORDER_ID, CUSTOMER_ID, PRODUCT, QUANTITY, PRICE, STATUS) VALUES " +
                        "(201, 21, 'Widget B', 5, 49.99, 'NEW')");
            }

            // Should get all 4 records
            List<SourceRecord> changes = pollUntilCount(task, 4, 30);
            assertThat(changes).hasSize(4);

            List<SourceRecord> custChanges = filterByTopic(changes, CUSTOMERS_TABLE);
            List<SourceRecord> orderChanges = filterByTopic(changes, ORDERS_TABLE);
            assertThat(custChanges).hasSize(3);
            assertThat(orderChanges).hasSize(1);

            LOGGER.info("Multi-table offset consistency validation passed");
        }
        finally {
            task.stop();
        }
    }

    // --- Helper methods ---

    private Map<String, String> buildTaskProps(String cdcMode) {
        Map<String, String> props = new HashMap<>();
        props.put("topic.prefix", "e2e-test");
        props.put(SnowflakeConnectorConfig.SNOWFLAKE_URL, snowflakeUrl);
        props.put(SnowflakeConnectorConfig.SNOWFLAKE_USER, snowflakeUser);
        props.put(SnowflakeConnectorConfig.SNOWFLAKE_PASSWORD, snowflakePassword);
        props.put(SnowflakeConnectorConfig.SNOWFLAKE_DATABASE, snowflakeDatabase);
        props.put(SnowflakeConnectorConfig.SNOWFLAKE_SCHEMA, snowflakeSchema);
        if (snowflakeWarehouse != null) {
            props.put(SnowflakeConnectorConfig.SNOWFLAKE_WAREHOUSE, snowflakeWarehouse);
        }
        props.put(SnowflakeConnectorConfig.CDC_MODE, cdcMode);
        props.put(SnowflakeConnectorConfig.SNAPSHOT_MODE, "initial");
        props.put(SnowflakeConnectorConfig.STREAM_MAX_ROWS_PER_POLL, "10000");
        props.put("poll.interval.ms", "2000");
        return props;
    }

    private SourceTaskContext createMockTaskContext() {
        return new SourceTaskContext() {
            @Override
            public Map<String, String> configs() {
                return new HashMap<>();
            }

            @Override
            public OffsetStorageReader offsetStorageReader() {
                return new OffsetStorageReader() {
                    @Override
                    public <T> Map<String, Object> offset(Map<String, T> partition) {
                        return null; // No existing offset — fresh start
                    }

                    @Override
                    public <T> Map<Map<String, T>, Map<String, Object>> offsets(
                            java.util.Collection<Map<String, T>> partitions) {
                        return new HashMap<>();
                    }
                };
            }
        };
    }

    /**
     * Polls the task repeatedly until the expected number of records are collected,
     * or the timeout (in seconds) is exceeded.
     */
    private List<SourceRecord> pollUntilCount(SnowflakeConnectorTask task, int expectedCount,
                                               int timeoutSeconds) throws InterruptedException {
        List<SourceRecord> collected = new ArrayList<>();
        long deadline = System.currentTimeMillis() + (timeoutSeconds * 1000L);

        while (collected.size() < expectedCount && System.currentTimeMillis() < deadline) {
            List<SourceRecord> batch = task.poll();
            if (batch != null && !batch.isEmpty()) {
                for (SourceRecord record : batch) {
                    // Skip internal marker records (e.g. snapshot completion)
                    if (record.value() == null || record.topic().endsWith(".snapshot_completion")) {
                        continue;
                    }
                    collected.add(record);
                }
                LOGGER.debug("Polled {} records (total: {})", batch.size(), collected.size());
            }
            else {
                Thread.sleep(500);
            }
        }

        return collected;
    }

    private List<SourceRecord> filterByTopic(List<SourceRecord> records, String tableName) {
        return records.stream()
                .filter(r -> r.topic().contains(tableName))
                .collect(Collectors.toList());
    }

    /**
     * Finds a record whose "after" struct contains the given field with the given value.
     * Returns the envelope Struct, or null if not found.
     */
    private Struct findRecordByFieldValue(List<SourceRecord> records, String fieldName, Object value) {
        for (SourceRecord record : records) {
            Struct envelope = (Struct) record.value();
            Struct after = envelope.getStruct("after");
            if (after != null) {
                Object fieldValue = after.get(fieldName);
                if (fieldValue != null && fieldValue.toString().equals(value.toString())) {
                    return envelope;
                }
            }
        }
        return null;
    }
}
