/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.snowflake;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowflakeChangeEventSourceCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeChangeEventSourceCoordinator.class);

    private final SnowflakeConnectorConfig config;
    private final SnowflakeConnection connection;
    private final SourceTaskContext taskContext;
    private final LinkedBlockingQueue<SourceRecord> queue;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread coordinatorThread;
    private SnowflakeOffsetContext offsetContext;

    public SnowflakeChangeEventSourceCoordinator(SnowflakeConnectorConfig config,
                                                  SnowflakeConnection connection,
                                                  SourceTaskContext taskContext) {
        this.config = config;
        this.connection = connection;
        this.taskContext = taskContext;
        this.queue = new LinkedBlockingQueue<>(config.getStreamMaxRowsPerPoll() * 2);
    }

    public void start() {
        // Load or create offset context
        this.offsetContext = loadOffsetContext();

        running.set(true);
        coordinatorThread = new Thread(this::run, "snowflake-coordinator");
        coordinatorThread.setDaemon(true);
        coordinatorThread.start();
        LOGGER.info("Snowflake change event source coordinator started");
    }

    private SnowflakeOffsetContext loadOffsetContext() {
        SnowflakePartition partition = new SnowflakePartition(config.getLogicalName());
        Map<String, Object> existingOffset = (Map<String, Object>) taskContext.offsetStorageReader()
                .offset(partition.getSourcePartition());

        if (existingOffset != null && !existingOffset.isEmpty()) {
            LOGGER.info("Loaded existing offset: {}", existingOffset);
            return new SnowflakeOffsetContext.Loader(config).load(existingOffset);
        }

        LOGGER.info("No existing offset found, starting fresh");
        return new SnowflakeOffsetContext(config, false, null, null);
    }

    private void run() {
        try {
            // Phase 1: Snapshot (if needed)
            if (shouldSnapshot()) {
                LOGGER.info("Starting snapshot phase");
                executeSnapshot();
                LOGGER.info("Snapshot phase completed");
            }

            // Phase 2: Streaming
            LOGGER.info("Starting streaming phase (mode: {})", config.getCdcMode());
            executeStreaming();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.info("Coordinator thread interrupted");
        }
        catch (Exception e) {
            LOGGER.error("Error in coordinator", e);
        }
    }

    private boolean shouldSnapshot() {
        switch (config.getSnapshotModeConfig()) {
            case INITIAL:
                return !offsetContext.isSnapshotCompleted();
            case ALWAYS:
                return true;
            case NEVER:
                return false;
            case WHEN_NEEDED:
                return !offsetContext.isSnapshotCompleted();
            default:
                return !offsetContext.isSnapshotCompleted();
        }
    }

    private void executeSnapshot() throws Exception {
        offsetContext.preSnapshotStart(true);
        List<String> tables = resolveTables();
        String snapshotTimestamp = connection.getCurrentTimestamp();

        for (String table : tables) {
            if (!running.get()) {
                break;
            }
            LOGGER.info("Snapshotting table: {}", table);
            snapshotTable(table, snapshotTimestamp);
        }

        // Create streams for streaming phase (if using stream mode)
        if (config.getCdcMode() == SnowflakeConnectorConfig.CdcMode.STREAM) {
            for (String table : tables) {
                String streamName = getStreamName(table);
                connection.createStream(streamName,
                        config.getSnowflakeSchema() + "." + table,
                        config.getStreamType(),
                        snapshotTimestamp);
            }
        }

        offsetContext.postSnapshotCompletion();
        offsetContext.setLastPollTimestamp(Instant.now());
    }

    private void snapshotTable(String tableName, String snapshotTimestamp) throws Exception {
        String fullTableName = config.getSnowflakeSchema() + "." + tableName;
        String sql = "SELECT * FROM " + fullTableName +
                " AT(TIMESTAMP => '" + snapshotTimestamp + "'::TIMESTAMP_LTZ)";

        List<Map<String, Object>> rows = connection.query(sql);
        Schema valueSchema = buildSchemaForTable(tableName);

        for (Map<String, Object> row : rows) {
            if (!running.get()) {
                break;
            }

            SourceRecord record = createSnapshotRecord(tableName, row, valueSchema);
            queue.put(record);
        }

        LOGGER.info("Snapshot of table {} complete: {} rows", tableName, rows.size());
    }

    private void executeStreaming() throws Exception {
        List<String> tables = resolveTables();
        long pollIntervalMs = config.getPollInterval().toMillis();

        while (running.get()) {
            boolean hasData = false;

            for (String table : tables) {
                if (!running.get()) {
                    break;
                }

                List<Map<String, Object>> changes;
                if (config.getCdcMode() == SnowflakeConnectorConfig.CdcMode.STREAM) {
                    changes = pollStreamMode(table);
                }
                else {
                    changes = pollChangesMode(table);
                }

                if (changes != null && !changes.isEmpty()) {
                    hasData = true;
                    processChanges(table, changes);
                    offsetContext.setLastPollTimestamp(Instant.now());
                }
            }

            if (!hasData) {
                Thread.sleep(pollIntervalMs);
            }
        }
    }

    private List<Map<String, Object>> pollStreamMode(String table) throws Exception {
        String streamName = getStreamName(table);

        if (!connection.streamHasData(streamName)) {
            return Collections.emptyList();
        }

        String tempTableName = "_DBZ_CHANGES_" + table.toUpperCase().replace(".", "_")
                + "_" + System.currentTimeMillis();

        return connection.consumeStreamViaTemp(streamName, tempTableName,
                config.getStreamMaxRowsPerPoll());
    }

    private List<Map<String, Object>> pollChangesMode(String table) throws Exception {
        String fullTableName = config.getSnowflakeSchema() + "." + table;
        String lastTimestamp = offsetContext.getStreamOffset(table);

        if (lastTimestamp == null) {
            lastTimestamp = offsetContext.getLastPollTimestamp() != null
                    ? offsetContext.getLastPollTimestamp().toString()
                    : connection.getCurrentTimestamp();
            offsetContext.updateStreamOffset(table, lastTimestamp);
            return Collections.emptyList();
        }

        String currentTimestamp = connection.getCurrentTimestamp();
        List<Map<String, Object>> changes = connection.queryChanges(
                fullTableName, lastTimestamp, currentTimestamp);

        if (!changes.isEmpty()) {
            offsetContext.updateStreamOffset(table, currentTimestamp);
        }

        return changes;
    }

    private void processChanges(String tableName, List<Map<String, Object>> changes) throws Exception {
        Schema valueSchema = buildSchemaForTable(tableName);

        // Group UPDATE pairs by METADATA$ROW_ID
        Map<String, List<Map<String, Object>>> updatePairs = new HashMap<>();
        List<Map<String, Object>> nonUpdateRows = new ArrayList<>();

        for (Map<String, Object> row : changes) {
            Boolean isUpdate = toBoolean(row.get("METADATA$ISUPDATE"));
            if (Boolean.TRUE.equals(isUpdate)) {
                String rowId = String.valueOf(row.get("METADATA$ROW_ID"));
                updatePairs.computeIfAbsent(rowId, k -> new ArrayList<>()).add(row);
            }
            else {
                nonUpdateRows.add(row);
            }
        }

        // Process inserts and deletes
        for (Map<String, Object> row : nonUpdateRows) {
            String action = String.valueOf(row.get("METADATA$ACTION"));
            SourceRecord record;
            if ("INSERT".equalsIgnoreCase(action)) {
                record = createChangeRecord(tableName, "c", null, row, valueSchema);
            }
            else {
                record = createChangeRecord(tableName, "d", row, null, valueSchema);
            }
            queue.put(record);
        }

        // Process updates (pair DELETE + INSERT with same ROW_ID)
        for (Map.Entry<String, List<Map<String, Object>>> entry : updatePairs.entrySet()) {
            List<Map<String, Object>> pairs = entry.getValue();
            Map<String, Object> beforeRow = null;
            Map<String, Object> afterRow = null;

            for (Map<String, Object> row : pairs) {
                String action = String.valueOf(row.get("METADATA$ACTION"));
                if ("DELETE".equalsIgnoreCase(action)) {
                    beforeRow = row;
                }
                else if ("INSERT".equalsIgnoreCase(action)) {
                    afterRow = row;
                }
            }

            SourceRecord record = createChangeRecord(tableName, "u", beforeRow, afterRow, valueSchema);
            queue.put(record);
        }
    }

    private Boolean toBoolean(Object value) {
        if (value == null) {
            return false;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return "true".equalsIgnoreCase(String.valueOf(value));
    }

    private List<String> resolveTables() {
        List<String> includeList = config.getTableIncludeList();
        if (includeList != null && !includeList.isEmpty()) {
            return includeList;
        }
        // If no include list, discover all tables
        try {
            List<Map<String, Object>> tables = connection.getTables(
                    config.getSnowflakeDatabase(), config.getSnowflakeSchema());
            List<String> tableNames = new ArrayList<>();
            for (Map<String, Object> row : tables) {
                tableNames.add(String.valueOf(row.get("TABLE_NAME")));
            }
            return tableNames;
        }
        catch (Exception e) {
            LOGGER.error("Failed to discover tables", e);
            return Collections.emptyList();
        }
    }

    private String getStreamName(String tableName) {
        return config.getStreamPrefix() + tableName.toUpperCase().replace(".", "_");
    }

    private Schema buildSchemaForTable(String tableName) {
        // Build a dynamic schema based on table columns
        try {
            List<Map<String, Object>> columns = connection.getTableColumns(
                    config.getSnowflakeDatabase(), config.getSnowflakeSchema(), tableName);

            SchemaBuilder builder = SchemaBuilder.struct()
                    .name(config.getLogicalName() + "." + config.getSnowflakeSchema() + "." + tableName + ".Value");

            for (Map<String, Object> col : columns) {
                String columnName = String.valueOf(col.get("COLUMN_NAME"));
                String dataType = String.valueOf(col.get("DATA_TYPE")).toUpperCase();
                boolean nullable = "YES".equalsIgnoreCase(String.valueOf(col.get("IS_NULLABLE")));

                Schema colSchema = SnowflakeValueConverters.toConnectSchema(dataType, col, nullable);
                builder.field(columnName, colSchema);
            }

            return builder.build();
        }
        catch (Exception e) {
            LOGGER.warn("Failed to build schema for table {}, using generic schema", tableName, e);
            return SchemaBuilder.struct().name(tableName + ".Value").build();
        }
    }

    private SourceRecord createSnapshotRecord(String tableName, Map<String, Object> row,
                                               Schema valueSchema) {
        return createChangeRecord(tableName, "r", null, row, valueSchema);
    }

    private SourceRecord createChangeRecord(String tableName, String op,
                                             Map<String, Object> beforeRow,
                                             Map<String, Object> afterRow,
                                             Schema valueSchema) {
        String topicName = config.getLogicalName() + "." +
                config.getSnowflakeDatabase() + "." +
                config.getSnowflakeSchema() + "." + tableName;

        // Build envelope schema
        Schema envelopeSchema = SchemaBuilder.struct()
                .name(topicName + ".Envelope")
                .field("before", valueSchema)
                .field("after", valueSchema)
                .field("source", SnowflakeSourceInfo.SCHEMA)
                .field("op", Schema.STRING_SCHEMA)
                .field("ts_ms", Schema.INT64_SCHEMA)
                .build();

        // Build source info
        offsetContext.event(new SnowflakeTableId(config.getSnowflakeDatabase(),
                config.getSnowflakeSchema(), tableName), Instant.now());

        Struct envelope = new Struct(envelopeSchema);
        envelope.put("before", beforeRow != null ? rowToStruct(beforeRow, valueSchema) : null);
        envelope.put("after", afterRow != null ? rowToStruct(afterRow, valueSchema) : null);
        envelope.put("source", offsetContext.getSourceInfo());
        envelope.put("op", op);
        envelope.put("ts_ms", System.currentTimeMillis());

        SnowflakePartition partition = new SnowflakePartition(config.getLogicalName());

        return new SourceRecord(
                partition.getSourcePartition(),
                offsetContext.getOffset(),
                topicName,
                envelopeSchema,
                envelope);
    }

    private Struct rowToStruct(Map<String, Object> row, Schema schema) {
        Struct struct = new Struct(schema);
        for (org.apache.kafka.connect.data.Field field : schema.fields()) {
            Object value = row.get(field.name());
            // Skip Snowflake metadata columns
            if (field.name().startsWith("METADATA$")) {
                continue;
            }
            try {
                struct.put(field.name(), SnowflakeValueConverters.convertValue(value, field.schema()));
            }
            catch (Exception e) {
                LOGGER.warn("Failed to convert value for field {}: {}", field.name(), e.getMessage());
                struct.put(field.name(), null);
            }
        }
        return struct;
    }

    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>();
        // Block for up to 1 second for the first record
        SourceRecord first = queue.poll(1, TimeUnit.SECONDS);
        if (first != null) {
            records.add(first);
            // Drain any additional available records
            queue.drainTo(records, config.getStreamMaxRowsPerPoll() - 1);
        }
        return records.isEmpty() ? null : records;
    }

    public void stop() {
        running.set(false);
        if (coordinatorThread != null) {
            coordinatorThread.interrupt();
            try {
                coordinatorThread.join(5000);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        LOGGER.info("Snowflake change event source coordinator stopped");
    }
}
