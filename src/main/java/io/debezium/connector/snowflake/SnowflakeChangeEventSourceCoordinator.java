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
import org.apache.kafka.connect.errors.ConnectException;
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
    private volatile Throwable fatalError;
    private Thread coordinatorThread;
    private SnowflakeOffsetContext offsetContext;

    // Schema caches to avoid per-row metadata queries
    private final Map<String, Schema> valueSchemaCache = new HashMap<>();
    private final Map<String, Schema> keySchemaCache = new HashMap<>();

    // Table discovery cache
    private List<String> cachedTables;
    private long tablesCacheExpiry;
    private static final long TABLES_CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes

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
            this.fatalError = e;
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

        // Prepare CDC infrastructure for streaming phase
        if (config.getCdcMode() == SnowflakeConnectorConfig.CdcMode.STREAM) {
            for (String table : tables) {
                String streamName = getStreamName(table);
                String qualifiedTable = config.getSnowflakeSchema() + "." + table;
                connection.createStream(streamName, qualifiedTable,
                        config.getStreamType(), snapshotTimestamp);
            }
        }
        else if (config.getCdcMode() == SnowflakeConnectorConfig.CdcMode.CHANGES) {
            // CHANGES clause requires change tracking to be enabled on each table
            for (String table : tables) {
                String quotedTable = SnowflakeIdentifiers.quoteQualifiedName(
                        config.getSnowflakeSchema(), table);
                connection.execute("ALTER TABLE " + quotedTable + " SET CHANGE_TRACKING = TRUE");
                LOGGER.info("Enabled change tracking on table {}", quotedTable);
                // Seed stream offset with snapshot timestamp to avoid gap between snapshot and streaming
                offsetContext.updateStreamOffset(table, snapshotTimestamp);
            }
        }

        offsetContext.postSnapshotCompletion();
        offsetContext.setLastPollTimestamp(Instant.now());

        // Emit a snapshot completion marker so offset with snapshot_completed=true is committed
        emitSnapshotCompletionMarker();
    }

    private void emitSnapshotCompletionMarker() throws InterruptedException {
        String topic = config.getLogicalName() + ".snapshot_completion";
        SnowflakePartition partition = new SnowflakePartition(config.getLogicalName());
        SourceRecord marker = new SourceRecord(
                partition.getSourcePartition(),
                offsetContext.getOffset(),
                topic,
                Schema.OPTIONAL_STRING_SCHEMA,
                null);
        queue.put(marker);
        LOGGER.info("Emitted snapshot completion marker");
    }

    private void snapshotTable(String tableName, String snapshotTimestamp) throws Exception {
        String qualifiedTable = config.getSnowflakeSchema() + "." + tableName;
        List<Map<String, Object>> rows = connection.queryAtTimestamp(qualifiedTable, snapshotTimestamp);
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

        // Ensure change tracking is enabled for changes mode (covers snapshot.mode=never)
        if (config.getCdcMode() == SnowflakeConnectorConfig.CdcMode.CHANGES) {
            for (String table : tables) {
                String quotedTable = SnowflakeIdentifiers.quoteQualifiedName(
                        config.getSnowflakeSchema(), table);
                try {
                    connection.execute("ALTER TABLE " + quotedTable + " SET CHANGE_TRACKING = TRUE");
                }
                catch (Exception e) {
                    LOGGER.debug("Change tracking may already be enabled on {}: {}", quotedTable, e.getMessage());
                }
            }
        }

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

        try {
            if (!connection.streamHasData(streamName)) {
                return Collections.emptyList();
            }
        }
        catch (Exception e) {
            // Handle stale stream errors
            if (isStaleStreamError(e)) {
                return handleStaleStream(table, streamName);
            }
            throw e;
        }

        String tempTableName = "_DBZ_CHANGES_" + table.toUpperCase().replace(".", "_")
                + "_" + System.currentTimeMillis();

        return connection.consumeStreamViaTemp(streamName, tempTableName,
                config.getStreamMaxRowsPerPoll());
    }

    private boolean isStaleStreamError(Exception e) {
        String msg = e.getMessage();
        return msg != null && (msg.contains("stale") || msg.contains("STALE"));
    }

    private List<Map<String, Object>> handleStaleStream(String table, String streamName) throws Exception {
        SnowflakeConnectorConfig.StaleHandlingMode mode = config.getStaleHandlingMode();
        switch (mode) {
            case FAIL:
                throw new ConnectException("Stream " + streamName + " is stale. " +
                        "Configure snowflake.stream.stale.handling.mode=recreate to auto-recover");
            case RECREATE:
                LOGGER.warn("Stream {} is stale, recreating. Data between the old stream offset " +
                        "and now may be lost.", streamName);
                connection.dropStream(streamName);
                String qualifiedTable = config.getSnowflakeSchema() + "." + table;
                connection.createStream(streamName, qualifiedTable,
                        config.getStreamType(), null);
                return Collections.emptyList();
            case SKIP:
                LOGGER.warn("Stream {} is stale, skipping table {} for this poll cycle", streamName, table);
                return Collections.emptyList();
            default:
                throw new ConnectException("Stream " + streamName + " is stale");
        }
    }

    private List<Map<String, Object>> pollChangesMode(String table) throws Exception {
        String fullTableName = config.getSnowflakeSchema() + "." + table;
        // queryChanges internally quotes the table name
        String lastTimestamp = offsetContext.getStreamOffset(table);

        if (lastTimestamp == null) {
            // Only reached when snapshot.mode=never (snapshot seeds offsets otherwise)
            lastTimestamp = connection.getCurrentTimestamp();
            offsetContext.updateStreamOffset(table, lastTimestamp);
            return Collections.emptyList();
        }

        String currentTimestamp = connection.getCurrentTimestamp();
        List<Map<String, Object>> changes = connection.queryChanges(
                fullTableName, lastTimestamp, currentTimestamp);

        // Only advance offset if we actually received changes to avoid skipping a window
        if (changes != null && !changes.isEmpty()) {
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
            List<Map<String, Object>> deletes = new ArrayList<>();
            List<Map<String, Object>> inserts = new ArrayList<>();

            for (Map<String, Object> row : pairs) {
                String action = String.valueOf(row.get("METADATA$ACTION"));
                if ("DELETE".equalsIgnoreCase(action)) {
                    deletes.add(row);
                }
                else if ("INSERT".equalsIgnoreCase(action)) {
                    inserts.add(row);
                }
            }

            // Pair deletes and inserts sequentially
            int pairCount = Math.min(deletes.size(), inserts.size());
            for (int i = 0; i < pairCount; i++) {
                SourceRecord record = createChangeRecord(tableName, "u",
                        deletes.get(i), inserts.get(i), valueSchema);
                queue.put(record);
            }

            // Handle orphaned deletes (missing INSERT pair)
            for (int i = pairCount; i < deletes.size(); i++) {
                LOGGER.warn("Orphaned DELETE in update pair for ROW_ID {}, emitting as delete", entry.getKey());
                queue.put(createChangeRecord(tableName, "d", deletes.get(i), null, valueSchema));
            }

            // Handle orphaned inserts (missing DELETE pair)
            for (int i = pairCount; i < inserts.size(); i++) {
                LOGGER.warn("Orphaned INSERT in update pair for ROW_ID {}, emitting as insert", entry.getKey());
                queue.put(createChangeRecord(tableName, "c", null, inserts.get(i), valueSchema));
            }
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

        // Return cached table list if still valid
        if (cachedTables != null && System.currentTimeMillis() < tablesCacheExpiry) {
            return cachedTables;
        }

        // Discover all tables from Snowflake
        try {
            List<Map<String, Object>> tables = connection.getTables(
                    config.getSnowflakeDatabase(), config.getSnowflakeSchema());
            List<String> tableNames = new ArrayList<>();
            for (Map<String, Object> row : tables) {
                tableNames.add(String.valueOf(row.get("TABLE_NAME")));
            }
            cachedTables = tableNames;
            tablesCacheExpiry = System.currentTimeMillis() + TABLES_CACHE_TTL_MS;
            return tableNames;
        }
        catch (Exception e) {
            LOGGER.error("Failed to discover tables", e);
            return cachedTables != null ? cachedTables : Collections.emptyList();
        }
    }

    private String getStreamName(String tableName) {
        return config.getStreamPrefix() + tableName.toUpperCase().replace(".", "_");
    }

    private Schema buildSchemaForTable(String tableName) {
        Schema cached = valueSchemaCache.get(tableName);
        if (cached != null) {
            return cached;
        }

        try {
            List<Map<String, Object>> columns = connection.getTableColumns(
                    config.getSnowflakeDatabase(), config.getSnowflakeSchema(), tableName);

            SchemaBuilder builder = SchemaBuilder.struct()
                    .name(config.getLogicalName() + "." + config.getSnowflakeSchema() + "." + tableName + ".Value");

            Map<String, Schema> columnSchemas = new HashMap<>();
            for (Map<String, Object> col : columns) {
                String columnName = String.valueOf(col.get("COLUMN_NAME"));
                String dataType = String.valueOf(col.get("DATA_TYPE")).toUpperCase();
                boolean nullable = "YES".equalsIgnoreCase(String.valueOf(col.get("IS_NULLABLE")));

                Schema colSchema = SnowflakeValueConverters.toConnectSchema(dataType, col, nullable);
                builder.field(columnName, colSchema);
                columnSchemas.put(columnName, colSchema);
            }

            Schema valueSchema = builder.build();
            valueSchemaCache.put(tableName, valueSchema);

            // Build key schema from primary keys
            buildKeySchemaForTable(tableName, columnSchemas);

            return valueSchema;
        }
        catch (Exception e) {
            LOGGER.warn("Failed to build schema for table {}, using generic schema", tableName, e);
            return SchemaBuilder.struct().name(tableName + ".Value").build();
        }
    }

    private void buildKeySchemaForTable(String tableName, Map<String, Schema> columnSchemas) {
        try {
            List<Map<String, Object>> pkResult = connection.getPrimaryKeys(
                    config.getSnowflakeSchema(), tableName);

            if (pkResult != null && !pkResult.isEmpty()) {
                SchemaBuilder keyBuilder = SchemaBuilder.struct()
                        .name(config.getLogicalName() + "." + config.getSnowflakeSchema() + "." + tableName + ".Key");

                for (Map<String, Object> pk : pkResult) {
                    String colName = String.valueOf(pk.get("column_name"));
                    Schema colSchema = columnSchemas.get(colName);
                    if (colSchema != null) {
                        keyBuilder.field(colName, colSchema);
                    }
                }

                Schema keySchema = keyBuilder.build();
                if (!keySchema.fields().isEmpty()) {
                    keySchemaCache.put(tableName, keySchema);
                }
            }
        }
        catch (Exception e) {
            LOGGER.debug("Could not retrieve primary keys for table {}: {}", tableName, e.getMessage());
        }
    }

    private Schema getKeySchemaForTable(String tableName) {
        return keySchemaCache.get(tableName);
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

        // Build envelope schema — before/after must be optional (null for inserts/deletes/snapshots)
        Schema optionalValueSchema = makeOptional(valueSchema);
        Schema envelopeSchema = SchemaBuilder.struct()
                .name(topicName + ".Envelope")
                .field("before", optionalValueSchema)
                .field("after", optionalValueSchema)
                .field("source", SnowflakeSourceInfo.SCHEMA)
                .field("op", Schema.STRING_SCHEMA)
                .field("ts_ms", Schema.INT64_SCHEMA)
                .build();

        // Build source info
        offsetContext.event(new SnowflakeTableId(config.getSnowflakeDatabase(),
                config.getSnowflakeSchema(), tableName), Instant.now());

        Struct envelope = new Struct(envelopeSchema);
        envelope.put("before", beforeRow != null ? rowToStruct(beforeRow, optionalValueSchema) : null);
        envelope.put("after", afterRow != null ? rowToStruct(afterRow, optionalValueSchema) : null);
        envelope.put("source", offsetContext.getSourceInfo());
        envelope.put("op", op);
        envelope.put("ts_ms", System.currentTimeMillis());

        SnowflakePartition partition = new SnowflakePartition(config.getLogicalName());

        // Build key struct from primary key schema if available
        Schema keySchema = getKeySchemaForTable(tableName);
        Struct keyStruct = null;
        if (keySchema != null) {
            Map<String, Object> keySourceRow = afterRow != null ? afterRow : beforeRow;
            if (keySourceRow != null) {
                keyStruct = rowToStruct(keySourceRow, keySchema);
            }
        }

        return new SourceRecord(
                partition.getSourcePartition(),
                offsetContext.getOffset(),
                topicName,
                keySchema,
                keyStruct,
                envelopeSchema,
                envelope);
    }

    private Schema makeOptional(Schema schema) {
        SchemaBuilder builder = SchemaBuilder.struct()
                .name(schema.name())
                .optional();
        for (org.apache.kafka.connect.data.Field field : schema.fields()) {
            builder.field(field.name(), field.schema());
        }
        return builder.build();
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
        if (fatalError != null) {
            throw new ConnectException("Coordinator thread failed", fatalError);
        }
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
                if (coordinatorThread.isAlive()) {
                    LOGGER.warn("Coordinator thread did not stop within 5 seconds");
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        LOGGER.info("Snowflake change event source coordinator stopped");
    }
}
