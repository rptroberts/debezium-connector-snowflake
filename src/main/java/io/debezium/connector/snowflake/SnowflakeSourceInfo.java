/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.snowflake;

import java.time.Instant;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SnapshotRecord;

public class SnowflakeSourceInfo extends AbstractSourceInfo {

    public static final String DATABASE_NAME_KEY = "db";
    public static final String SCHEMA_NAME_KEY = "schema";
    public static final String TABLE_NAME_KEY = "table";
    public static final String TIMESTAMP_KEY = "ts_ms";
    public static final String SNAPSHOT_KEY = "snapshot";

    static final Schema SCHEMA = SchemaBuilder.struct()
            .name("io.debezium.connector.snowflake.Source")
            .field(DEBEZIUM_VERSION_KEY, Schema.STRING_SCHEMA)
            .field(DEBEZIUM_CONNECTOR_KEY, Schema.STRING_SCHEMA)
            .field(SERVER_NAME_KEY, Schema.STRING_SCHEMA)
            .field(TIMESTAMP_KEY, Schema.INT64_SCHEMA)
            .field(SNAPSHOT_KEY, Schema.OPTIONAL_STRING_SCHEMA)
            .field(DATABASE_NAME_KEY, Schema.STRING_SCHEMA)
            .field(SCHEMA_NAME_KEY, Schema.STRING_SCHEMA)
            .field(TABLE_NAME_KEY, Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    private final String databaseName;
    private final String schemaName;
    private String tableId;
    private Instant eventTimestamp;
    private SnapshotRecord snapshotRecord = SnapshotRecord.FALSE;

    public SnowflakeSourceInfo(SnowflakeConnectorConfig config) {
        super(config);
        this.databaseName = config.getSnowflakeDatabase();
        this.schemaName = config.getSnowflakeSchema();
    }

    public Schema schema() {
        return SCHEMA;
    }

    public Struct struct() {
        Struct struct = new Struct(SCHEMA);
        struct.put(DEBEZIUM_VERSION_KEY, Module.version());
        struct.put(DEBEZIUM_CONNECTOR_KEY, Module.name());
        struct.put(SERVER_NAME_KEY, serverName());
        struct.put(TIMESTAMP_KEY, eventTimestamp != null ? eventTimestamp.toEpochMilli() : 0L);
        struct.put(SNAPSHOT_KEY, snapshotRecord != null && snapshotRecord != SnapshotRecord.FALSE
                ? snapshotRecord.toString().toLowerCase() : null);
        struct.put(DATABASE_NAME_KEY, databaseName);
        struct.put(SCHEMA_NAME_KEY, schemaName);
        struct.put(TABLE_NAME_KEY, tableId);
        return struct;
    }

    public void setTimestamp(Instant timestamp) {
        this.eventTimestamp = timestamp;
    }

    public void setTableId(String tableId) {
        this.tableId = tableId;
    }

    public void setSnapshot(SnapshotRecord snapshot) {
        this.snapshotRecord = snapshot;
    }

    @Override
    public SnapshotRecord snapshot() {
        return snapshotRecord;
    }

    @Override
    public String database() {
        return databaseName;
    }

    @Override
    public Instant timestamp() {
        return eventTimestamp;
    }
}
