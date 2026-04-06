/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.snowflake;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.SnapshotRecord;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.spi.schema.DataCollectionId;

public class SnowflakeOffsetContext implements OffsetContext {

    private static final String SNAPSHOT_COMPLETED_KEY = "snapshot_completed";
    private static final String LAST_POLL_TIMESTAMP_KEY = "last_poll_timestamp";
    private static final String STREAM_OFFSET_PREFIX = "stream_offset_";

    private final SnowflakeSourceInfo sourceInfo;
    private final TransactionContext transactionContext;
    private boolean snapshotCompleted;
    private Instant lastPollTimestamp;
    private final Map<String, String> streamOffsets;

    public SnowflakeOffsetContext(SnowflakeConnectorConfig config, boolean snapshotCompleted,
                                  Instant lastPollTimestamp, Map<String, String> streamOffsets) {
        this.sourceInfo = new SnowflakeSourceInfo(config);
        this.transactionContext = new TransactionContext();
        this.snapshotCompleted = snapshotCompleted;
        this.lastPollTimestamp = lastPollTimestamp;
        this.streamOffsets = streamOffsets != null ? new HashMap<>(streamOffsets) : new HashMap<>();
    }

    @Override
    public Map<String, ?> getOffset() {
        Map<String, Object> offset = new HashMap<>();
        offset.put(SNAPSHOT_COMPLETED_KEY, snapshotCompleted);
        if (lastPollTimestamp != null) {
            offset.put(LAST_POLL_TIMESTAMP_KEY, lastPollTimestamp.toEpochMilli());
        }
        for (Map.Entry<String, String> entry : streamOffsets.entrySet()) {
            offset.put(STREAM_OFFSET_PREFIX + entry.getKey(), entry.getValue());
        }
        return offset;
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfo.schema();
    }

    @Override
    public Struct getSourceInfo() {
        return sourceInfo.struct();
    }

    @Override
    public boolean isInitialSnapshotRunning() {
        return !snapshotCompleted;
    }

    @Override
    public void markSnapshotRecord(SnapshotRecord record) {
        sourceInfo.setSnapshot(record);
    }

    @Override
    public void event(DataCollectionId collectionId, Instant timestamp) {
        sourceInfo.setTimestamp(timestamp);
        sourceInfo.setTableId(collectionId != null ? collectionId.toString() : null);
    }

    @Override
    public void preSnapshotStart(boolean firstSnapshot) {
        snapshotCompleted = false;
        sourceInfo.setSnapshot(SnapshotRecord.TRUE);
    }

    @Override
    public void preSnapshotCompletion() {
        // no-op
    }

    @Override
    public void postSnapshotCompletion() {
        snapshotCompleted = true;
        sourceInfo.setSnapshot(SnapshotRecord.FALSE);
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    public SnowflakeSourceInfo getSourceInfoObject() {
        return sourceInfo;
    }

    public boolean isSnapshotCompleted() {
        return snapshotCompleted;
    }

    public Instant getLastPollTimestamp() {
        return lastPollTimestamp;
    }

    public void setLastPollTimestamp(Instant timestamp) {
        this.lastPollTimestamp = timestamp;
    }

    public void updateStreamOffset(String tableName, String timestamp) {
        streamOffsets.put(tableName, timestamp);
    }

    public String getStreamOffset(String tableName) {
        return streamOffsets.get(tableName);
    }

    public Map<String, String> getStreamOffsets() {
        return new HashMap<>(streamOffsets);
    }

    public static class Loader implements OffsetContext.Loader<SnowflakeOffsetContext> {

        private final SnowflakeConnectorConfig config;

        public Loader(SnowflakeConnectorConfig config) {
            this.config = config;
        }

        @Override
        public SnowflakeOffsetContext load(Map<String, ?> offset) {
            boolean snapshotCompleted = Boolean.TRUE.equals(offset.get(SNAPSHOT_COMPLETED_KEY));

            Instant lastPollTimestamp = null;
            Object lastPollTs = offset.get(LAST_POLL_TIMESTAMP_KEY);
            if (lastPollTs instanceof Long) {
                lastPollTimestamp = Instant.ofEpochMilli((Long) lastPollTs);
            }

            Map<String, String> streamOffsets = new HashMap<>();
            for (Map.Entry<String, ?> entry : offset.entrySet()) {
                if (entry.getKey().startsWith(STREAM_OFFSET_PREFIX) && entry.getValue() != null) {
                    String tableName = entry.getKey().substring(STREAM_OFFSET_PREFIX.length());
                    streamOffsets.put(tableName, entry.getValue().toString());
                }
            }

            return new SnowflakeOffsetContext(config, snapshotCompleted, lastPollTimestamp, streamOffsets);
        }
    }
}
