/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.snowflake;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;

class SnowflakeOffsetContextTest {

    private SnowflakeConnectorConfig config;

    @BeforeEach
    void setUp() {
        Configuration rawConfig = Configuration.create()
                .with("topic.prefix", "test-server")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_URL, "https://test.snowflakecomputing.com")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_USER, "user")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_PASSWORD, "pass")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_DATABASE, "testdb")
                .build();
        config = new SnowflakeConnectorConfig(rawConfig);
    }

    @Test
    void shouldSerializeAndDeserializeOffset() {
        Instant now = Instant.now();
        Map<String, String> streamOffsets = new HashMap<>();
        streamOffsets.put("TABLE1", "2026-01-01T00:00:00Z");
        streamOffsets.put("TABLE2", "2026-01-02T00:00:00Z");

        SnowflakeOffsetContext context = new SnowflakeOffsetContext(config, true, now, streamOffsets);
        Map<String, ?> serialized = context.getOffset();

        assertThat(serialized.get("snapshot_completed")).isEqualTo(true);
        assertThat(serialized.get("last_poll_timestamp")).isEqualTo(now.toEpochMilli());
        assertThat(serialized.get("stream_offset_TABLE1")).isEqualTo("2026-01-01T00:00:00Z");
        assertThat(serialized.get("stream_offset_TABLE2")).isEqualTo("2026-01-02T00:00:00Z");

        // Deserialize
        SnowflakeOffsetContext.Loader loader = new SnowflakeOffsetContext.Loader(config);
        SnowflakeOffsetContext restored = loader.load((Map<String, Object>) (Map) serialized);

        assertThat(restored.isSnapshotCompleted()).isTrue();
        // Timestamps lose sub-millisecond precision through serialization (epoch millis)
        assertThat(restored.getLastPollTimestamp().toEpochMilli()).isEqualTo(now.toEpochMilli());
        assertThat(restored.getStreamOffset("TABLE1")).isEqualTo("2026-01-01T00:00:00Z");
        assertThat(restored.getStreamOffset("TABLE2")).isEqualTo("2026-01-02T00:00:00Z");
    }

    @Test
    void shouldHandleEmptyOffset() {
        SnowflakeOffsetContext context = new SnowflakeOffsetContext(config, false, null, null);

        assertThat(context.isSnapshotCompleted()).isFalse();
        assertThat(context.isInitialSnapshotRunning()).isTrue();
        assertThat(context.getLastPollTimestamp()).isNull();
        assertThat(context.getStreamOffsets()).isEmpty();
    }

    @Test
    void shouldUpdateStreamOffset() {
        SnowflakeOffsetContext context = new SnowflakeOffsetContext(config, false, null, null);

        context.updateStreamOffset("TABLE1", "2026-03-15T10:00:00Z");
        assertThat(context.getStreamOffset("TABLE1")).isEqualTo("2026-03-15T10:00:00Z");

        context.updateStreamOffset("TABLE1", "2026-03-15T11:00:00Z");
        assertThat(context.getStreamOffset("TABLE1")).isEqualTo("2026-03-15T11:00:00Z");
    }

    @Test
    void shouldTrackSnapshotLifecycle() {
        SnowflakeOffsetContext context = new SnowflakeOffsetContext(config, false, null, null);

        assertThat(context.isInitialSnapshotRunning()).isTrue();

        context.preSnapshotStart(true);
        assertThat(context.isInitialSnapshotRunning()).isTrue();

        context.postSnapshotCompletion();
        assertThat(context.isInitialSnapshotRunning()).isFalse();
        assertThat(context.isSnapshotCompleted()).isTrue();
    }

    @Test
    void shouldNormalizeTableNameCaseInStreamOffsets() {
        SnowflakeOffsetContext context = new SnowflakeOffsetContext(config, false, null, null);

        // Store with lowercase
        context.updateStreamOffset("orders", "2026-03-15T10:00:00Z");
        // Retrieve with uppercase — should find the same entry
        assertThat(context.getStreamOffset("ORDERS")).isEqualTo("2026-03-15T10:00:00Z");
        // Retrieve with mixed case
        assertThat(context.getStreamOffset("Orders")).isEqualTo("2026-03-15T10:00:00Z");

        // Should not create duplicate entries
        context.updateStreamOffset("ORDERS", "2026-03-15T11:00:00Z");
        assertThat(context.getStreamOffsets()).hasSize(1);
        assertThat(context.getStreamOffset("orders")).isEqualTo("2026-03-15T11:00:00Z");
    }

    @Test
    void shouldNormalizeCaseDuringOffsetLoad() {
        // Simulate loading an offset with a lowercase table name (backward compat)
        Map<String, Object> rawOffset = new HashMap<>();
        rawOffset.put("snapshot_completed", true);
        rawOffset.put("stream_offset_orders", "2026-03-15T10:00:00Z");

        SnowflakeOffsetContext.Loader loader = new SnowflakeOffsetContext.Loader(config);
        SnowflakeOffsetContext restored = loader.load(rawOffset);

        // Should be accessible via uppercase
        assertThat(restored.getStreamOffset("ORDERS")).isEqualTo("2026-03-15T10:00:00Z");
    }

    @Test
    void shouldProduceSourceInfoStruct() {
        SnowflakeOffsetContext context = new SnowflakeOffsetContext(config, false, null, null);

        assertThat(context.getSourceInfoSchema()).isNotNull();
        assertThat(context.getSourceInfo()).isNotNull();
        assertThat(context.getSourceInfo().getString("db")).isEqualTo("testdb");
        assertThat(context.getSourceInfo().getString("connector")).isEqualTo("snowflake");
    }
}
