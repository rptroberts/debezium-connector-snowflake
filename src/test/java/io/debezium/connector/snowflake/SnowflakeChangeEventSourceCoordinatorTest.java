/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.snowflake;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;

class SnowflakeChangeEventSourceCoordinatorTest {

    private SnowflakeConnectorConfig config;
    private SnowflakeConnection mockConnection;
    private SourceTaskContext mockTaskContext;
    private OffsetStorageReader mockOffsetReader;

    @BeforeEach
    void setUp() {
        Configuration rawConfig = Configuration.create()
                .with("topic.prefix", "test-server")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_URL, "https://test.snowflakecomputing.com")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_USER, "user")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_PASSWORD, "pass")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_DATABASE, "testdb")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_SCHEMA, "PUBLIC")
                .with(SnowflakeConnectorConfig.TABLE_INCLUDE_LIST, "TEST_TABLE")
                .build();
        config = new SnowflakeConnectorConfig(rawConfig);

        mockConnection = mock(SnowflakeConnection.class);
        mockTaskContext = mock(SourceTaskContext.class);
        mockOffsetReader = mock(OffsetStorageReader.class);

        when(mockTaskContext.offsetStorageReader()).thenReturn(mockOffsetReader);
        when(mockOffsetReader.offset(any())).thenReturn(null);
    }

    @Test
    void shouldPropagateFatalErrorThroughPoll() throws Exception {
        // Simulate a connection failure that kills the coordinator thread
        when(mockConnection.getCurrentTimestamp()).thenThrow(new SQLException("Connection lost"));

        SnowflakeChangeEventSourceCoordinator coordinator =
                new SnowflakeChangeEventSourceCoordinator(config, mockConnection, mockTaskContext);
        coordinator.start();

        // Wait for coordinator thread to fail
        Thread.sleep(500);

        assertThatThrownBy(coordinator::poll)
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("Coordinator thread failed");

        coordinator.stop();
    }

    @Test
    void shouldReturnNullFromPollWhenQueueEmpty() throws Exception {
        // Set up a coordinator that won't actually start streaming (snapshot.mode=never, no tables)
        Configuration rawConfig = Configuration.create()
                .with("topic.prefix", "test-server")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_URL, "https://test.snowflakecomputing.com")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_USER, "user")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_PASSWORD, "pass")
                .with(SnowflakeConnectorConfig.SNOWFLAKE_DATABASE, "testdb")
                .with(SnowflakeConnectorConfig.SNAPSHOT_MODE, "never")
                .with(SnowflakeConnectorConfig.TABLE_INCLUDE_LIST, "TEST_TABLE")
                .with(SnowflakeConnectorConfig.CDC_MODE, "changes")
                .build();
        SnowflakeConnectorConfig neverConfig = new SnowflakeConnectorConfig(rawConfig);

        when(mockConnection.getCurrentTimestamp()).thenReturn("2026-01-01 00:00:00.000000");
        when(mockConnection.queryChanges(anyString(), anyString(), anyString()))
                .thenReturn(Collections.emptyList());

        SnowflakeChangeEventSourceCoordinator coordinator =
                new SnowflakeChangeEventSourceCoordinator(neverConfig, mockConnection, mockTaskContext);
        coordinator.start();

        // poll should return null when no data
        Thread.sleep(200);
        List<SourceRecord> records = coordinator.poll();
        assertThat(records).isNull();

        coordinator.stop();
    }

    @Test
    void shouldEmitSnapshotRecords() throws Exception {
        // Mock snapshot query returning rows
        List<Map<String, Object>> snapshotRows = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("ID", 1);
        row1.put("NAME", "Alice");
        snapshotRows.add(row1);

        List<Map<String, Object>> columns = new ArrayList<>();
        Map<String, Object> col1 = new HashMap<>();
        col1.put("COLUMN_NAME", "ID");
        col1.put("DATA_TYPE", "NUMBER");
        col1.put("NUMERIC_PRECISION", 10);
        col1.put("NUMERIC_SCALE", 0);
        col1.put("IS_NULLABLE", "NO");
        columns.add(col1);

        Map<String, Object> col2 = new HashMap<>();
        col2.put("COLUMN_NAME", "NAME");
        col2.put("DATA_TYPE", "VARCHAR");
        col2.put("IS_NULLABLE", "YES");
        columns.add(col2);

        when(mockConnection.getCurrentTimestamp()).thenReturn("2026-01-01 00:00:00.000000");
        when(mockConnection.queryAtTimestamp(anyString(), anyString())).thenReturn(snapshotRows);
        when(mockConnection.getTableColumns(anyString(), anyString(), anyString())).thenReturn(columns);
        when(mockConnection.getPrimaryKeys(anyString(), anyString())).thenReturn(Collections.emptyList());
        when(mockConnection.streamHasData(anyString())).thenReturn(false);

        SnowflakeChangeEventSourceCoordinator coordinator =
                new SnowflakeChangeEventSourceCoordinator(config, mockConnection, mockTaskContext);
        coordinator.start();

        // Wait for snapshot to complete and poll results
        Thread.sleep(1000);
        List<SourceRecord> records = coordinator.poll();

        // Should have snapshot records + snapshot completion marker
        assertThat(records).isNotNull();
        assertThat(records.size()).isGreaterThanOrEqualTo(1);

        coordinator.stop();
    }

    @Test
    void shouldCacheSchemaForTable() throws Exception {
        List<Map<String, Object>> columns = new ArrayList<>();
        Map<String, Object> col = new HashMap<>();
        col.put("COLUMN_NAME", "ID");
        col.put("DATA_TYPE", "NUMBER");
        col.put("NUMERIC_PRECISION", 10);
        col.put("NUMERIC_SCALE", 0);
        col.put("IS_NULLABLE", "NO");
        columns.add(col);

        List<Map<String, Object>> snapshotRows = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("ID", i);
            snapshotRows.add(row);
        }

        when(mockConnection.getCurrentTimestamp()).thenReturn("2026-01-01 00:00:00.000000");
        when(mockConnection.queryAtTimestamp(anyString(), anyString())).thenReturn(snapshotRows);
        when(mockConnection.getTableColumns(anyString(), anyString(), anyString())).thenReturn(columns);
        when(mockConnection.getPrimaryKeys(anyString(), anyString())).thenReturn(Collections.emptyList());
        when(mockConnection.streamHasData(anyString())).thenReturn(false);

        SnowflakeChangeEventSourceCoordinator coordinator =
                new SnowflakeChangeEventSourceCoordinator(config, mockConnection, mockTaskContext);
        coordinator.start();

        Thread.sleep(1000);
        coordinator.poll();
        coordinator.stop();

        // Schema should have been queried only once (cached), not per-row
        verify(mockConnection, times(1)).getTableColumns(anyString(), anyString(), anyString());
    }

    @Test
    void shouldHandleConcurrentOffsetAccess() throws Exception {
        // Test thread safety of SnowflakeOffsetContext
        SnowflakeOffsetContext offsetContext = new SnowflakeOffsetContext(config, false, null, null);

        int threadCount = 10;
        int iterationsPerThread = 1000;
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicBoolean failed = new AtomicBoolean(false);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < iterationsPerThread; i++) {
                        offsetContext.updateStreamOffset("TABLE_" + threadId, "2026-01-01T00:" + i);
                        offsetContext.setLastPollTimestamp(Instant.now());
                        offsetContext.getOffset(); // concurrent read
                        offsetContext.getStreamOffset("TABLE_" + threadId);
                    }
                }
                catch (Exception e) {
                    failed.set(true);
                }
                finally {
                    latch.countDown();
                }
            });
        }

        latch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(failed.get()).isFalse();
    }
}
