/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.snowflake;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;

class SnowflakePartitionTest {

    @Test
    void shouldReturnSourcePartition() {
        SnowflakePartition partition = new SnowflakePartition("my-server");
        Map<String, String> sourcePartition = partition.getSourcePartition();

        assertThat(sourcePartition).containsEntry("server", "my-server");
        assertThat(sourcePartition).hasSize(1);
    }

    @Test
    void shouldBeEqualForSameServerName() {
        SnowflakePartition p1 = new SnowflakePartition("server-a");
        SnowflakePartition p2 = new SnowflakePartition("server-a");

        assertThat(p1).isEqualTo(p2);
        assertThat(p1.hashCode()).isEqualTo(p2.hashCode());
    }

    @Test
    void shouldNotBeEqualForDifferentServerNames() {
        SnowflakePartition p1 = new SnowflakePartition("server-a");
        SnowflakePartition p2 = new SnowflakePartition("server-b");

        assertThat(p1).isNotEqualTo(p2);
    }
}
