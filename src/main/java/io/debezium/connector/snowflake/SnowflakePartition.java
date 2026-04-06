/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.snowflake;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.debezium.pipeline.spi.Partition;

public class SnowflakePartition implements Partition {

    private static final String SERVER_PARTITION_KEY = "server";

    private final String serverName;

    public SnowflakePartition(String serverName) {
        this.serverName = serverName;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return Collections.singletonMap(SERVER_PARTITION_KEY, serverName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SnowflakePartition that = (SnowflakePartition) o;
        return Objects.equals(serverName, that.serverName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serverName);
    }

    @Override
    public String toString() {
        return "SnowflakePartition{serverName='" + serverName + "'}";
    }

    public static class Provider implements Partition.Provider<SnowflakePartition> {

        private final SnowflakeConnectorConfig config;

        public Provider(SnowflakeConnectorConfig config) {
            this.config = config;
        }

        @Override
        public Set<SnowflakePartition> getPartitions() {
            return Collections.singleton(new SnowflakePartition(config.getLogicalName()));
        }
    }
}
