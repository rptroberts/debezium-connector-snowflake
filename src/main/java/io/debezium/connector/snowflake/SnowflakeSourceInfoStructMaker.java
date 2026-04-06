/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.snowflake;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.SourceInfoStructMaker;

public class SnowflakeSourceInfoStructMaker implements SourceInfoStructMaker<SnowflakeSourceInfo> {

    private Schema schema;

    @Override
    public void init(String connector, String version, CommonConnectorConfig connectorConfig) {
        this.schema = SnowflakeSourceInfo.SCHEMA;
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SnowflakeSourceInfo sourceInfo) {
        return sourceInfo.struct();
    }
}
