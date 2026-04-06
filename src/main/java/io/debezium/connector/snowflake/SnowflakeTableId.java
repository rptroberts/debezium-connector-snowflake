/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.snowflake;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import io.debezium.spi.schema.DataCollectionId;

public class SnowflakeTableId implements DataCollectionId {

    private final String database;
    private final String schema;
    private final String table;

    public SnowflakeTableId(String database, String schema, String table) {
        this.database = database;
        this.schema = schema;
        this.table = table;
    }

    public String getDatabase() {
        return database;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    @Override
    public String identifier() {
        return database + "." + schema + "." + table;
    }

    @Override
    public List<String> parts() {
        return Arrays.asList(database, schema, table);
    }

    @Override
    public List<String> databaseParts() {
        return Arrays.asList(database);
    }

    @Override
    public List<String> schemaParts() {
        return Arrays.asList(database, schema);
    }

    @Override
    public String toString() {
        return identifier();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SnowflakeTableId that = (SnowflakeTableId) o;
        return Objects.equals(database, that.database)
                && Objects.equals(schema, that.schema)
                && Objects.equals(table, that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(database, schema, table);
    }
}
