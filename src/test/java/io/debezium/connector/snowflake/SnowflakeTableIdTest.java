/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.snowflake;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SnowflakeTableIdTest {

    @Test
    void shouldReturnIdentifier() {
        SnowflakeTableId id = new SnowflakeTableId("MYDB", "PUBLIC", "USERS");
        assertThat(id.identifier()).isEqualTo("MYDB.PUBLIC.USERS");
    }

    @Test
    void shouldReturnParts() {
        SnowflakeTableId id = new SnowflakeTableId("MYDB", "MYSCHEMA", "ORDERS");
        assertThat(id.parts()).containsExactly("MYDB", "MYSCHEMA", "ORDERS");
        assertThat(id.databaseParts()).containsExactly("MYDB");
        assertThat(id.schemaParts()).containsExactly("MYDB", "MYSCHEMA");
    }

    @Test
    void shouldBeEqualForSameComponents() {
        SnowflakeTableId id1 = new SnowflakeTableId("DB", "SCHEMA", "TABLE");
        SnowflakeTableId id2 = new SnowflakeTableId("DB", "SCHEMA", "TABLE");

        assertThat(id1).isEqualTo(id2);
        assertThat(id1.hashCode()).isEqualTo(id2.hashCode());
    }
}
