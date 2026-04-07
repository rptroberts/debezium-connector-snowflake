/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.snowflake;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

class SnowflakeValueConvertersTest {

    @Test
    void shouldMapIntegerTypes() {
        Map<String, Object> meta = createMeta(9, 0);
        Schema schema = SnowflakeValueConverters.toConnectSchema("NUMBER", meta, false);
        assertThat(schema.type()).isEqualTo(Schema.Type.INT32);

        meta = createMeta(18, 0);
        schema = SnowflakeValueConverters.toConnectSchema("NUMBER", meta, false);
        assertThat(schema.type()).isEqualTo(Schema.Type.INT64);
    }

    @Test
    void shouldMapDecimalType() {
        Map<String, Object> meta = createMeta(10, 2);
        Schema schema = SnowflakeValueConverters.toConnectSchema("NUMBER", meta, false);
        assertThat(schema.type()).isEqualTo(Schema.Type.BYTES);
        assertThat(schema.name()).isEqualTo("org.apache.kafka.connect.data.Decimal");
    }

    @Test
    void shouldMapStringTypes() {
        Map<String, Object> meta = createMeta(null, null);
        assertThat(SnowflakeValueConverters.toConnectSchema("VARCHAR", meta, false).type())
                .isEqualTo(Schema.Type.STRING);
        assertThat(SnowflakeValueConverters.toConnectSchema("TEXT", meta, false).type())
                .isEqualTo(Schema.Type.STRING);
        assertThat(SnowflakeValueConverters.toConnectSchema("STRING", meta, false).type())
                .isEqualTo(Schema.Type.STRING);
    }

    @Test
    void shouldMapFloatTypes() {
        Map<String, Object> meta = createMeta(null, null);
        assertThat(SnowflakeValueConverters.toConnectSchema("FLOAT", meta, false).type())
                .isEqualTo(Schema.Type.FLOAT64);
        assertThat(SnowflakeValueConverters.toConnectSchema("DOUBLE", meta, false).type())
                .isEqualTo(Schema.Type.FLOAT64);
    }

    @Test
    void shouldMapBooleanType() {
        Map<String, Object> meta = createMeta(null, null);
        assertThat(SnowflakeValueConverters.toConnectSchema("BOOLEAN", meta, false).type())
                .isEqualTo(Schema.Type.BOOLEAN);
    }

    @Test
    void shouldMapSemiStructuredTypesToString() {
        Map<String, Object> meta = createMeta(null, null);
        assertThat(SnowflakeValueConverters.toConnectSchema("VARIANT", meta, false).type())
                .isEqualTo(Schema.Type.STRING);
        assertThat(SnowflakeValueConverters.toConnectSchema("ARRAY", meta, false).type())
                .isEqualTo(Schema.Type.STRING);
        assertThat(SnowflakeValueConverters.toConnectSchema("OBJECT", meta, false).type())
                .isEqualTo(Schema.Type.STRING);
    }

    @Test
    void shouldHandleNullableSchemas() {
        Map<String, Object> meta = createMeta(null, null);
        Schema nullable = SnowflakeValueConverters.toConnectSchema("VARCHAR", meta, true);
        assertThat(nullable.isOptional()).isTrue();

        Schema nonNullable = SnowflakeValueConverters.toConnectSchema("VARCHAR", meta, false);
        assertThat(nonNullable.isOptional()).isFalse();
    }

    @Test
    void shouldConvertIntValues() {
        assertThat(SnowflakeValueConverters.convertValue(42L, Schema.INT32_SCHEMA)).isEqualTo(42);
        assertThat(SnowflakeValueConverters.convertValue(42, Schema.INT64_SCHEMA)).isEqualTo(42L);
    }

    @Test
    void shouldConvertFloatValues() {
        assertThat(SnowflakeValueConverters.convertValue(3.14, Schema.FLOAT64_SCHEMA)).isEqualTo(3.14);
    }

    @Test
    void shouldConvertBooleanValues() {
        assertThat(SnowflakeValueConverters.convertValue(true, Schema.BOOLEAN_SCHEMA)).isEqualTo(true);
        assertThat(SnowflakeValueConverters.convertValue("true", Schema.BOOLEAN_SCHEMA)).isEqualTo(true);
    }

    @Test
    void shouldConvertStringValues() {
        assertThat(SnowflakeValueConverters.convertValue("hello", Schema.STRING_SCHEMA)).isEqualTo("hello");
        assertThat(SnowflakeValueConverters.convertValue(123, Schema.STRING_SCHEMA)).isEqualTo("123");
    }

    @Test
    void shouldConvertNullValues() {
        assertThat(SnowflakeValueConverters.convertValue(null, Schema.STRING_SCHEMA)).isNull();
        assertThat(SnowflakeValueConverters.convertValue(null, Schema.INT32_SCHEMA)).isNull();
    }

    // --- Tests for nullable schema parameter preservation (Bug 1A) ---

    @Test
    void shouldPreserveDecimalParametersWhenNullable() {
        Map<String, Object> meta = createMeta(10, 2);
        Schema schema = SnowflakeValueConverters.toConnectSchema("NUMBER", meta, true);
        assertThat(schema.isOptional()).isTrue();
        assertThat(schema.name()).isEqualTo(Decimal.LOGICAL_NAME);
        assertThat(schema.parameters()).isNotNull();
        assertThat(schema.parameters()).containsEntry("scale", "2");

        // Round-trip through Decimal serialization should not throw NPE
        BigDecimal value = new BigDecimal("123.45");
        byte[] bytes = Decimal.fromLogical(schema, value);
        BigDecimal roundTripped = Decimal.toLogical(schema, bytes);
        assertThat(roundTripped).isEqualByComparingTo(value);
    }

    @Test
    void shouldPreserveTimestampVersionWhenNullable() {
        Map<String, Object> meta = createMeta(null, null);
        Schema schema = SnowflakeValueConverters.toConnectSchema("TIMESTAMP_NTZ", meta, true);
        assertThat(schema.isOptional()).isTrue();
        assertThat(schema.name()).isEqualTo(org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME);
        assertThat(schema.version()).isEqualTo(org.apache.kafka.connect.data.Timestamp.SCHEMA.version());
    }

    @Test
    void shouldPreserveDateVersionWhenNullable() {
        Map<String, Object> meta = createMeta(null, null);
        Schema schema = SnowflakeValueConverters.toConnectSchema("DATE", meta, true);
        assertThat(schema.isOptional()).isTrue();
        assertThat(schema.name()).isEqualTo(org.apache.kafka.connect.data.Date.LOGICAL_NAME);
    }

    // --- Tests for String-to-numeric conversion (Bug 1C) ---

    @Test
    void shouldConvertStringToInt32() {
        assertThat(SnowflakeValueConverters.convertValue("42", Schema.INT32_SCHEMA)).isEqualTo(42);
    }

    @Test
    void shouldConvertStringToInt64() {
        assertThat(SnowflakeValueConverters.convertValue("9999999999", Schema.INT64_SCHEMA))
                .isEqualTo(9999999999L);
    }

    @Test
    void shouldConvertStringToFloat32() {
        assertThat(SnowflakeValueConverters.convertValue("2.5", Schema.FLOAT32_SCHEMA)).isEqualTo(2.5f);
    }

    @Test
    void shouldConvertStringToFloat64() {
        assertThat(SnowflakeValueConverters.convertValue("3.14", Schema.FLOAT64_SCHEMA)).isEqualTo(3.14);
    }

    @Test
    void shouldConvertBigDecimalToInt32() {
        assertThat(SnowflakeValueConverters.convertValue(new BigDecimal("42"), Schema.INT32_SCHEMA))
                .isEqualTo(42);
    }

    @Test
    void shouldThrowOnInvalidStringToInt() {
        assertThatThrownBy(() -> SnowflakeValueConverters.convertValue("abc", Schema.INT32_SCHEMA))
                .isInstanceOf(NumberFormatException.class);
    }

    // --- Tests for boolean conversion from numeric values (Bug 1D) ---

    @Test
    void shouldConvertIntegerOneToTrue() {
        assertThat(SnowflakeValueConverters.convertValue(1, Schema.BOOLEAN_SCHEMA)).isEqualTo(true);
    }

    @Test
    void shouldConvertIntegerZeroToFalse() {
        assertThat(SnowflakeValueConverters.convertValue(0, Schema.BOOLEAN_SCHEMA)).isEqualTo(false);
    }

    @Test
    void shouldConvertNonZeroIntegerToTrue() {
        assertThat(SnowflakeValueConverters.convertValue(42, Schema.BOOLEAN_SCHEMA)).isEqualTo(true);
    }

    @Test
    void shouldConvertString1ToTrue() {
        assertThat(SnowflakeValueConverters.convertValue("1", Schema.BOOLEAN_SCHEMA)).isEqualTo(true);
    }

    @Test
    void shouldConvertString0ToFalse() {
        assertThat(SnowflakeValueConverters.convertValue("0", Schema.BOOLEAN_SCHEMA)).isEqualTo(false);
    }

    @Test
    void shouldConvertStringTrueToTrueCaseInsensitive() {
        assertThat(SnowflakeValueConverters.convertValue("TRUE", Schema.BOOLEAN_SCHEMA)).isEqualTo(true);
        assertThat(SnowflakeValueConverters.convertValue("True", Schema.BOOLEAN_SCHEMA)).isEqualTo(true);
    }

    @Test
    void shouldConvertStringFalseToFalse() {
        assertThat(SnowflakeValueConverters.convertValue("false", Schema.BOOLEAN_SCHEMA)).isEqualTo(false);
        assertThat(SnowflakeValueConverters.convertValue("FALSE", Schema.BOOLEAN_SCHEMA)).isEqualTo(false);
    }

    private Map<String, Object> createMeta(Integer precision, Integer scale) {
        Map<String, Object> meta = new HashMap<>();
        if (precision != null) {
            meta.put("NUMERIC_PRECISION", precision);
        }
        if (scale != null) {
            meta.put("NUMERIC_SCALE", scale);
        }
        return meta;
    }
}
