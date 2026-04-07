/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.snowflake;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Map;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SnowflakeValueConverters {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeValueConverters.class);

    private SnowflakeValueConverters() {
    }

    public static Schema toConnectSchema(String snowflakeType, Map<String, Object> columnMeta,
                                          boolean nullable) {
        Schema schema = mapType(snowflakeType, columnMeta);
        if (nullable) {
            SchemaBuilder builder = SchemaBuilder.type(schema.type())
                    .name(schema.name())
                    .optional();
            if (schema.parameters() != null) {
                for (Map.Entry<String, String> entry : schema.parameters().entrySet()) {
                    builder.parameter(entry.getKey(), entry.getValue());
                }
            }
            if (schema.version() != null) {
                builder.version(schema.version());
            }
            return builder.build();
        }
        return schema;
    }

    private static Schema mapType(String snowflakeType, Map<String, Object> columnMeta) {
        // Normalize the type string
        String type = snowflakeType.toUpperCase().trim();

        // Remove size specifications for matching
        if (type.contains("(")) {
            String baseName = type.substring(0, type.indexOf("(")).trim();
            return mapBaseType(baseName, columnMeta);
        }

        return mapBaseType(type, columnMeta);
    }

    private static Schema mapBaseType(String type, Map<String, Object> columnMeta) {
        switch (type) {
            case "NUMBER":
            case "DECIMAL":
            case "NUMERIC":
                return mapNumberType(columnMeta);

            case "INT":
            case "INTEGER":
            case "BIGINT":
            case "SMALLINT":
            case "TINYINT":
            case "BYTEINT":
                return Schema.INT64_SCHEMA;

            case "FLOAT":
            case "FLOAT4":
            case "FLOAT8":
            case "DOUBLE":
            case "DOUBLE PRECISION":
            case "REAL":
                return Schema.FLOAT64_SCHEMA;

            case "VARCHAR":
            case "CHAR":
            case "CHARACTER":
            case "STRING":
            case "TEXT":
                return Schema.STRING_SCHEMA;

            case "BINARY":
            case "VARBINARY":
                return Schema.BYTES_SCHEMA;

            case "BOOLEAN":
                return Schema.BOOLEAN_SCHEMA;

            case "DATE":
                return org.apache.kafka.connect.data.Date.SCHEMA;

            case "TIME":
                return org.apache.kafka.connect.data.Time.SCHEMA;

            case "DATETIME":
            case "TIMESTAMP":
            case "TIMESTAMP_NTZ":
            case "TIMESTAMP_LTZ":
            case "TIMESTAMP_TZ":
                return org.apache.kafka.connect.data.Timestamp.SCHEMA;

            case "VARIANT":
            case "ARRAY":
            case "OBJECT":
                // Semi-structured types serialized as JSON strings
                return Schema.STRING_SCHEMA;

            case "GEOGRAPHY":
            case "GEOMETRY":
                return Schema.STRING_SCHEMA;

            case "VECTOR":
                return Schema.STRING_SCHEMA;

            default:
                // Default to string for unknown types
                return Schema.STRING_SCHEMA;
        }
    }

    private static Schema mapNumberType(Map<String, Object> columnMeta) {
        Object precisionObj = columnMeta.get("NUMERIC_PRECISION");
        Object scaleObj = columnMeta.get("NUMERIC_SCALE");

        int precision = precisionObj != null ? ((Number) precisionObj).intValue() : 38;
        int scale = scaleObj != null ? ((Number) scaleObj).intValue() : 0;

        if (scale == 0) {
            if (precision <= 9) {
                return Schema.INT32_SCHEMA;
            }
            else if (precision <= 18) {
                return Schema.INT64_SCHEMA;
            }
        }

        return Decimal.schema(scale);
    }

    public static Object convertValue(Object value, Schema targetSchema) {
        if (value == null) {
            return null;
        }

        switch (targetSchema.type()) {
            case INT32:
                if (value instanceof Number) return ((Number) value).intValue();
                if (value instanceof String) return Integer.parseInt((String) value);
                throw new IllegalArgumentException("Cannot convert " + value.getClass().getName() + " to INT32");

            case INT64:
                if (value instanceof Number) return ((Number) value).longValue();
                if (value instanceof String) return Long.parseLong((String) value);
                throw new IllegalArgumentException("Cannot convert " + value.getClass().getName() + " to INT64");

            case FLOAT32:
                if (value instanceof Number) return ((Number) value).floatValue();
                if (value instanceof String) return Float.parseFloat((String) value);
                throw new IllegalArgumentException("Cannot convert " + value.getClass().getName() + " to FLOAT32");

            case FLOAT64:
                if (value instanceof Number) return ((Number) value).doubleValue();
                if (value instanceof String) return Double.parseDouble((String) value);
                throw new IllegalArgumentException("Cannot convert " + value.getClass().getName() + " to FLOAT64");

            case BOOLEAN:
                if (value instanceof Boolean) return value;
                if (value instanceof Number) return ((Number) value).intValue() != 0;
                if (value instanceof String) {
                    String s = ((String) value).trim().toLowerCase();
                    if ("true".equals(s) || "1".equals(s)) return true;
                    if ("false".equals(s) || "0".equals(s)) return false;
                }
                throw new IllegalArgumentException("Cannot convert " + value + " to Boolean");

            case STRING:
                return String.valueOf(value);

            case BYTES:
                if (targetSchema.name() != null && targetSchema.name().equals(Decimal.LOGICAL_NAME)) {
                    if (value instanceof BigDecimal) {
                        return value;
                    }
                    return new BigDecimal(String.valueOf(value));
                }
                if (value instanceof byte[]) {
                    return value;
                }
                return String.valueOf(value).getBytes();

            default:
                // For logical types (Date, Time, Timestamp), handle conversion
                if (targetSchema.name() != null) {
                    return convertLogicalType(value, targetSchema);
                }
                return String.valueOf(value);
        }
    }

    private static Object convertLogicalType(Object value, Schema targetSchema) {
        String logicalName = targetSchema.name();

        if (org.apache.kafka.connect.data.Date.LOGICAL_NAME.equals(logicalName)) {
            if (value instanceof Date) {
                return value;
            }
            if (value instanceof java.util.Date) {
                return value;
            }
            if (value instanceof LocalDate) {
                return java.util.Date.from(((LocalDate) value).atStartOfDay(ZoneOffset.UTC).toInstant());
            }
            return value;
        }

        if (org.apache.kafka.connect.data.Time.LOGICAL_NAME.equals(logicalName)) {
            if (value instanceof Time) {
                return value;
            }
            if (value instanceof java.util.Date) {
                return value;
            }
            return value;
        }

        if (org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME.equals(logicalName)) {
            if (value instanceof Timestamp) {
                return value;
            }
            if (value instanceof java.util.Date) {
                return value;
            }
            if (value instanceof Instant) {
                return java.util.Date.from((Instant) value);
            }
            return value;
        }

        return value;
    }
}
