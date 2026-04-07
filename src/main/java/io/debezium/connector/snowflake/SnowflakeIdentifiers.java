/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.snowflake;

import java.util.regex.Pattern;

/**
 * Utility for safely quoting Snowflake SQL identifiers and validating timestamp literals.
 */
public final class SnowflakeIdentifiers {

    private static final Pattern TIMESTAMP_PATTERN = Pattern.compile(
            "^\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}:\\d{2}(\\.\\d{1,9})?( ?[+-]\\d{2}:?\\d{2})?$");

    private SnowflakeIdentifiers() {
    }

    /**
     * Wraps a Snowflake identifier in double quotes, escaping any embedded double quotes
     * by doubling them per Snowflake's standard identifier quoting rules.
     *
     * @param identifier the identifier to quote (table name, schema name, stream name, etc.)
     * @return the quoted identifier
     * @throws IllegalArgumentException if identifier is null or empty
     */
    public static String quoteIdentifier(String identifier) {
        if (identifier == null || identifier.isEmpty()) {
            throw new IllegalArgumentException("Identifier must not be null or empty");
        }
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    /**
     * Quotes a fully-qualified name like SCHEMA.TABLE as "SCHEMA"."TABLE".
     *
     * @param parts the identifier parts (e.g. schema, table)
     * @return the quoted fully-qualified identifier
     */
    public static String quoteQualifiedName(String... parts) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) {
                sb.append(".");
            }
            sb.append(quoteIdentifier(parts[i]));
        }
        return sb.toString();
    }

    /**
     * Quotes a dot-separated qualified name by splitting on '.' and quoting each part.
     * E.g. "SCHEMA.TABLE" becomes "SCHEMA"."TABLE".
     * If there's no dot, behaves like quoteIdentifier.
     *
     * @param qualifiedName a possibly dot-separated identifier
     * @return the quoted name
     */
    public static String quoteDotSeparatedName(String qualifiedName) {
        if (qualifiedName == null || qualifiedName.isEmpty()) {
            throw new IllegalArgumentException("Identifier must not be null or empty");
        }
        String[] parts = qualifiedName.split("\\.");
        return quoteQualifiedName(parts);
    }

    /**
     * Validates that a string looks like a Snowflake timestamp literal.
     * Used for DDL contexts where PreparedStatement parameter binding is not possible.
     *
     * @param timestamp the timestamp string to validate
     * @return the validated timestamp (unchanged)
     * @throws IllegalArgumentException if the timestamp does not match expected format
     */
    public static String validateTimestamp(String timestamp) {
        if (timestamp == null || timestamp.isEmpty()) {
            throw new IllegalArgumentException("Timestamp must not be null or empty");
        }
        if (!TIMESTAMP_PATTERN.matcher(timestamp).matches()) {
            throw new IllegalArgumentException(
                    "Invalid timestamp format: " + timestamp + ". Expected format like 2024-01-15 10:30:00");
        }
        return timestamp;
    }
}
