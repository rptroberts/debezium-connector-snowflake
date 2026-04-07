/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.snowflake;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class SnowflakeIdentifiersTest {

    @Test
    void shouldQuoteSimpleIdentifier() {
        assertThat(SnowflakeIdentifiers.quoteIdentifier("MY_TABLE"))
                .isEqualTo("\"MY_TABLE\"");
    }

    @Test
    void shouldEscapeEmbeddedDoubleQuotes() {
        assertThat(SnowflakeIdentifiers.quoteIdentifier("table\"name"))
                .isEqualTo("\"table\"\"name\"");
    }

    @Test
    void shouldHandleMultipleEmbeddedQuotes() {
        assertThat(SnowflakeIdentifiers.quoteIdentifier("a\"b\"c"))
                .isEqualTo("\"a\"\"b\"\"c\"");
    }

    @Test
    void shouldRejectNullIdentifier() {
        assertThatThrownBy(() -> SnowflakeIdentifiers.quoteIdentifier(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("null or empty");
    }

    @Test
    void shouldRejectEmptyIdentifier() {
        assertThatThrownBy(() -> SnowflakeIdentifiers.quoteIdentifier(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("null or empty");
    }

    @Test
    void shouldQuoteQualifiedName() {
        assertThat(SnowflakeIdentifiers.quoteQualifiedName("SCHEMA", "TABLE"))
                .isEqualTo("\"SCHEMA\".\"TABLE\"");
    }

    @Test
    void shouldQuoteThreePartQualifiedName() {
        assertThat(SnowflakeIdentifiers.quoteQualifiedName("DB", "SCHEMA", "TABLE"))
                .isEqualTo("\"DB\".\"SCHEMA\".\"TABLE\"");
    }

    @Test
    void shouldValidateCorrectTimestamp() {
        String ts = "2026-01-15 10:30:00.123456";
        assertThat(SnowflakeIdentifiers.validateTimestamp(ts)).isEqualTo(ts);
    }

    @Test
    void shouldValidateIsoTimestamp() {
        String ts = "2026-01-15T10:30:00.123456";
        assertThat(SnowflakeIdentifiers.validateTimestamp(ts)).isEqualTo(ts);
    }

    @Test
    void shouldRejectInvalidTimestamp() {
        assertThatThrownBy(() -> SnowflakeIdentifiers.validateTimestamp("not-a-timestamp"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid timestamp format");
    }

    @Test
    void shouldRejectNullTimestamp() {
        assertThatThrownBy(() -> SnowflakeIdentifiers.validateTimestamp(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectEmptyTimestamp() {
        assertThatThrownBy(() -> SnowflakeIdentifiers.validateTimestamp(""))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectSqlInjectionInTimestamp() {
        assertThatThrownBy(() -> SnowflakeIdentifiers.validateTimestamp("'; DROP TABLE users; --"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectTimestampWithSqlInjectionSuffix() {
        // This was the primary SQL injection vector — valid prefix + malicious suffix
        assertThatThrownBy(() -> SnowflakeIdentifiers.validateTimestamp(
                "2024-01-15 10:30:00'; DROP TABLE users; --"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectTimestampWithUnionSelect() {
        assertThatThrownBy(() -> SnowflakeIdentifiers.validateTimestamp(
                "2024-01-15 10:30:00' UNION SELECT * FROM credentials --"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectTimestampWithTrailingGarbage() {
        assertThatThrownBy(() -> SnowflakeIdentifiers.validateTimestamp(
                "2024-01-15 10:30:00 extra"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldAcceptTimestampWithFractionalSeconds() {
        String ts = "2026-01-15 10:30:00.123456789";
        assertThat(SnowflakeIdentifiers.validateTimestamp(ts)).isEqualTo(ts);
    }

    @Test
    void shouldAcceptTimestampWithTimezoneOffset() {
        String ts = "2026-01-15T10:30:00+00:00";
        assertThat(SnowflakeIdentifiers.validateTimestamp(ts)).isEqualTo(ts);
    }

    @Test
    void shouldAcceptTimestampWithCompactTimezone() {
        String ts = "2026-01-15T10:30:00+0000";
        assertThat(SnowflakeIdentifiers.validateTimestamp(ts)).isEqualTo(ts);
    }

    @Test
    void shouldAcceptTimestampWithNegativeTimezone() {
        String ts = "2026-01-15T10:30:00-05:00";
        assertThat(SnowflakeIdentifiers.validateTimestamp(ts)).isEqualTo(ts);
    }

    @Test
    void shouldAcceptTimestampWithSpaceSeparatedTimezone() {
        // Snowflake getCurrentTimestamp() returns this format
        String ts = "2026-04-06 22:00:08.503 -0700";
        assertThat(SnowflakeIdentifiers.validateTimestamp(ts)).isEqualTo(ts);
    }

    @Test
    void shouldAcceptBasicTimestampWithoutFractional() {
        String ts = "2026-01-15 10:30:00";
        assertThat(SnowflakeIdentifiers.validateTimestamp(ts)).isEqualTo(ts);
    }
}
