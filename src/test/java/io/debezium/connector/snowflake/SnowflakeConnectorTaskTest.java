/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.snowflake;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SnowflakeConnectorTaskTest {

    @Test
    void shouldReturnVersion() {
        SnowflakeConnectorTask task = new SnowflakeConnectorTask();
        assertThat(task.version()).isNotNull();
    }

    @Test
    void shouldStopGracefullyWhenNeverStarted() {
        SnowflakeConnectorTask task = new SnowflakeConnectorTask();
        // stop() should not throw even if start() was never called
        task.stop();
    }
}
