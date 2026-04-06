/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.snowflake;

import java.util.Properties;

public final class Module {

    private static final Properties INFO = new Properties();
    private static final String VERSION;

    static {
        try {
            INFO.load(Module.class.getClassLoader().getResourceAsStream("io/debezium/connector/snowflake/build.version"));
        }
        catch (Exception e) {
            // ignore
        }
        VERSION = INFO.getProperty("version", "unknown");
    }

    public static String version() {
        return VERSION;
    }

    public static String name() {
        return "snowflake";
    }

    private Module() {
    }
}
