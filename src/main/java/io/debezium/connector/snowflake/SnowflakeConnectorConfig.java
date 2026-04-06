/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.snowflake;

import java.util.List;
import java.util.Optional;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.SourceInfoStructMaker;

public class SnowflakeConnectorConfig extends CommonConnectorConfig {

    public static final String SNOWFLAKE_URL = "snowflake.url";
    public static final String SNOWFLAKE_USER = "snowflake.user";
    public static final String SNOWFLAKE_PASSWORD = "snowflake.password";
    public static final String SNOWFLAKE_PRIVATE_KEY = "snowflake.private.key";
    public static final String SNOWFLAKE_PRIVATE_KEY_FILE = "snowflake.private.key.file";
    public static final String SNOWFLAKE_PRIVATE_KEY_PASSPHRASE = "snowflake.private.key.passphrase";
    public static final String SNOWFLAKE_AUTHENTICATOR = "snowflake.authenticator";
    public static final String SNOWFLAKE_DATABASE = "snowflake.database";
    public static final String SNOWFLAKE_SCHEMA = "snowflake.schema";
    public static final String SNOWFLAKE_WAREHOUSE = "snowflake.warehouse";
    public static final String SNOWFLAKE_ROLE = "snowflake.role";

    public static final Field URL_FIELD = Field.create(SNOWFLAKE_URL)
            .withDisplayName("Snowflake URL")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withDescription("Snowflake account URL, e.g. https://myaccount.snowflakecomputing.com")
            .required();

    public static final Field USER_FIELD = Field.create(SNOWFLAKE_USER)
            .withDisplayName("Snowflake User")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withDescription("Snowflake username")
            .required();

    public static final Field PASSWORD_FIELD = Field.create(SNOWFLAKE_PASSWORD)
            .withDisplayName("Snowflake Password")
            .withType(Type.PASSWORD)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withDescription("Snowflake password (for username/password authentication)");

    public static final Field PRIVATE_KEY_FIELD = Field.create(SNOWFLAKE_PRIVATE_KEY)
            .withDisplayName("Snowflake Private Key")
            .withType(Type.PASSWORD)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("PEM-encoded private key content for key pair authentication");

    public static final Field PRIVATE_KEY_FILE_FIELD = Field.create(SNOWFLAKE_PRIVATE_KEY_FILE)
            .withDisplayName("Snowflake Private Key File")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("Path to PEM private key file for key pair authentication");

    public static final Field PRIVATE_KEY_PASSPHRASE_FIELD = Field.create(SNOWFLAKE_PRIVATE_KEY_PASSPHRASE)
            .withDisplayName("Snowflake Private Key Passphrase")
            .withType(Type.PASSWORD)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("Passphrase for encrypted private key");

    public static final Field AUTHENTICATOR_FIELD = Field.create(SNOWFLAKE_AUTHENTICATOR)
            .withDisplayName("Snowflake Authenticator")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDefault("snowflake")
            .withDescription("Authentication method: snowflake, snowflake_jwt, oauth, externalbrowser");

    public static final Field DATABASE_FIELD = Field.create(SNOWFLAKE_DATABASE)
            .withDisplayName("Snowflake Database")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withDescription("Snowflake database name")
            .required();

    public static final Field SCHEMA_FIELD = Field.create(SNOWFLAKE_SCHEMA)
            .withDisplayName("Snowflake Schema")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withDefault("PUBLIC")
            .withDescription("Snowflake schema name");

    public static final Field WAREHOUSE_FIELD = Field.create(SNOWFLAKE_WAREHOUSE)
            .withDisplayName("Snowflake Warehouse")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("Snowflake warehouse name for compute");

    public static final Field ROLE_FIELD = Field.create(SNOWFLAKE_ROLE)
            .withDisplayName("Snowflake Role")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("Snowflake role to assume");

    // CDC configuration
    public static final String CDC_MODE = "snowflake.cdc.mode";
    public static final String SNAPSHOT_MODE = "snapshot.mode";
    public static final String TABLE_INCLUDE_LIST = "table.include.list";
    public static final String TABLE_EXCLUDE_LIST = "table.exclude.list";
    public static final String STREAM_PREFIX = "snowflake.stream.prefix";
    public static final String STREAM_TYPE = "snowflake.stream.type";
    public static final String STREAM_MAX_ROWS_PER_POLL = "snowflake.stream.max.rows.per.poll";
    public static final String STREAM_STALE_HANDLING_MODE = "snowflake.stream.stale.handling.mode";
    public static final String STREAM_STALE_WARNING_DAYS = "snowflake.stream.stale.warning.days";

    public static final Field CDC_MODE_FIELD = Field.create(CDC_MODE)
            .withDisplayName("CDC Mode")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withDefault(CdcMode.STREAM.getValue())
            .withDescription("CDC mode: 'stream' uses Snowflake Streams with temp table consumption, " +
                    "'changes' uses the CHANGES clause with timestamp-based tracking");

    public static final Field SNAPSHOT_MODE_FIELD = Field.create(SNAPSHOT_MODE)
            .withDisplayName("Snapshot Mode")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withDefault(SnapshotMode.INITIAL.getValue())
            .withDescription("Snapshot mode: initial, always, never, when_needed");

    public static final Field TABLE_INCLUDE_LIST_FIELD = Field.create(TABLE_INCLUDE_LIST)
            .withDisplayName("Table Include List")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withDescription("Comma-separated list of table names to capture (e.g. SCHEMA.TABLE1,SCHEMA.TABLE2)");

    public static final Field TABLE_EXCLUDE_LIST_FIELD = Field.create(TABLE_EXCLUDE_LIST)
            .withDisplayName("Table Exclude List")
            .withType(Type.LIST)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("Comma-separated list of table names to exclude from capture");

    public static final Field STREAM_PREFIX_FIELD = Field.create(STREAM_PREFIX)
            .withDisplayName("Stream Prefix")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDefault("_DBZ_STREAM_")
            .withDescription("Prefix for auto-created Snowflake stream names");

    public static final Field STREAM_TYPE_FIELD = Field.create(STREAM_TYPE)
            .withDisplayName("Stream Type")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDefault("STANDARD")
            .withDescription("Snowflake stream type: STANDARD or APPEND_ONLY");

    public static final Field STREAM_MAX_ROWS_FIELD = Field.create(STREAM_MAX_ROWS_PER_POLL)
            .withDisplayName("Max Rows Per Poll")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(10000)
            .withDescription("Maximum number of rows to consume per poll cycle per table");

    public static final Field STREAM_STALE_HANDLING_FIELD = Field.create(STREAM_STALE_HANDLING_MODE)
            .withDisplayName("Stream Stale Handling Mode")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDefault(StaleHandlingMode.FAIL.getValue())
            .withDescription("How to handle stale streams: fail, recreate, skip");

    public static final Field STREAM_STALE_WARNING_DAYS_FIELD = Field.create(STREAM_STALE_WARNING_DAYS)
            .withDisplayName("Stream Stale Warning Days")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(2)
            .withDescription("Days before stream STALE_AFTER to emit a warning");

    private static final ConfigDef CONFIG_DEF = configDef();

    public static ConfigDef configDef() {
        ConfigDef config = new ConfigDef();

        // Connection group
        Field.group(config, "Connection",
                URL_FIELD, USER_FIELD, PASSWORD_FIELD,
                PRIVATE_KEY_FIELD, PRIVATE_KEY_FILE_FIELD, PRIVATE_KEY_PASSPHRASE_FIELD,
                AUTHENTICATOR_FIELD, DATABASE_FIELD, SCHEMA_FIELD,
                WAREHOUSE_FIELD, ROLE_FIELD);

        // CDC group
        Field.group(config, "CDC",
                CDC_MODE_FIELD, SNAPSHOT_MODE_FIELD,
                TABLE_INCLUDE_LIST_FIELD, TABLE_EXCLUDE_LIST_FIELD,
                STREAM_PREFIX_FIELD, STREAM_TYPE_FIELD, STREAM_MAX_ROWS_FIELD,
                STREAM_STALE_HANDLING_FIELD, STREAM_STALE_WARNING_DAYS_FIELD);

        return config;
    }

    private final CdcMode cdcMode;
    private final SnapshotMode snapshotMode;
    private final StaleHandlingMode staleHandlingMode;

    public SnowflakeConnectorConfig(Configuration config) {
        super(config, 10_000);
        this.cdcMode = CdcMode.parse(config.getString(CDC_MODE_FIELD));
        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE_FIELD));
        this.staleHandlingMode = StaleHandlingMode.parse(config.getString(STREAM_STALE_HANDLING_FIELD));
    }

    public String getSnowflakeUrl() {
        return getConfig().getString(URL_FIELD);
    }

    public String getSnowflakeUser() {
        return getConfig().getString(USER_FIELD);
    }

    public String getSnowflakePassword() {
        return getConfig().getString(PASSWORD_FIELD);
    }

    public String getSnowflakePrivateKey() {
        return getConfig().getString(PRIVATE_KEY_FIELD);
    }

    public String getSnowflakePrivateKeyFile() {
        return getConfig().getString(PRIVATE_KEY_FILE_FIELD);
    }

    public String getSnowflakePrivateKeyPassphrase() {
        return getConfig().getString(PRIVATE_KEY_PASSPHRASE_FIELD);
    }

    public String getSnowflakeAuthenticator() {
        return getConfig().getString(AUTHENTICATOR_FIELD);
    }

    public String getSnowflakeDatabase() {
        return getConfig().getString(DATABASE_FIELD);
    }

    public String getSnowflakeSchema() {
        return getConfig().getString(SCHEMA_FIELD);
    }

    public String getSnowflakeWarehouse() {
        return getConfig().getString(WAREHOUSE_FIELD);
    }

    public String getSnowflakeRole() {
        return getConfig().getString(ROLE_FIELD);
    }

    public CdcMode getCdcMode() {
        return cdcMode;
    }

    public SnapshotMode getSnapshotModeConfig() {
        return snapshotMode;
    }

    public List<String> getTableIncludeList() {
        return getConfig().getList(TABLE_INCLUDE_LIST_FIELD);
    }

    public List<String> getTableExcludeList() {
        return getConfig().getList(TABLE_EXCLUDE_LIST_FIELD);
    }

    public String getStreamPrefix() {
        return getConfig().getString(STREAM_PREFIX_FIELD);
    }

    public String getStreamType() {
        return getConfig().getString(STREAM_TYPE_FIELD);
    }

    public int getStreamMaxRowsPerPoll() {
        return getConfig().getInteger(STREAM_MAX_ROWS_FIELD);
    }

    public StaleHandlingMode getStaleHandlingMode() {
        return staleHandlingMode;
    }

    public int getStaleWarningDays() {
        return getConfig().getInteger(STREAM_STALE_WARNING_DAYS_FIELD);
    }

    @Override
    public String getContextName() {
        return Module.name();
    }

    @Override
    public String getConnectorName() {
        return Module.name();
    }

    @Override
    public EnumeratedValue getSnapshotMode() {
        return snapshotMode;
    }

    @Override
    public Optional<? extends EnumeratedValue> getSnapshotLockingMode() {
        return Optional.empty();
    }

    @Override
    protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
        SnowflakeSourceInfoStructMaker maker = new SnowflakeSourceInfoStructMaker();
        maker.init(Module.name(), Module.version(), this);
        return maker;
    }

    public enum CdcMode implements EnumeratedValue {
        STREAM("stream"),
        CHANGES("changes");

        private final String value;

        CdcMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public static CdcMode parse(String value) {
            for (CdcMode mode : values()) {
                if (mode.getValue().equalsIgnoreCase(value)) {
                    return mode;
                }
            }
            return STREAM;
        }
    }

    public enum SnapshotMode implements EnumeratedValue {
        INITIAL("initial"),
        ALWAYS("always"),
        NEVER("never"),
        WHEN_NEEDED("when_needed");

        private final String value;

        SnapshotMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public static SnapshotMode parse(String value) {
            for (SnapshotMode mode : values()) {
                if (mode.getValue().equalsIgnoreCase(value)) {
                    return mode;
                }
            }
            return INITIAL;
        }
    }

    public enum StaleHandlingMode implements EnumeratedValue {
        FAIL("fail"),
        RECREATE("recreate"),
        SKIP("skip");

        private final String value;

        StaleHandlingMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public static StaleHandlingMode parse(String value) {
            for (StaleHandlingMode mode : values()) {
                if (mode.getValue().equalsIgnoreCase(value)) {
                    return mode;
                }
            }
            return FAIL;
        }
    }
}
