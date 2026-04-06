/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.snowflake;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;

public class SnowflakeConnection implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeConnection.class);

    private final Configuration config;
    private Connection jdbcConnection;

    public SnowflakeConnection(Configuration config) {
        this.config = config;
    }

    public void connect() throws SQLException {
        if (jdbcConnection != null && !jdbcConnection.isClosed()) {
            return;
        }

        String url = config.getString(SnowflakeConnectorConfig.URL_FIELD);
        String user = config.getString(SnowflakeConnectorConfig.USER_FIELD);
        String password = config.getString(SnowflakeConnectorConfig.PASSWORD_FIELD);
        String database = config.getString(SnowflakeConnectorConfig.DATABASE_FIELD);
        String schema = config.getString(SnowflakeConnectorConfig.SCHEMA_FIELD);
        String warehouse = config.getString(SnowflakeConnectorConfig.WAREHOUSE_FIELD);
        String role = config.getString(SnowflakeConnectorConfig.ROLE_FIELD);
        String authenticator = config.getString(SnowflakeConnectorConfig.AUTHENTICATOR_FIELD);

        // Build JDBC URL
        String jdbcUrl = buildJdbcUrl(url);

        Properties props = new Properties();
        props.put("user", user);
        if (password != null && !password.isEmpty()) {
            props.put("password", password);
        }
        if (database != null) {
            props.put("db", database);
        }
        if (schema != null) {
            props.put("schema", schema);
        }
        if (warehouse != null && !warehouse.isEmpty()) {
            props.put("warehouse", warehouse);
        }
        if (role != null && !role.isEmpty()) {
            props.put("role", role);
        }
        if (authenticator != null && !authenticator.isEmpty()) {
            props.put("authenticator", authenticator);
        }

        // Handle key pair authentication
        String privateKey = config.getString(SnowflakeConnectorConfig.PRIVATE_KEY_FIELD);
        String privateKeyFile = config.getString(SnowflakeConnectorConfig.PRIVATE_KEY_FILE_FIELD);
        if (privateKey != null && !privateKey.isEmpty()) {
            props.put("privateKey", privateKey);
            props.put("authenticator", "snowflake_jwt");
        }
        else if (privateKeyFile != null && !privateKeyFile.isEmpty()) {
            props.put("private_key_file", privateKeyFile);
            props.put("authenticator", "snowflake_jwt");
            String passphrase = config.getString(SnowflakeConnectorConfig.PRIVATE_KEY_PASSPHRASE_FIELD);
            if (passphrase != null && !passphrase.isEmpty()) {
                props.put("private_key_file_pwd", passphrase);
            }
        }

        LOGGER.info("Connecting to Snowflake at {}", jdbcUrl);
        this.jdbcConnection = DriverManager.getConnection(jdbcUrl, props);
        LOGGER.info("Connected to Snowflake successfully");
    }

    private String buildJdbcUrl(String url) {
        // Support both raw account URL and full JDBC URL
        if (url.startsWith("jdbc:snowflake://")) {
            return url;
        }
        // Strip protocol if present
        String host = url;
        if (host.startsWith("https://")) {
            host = host.substring("https://".length());
        }
        if (host.startsWith("http://")) {
            host = host.substring("http://".length());
        }
        // Remove trailing slash
        if (host.endsWith("/")) {
            host = host.substring(0, host.length() - 1);
        }
        return "jdbc:snowflake://" + host;
    }

    public Connection getJdbcConnection() throws SQLException {
        if (jdbcConnection == null || jdbcConnection.isClosed()) {
            connect();
        }
        return jdbcConnection;
    }

    public void execute(String sql) throws SQLException {
        LOGGER.debug("Executing SQL: {}", sql);
        try (Statement stmt = getJdbcConnection().createStatement()) {
            stmt.execute(sql);
        }
    }

    public List<Map<String, Object>> query(String sql) throws SQLException {
        LOGGER.debug("Querying: {}", sql);
        List<Map<String, Object>> results = new ArrayList<>();
        try (Statement stmt = getJdbcConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.put(meta.getColumnName(i), rs.getObject(i));
                }
                results.add(row);
            }
        }
        return results;
    }

    public boolean streamHasData(String streamName) throws SQLException {
        String sql = "SELECT SYSTEM$STREAM_HAS_DATA('" + streamName + "') AS HAS_DATA";
        List<Map<String, Object>> result = query(sql);
        if (!result.isEmpty()) {
            Object value = result.get(0).get("HAS_DATA");
            return "true".equalsIgnoreCase(String.valueOf(value));
        }
        return false;
    }

    public void createStream(String streamName, String tableName, String streamType,
                             String atTimestamp) throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE STREAM IF NOT EXISTS ").append(streamName);
        sql.append(" ON TABLE ").append(tableName);

        if ("APPEND_ONLY".equalsIgnoreCase(streamType)) {
            sql.append(" APPEND_ONLY = TRUE");
        }

        if (atTimestamp != null) {
            sql.append(" AT(TIMESTAMP => '").append(atTimestamp).append("'::TIMESTAMP_LTZ)");
        }

        execute(sql.toString());
        LOGGER.info("Created stream {} on table {}", streamName, tableName);
    }

    public void dropStream(String streamName) throws SQLException {
        execute("DROP STREAM IF EXISTS " + streamName);
        LOGGER.info("Dropped stream {}", streamName);
    }

    public List<Map<String, Object>> consumeStreamViaTemp(String streamName,
                                                           String tempTableName,
                                                           int maxRows) throws SQLException {
        // Step 1: Atomically consume stream into temp table
        String createSql = "CREATE OR REPLACE TEMPORARY TABLE " + tempTableName +
                " AS SELECT *, METADATA$ACTION, METADATA$ISUPDATE, METADATA$ROW_ID" +
                " FROM " + streamName;
        if (maxRows > 0) {
            createSql += " LIMIT " + maxRows;
        }

        execute("BEGIN");
        try {
            execute(createSql);
            execute("COMMIT");
        }
        catch (SQLException e) {
            try {
                execute("ROLLBACK");
            }
            catch (SQLException rollbackEx) {
                LOGGER.warn("Failed to rollback after stream consumption error", rollbackEx);
            }
            throw e;
        }

        // Step 2: Read changes from temp table
        List<Map<String, Object>> changes = query("SELECT * FROM " + tempTableName);

        // Step 3: Cleanup
        try {
            execute("DROP TABLE IF EXISTS " + tempTableName);
        }
        catch (SQLException e) {
            LOGGER.warn("Failed to drop temp table {}", tempTableName, e);
        }

        return changes;
    }

    public List<Map<String, Object>> queryChanges(String tableName, String fromTimestamp,
                                                   String toTimestamp) throws SQLException {
        String sql = "SELECT *, METADATA$ACTION, METADATA$ISUPDATE, METADATA$ROW_ID" +
                " FROM " + tableName +
                " CHANGES(INFORMATION => DEFAULT)" +
                " AT(TIMESTAMP => '" + fromTimestamp + "'::TIMESTAMP_LTZ)";
        if (toTimestamp != null) {
            sql += " END(TIMESTAMP => '" + toTimestamp + "'::TIMESTAMP_LTZ)";
        }
        return query(sql);
    }

    public List<Map<String, Object>> describeStream(String streamName) throws SQLException {
        return query("DESCRIBE STREAM " + streamName);
    }

    public List<Map<String, Object>> getTableColumns(String database, String schema,
                                                      String tableName) throws SQLException {
        String sql = "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, " +
                "NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE, COLUMN_DEFAULT, ORDINAL_POSITION " +
                "FROM " + database + ".INFORMATION_SCHEMA.COLUMNS " +
                "WHERE TABLE_SCHEMA = '" + schema + "' AND TABLE_NAME = '" + tableName + "' " +
                "ORDER BY ORDINAL_POSITION";
        return query(sql);
    }

    public List<Map<String, Object>> getPrimaryKeys(String schema, String tableName) throws SQLException {
        String sql = "SHOW PRIMARY KEYS IN TABLE " + schema + "." + tableName;
        return query(sql);
    }

    public List<Map<String, Object>> getTables(String database, String schema) throws SQLException {
        String sql = "SELECT TABLE_NAME FROM " + database + ".INFORMATION_SCHEMA.TABLES " +
                "WHERE TABLE_SCHEMA = '" + schema + "' AND TABLE_TYPE = 'BASE TABLE' " +
                "ORDER BY TABLE_NAME";
        return query(sql);
    }

    public String getCurrentTimestamp() throws SQLException {
        List<Map<String, Object>> result = query("SELECT CURRENT_TIMESTAMP()::VARCHAR AS TS");
        if (!result.isEmpty()) {
            return String.valueOf(result.get(0).get("TS"));
        }
        throw new SQLException("Failed to get current timestamp from Snowflake");
    }

    @Override
    public void close() {
        if (jdbcConnection != null) {
            try {
                jdbcConnection.close();
                LOGGER.info("Snowflake connection closed");
            }
            catch (SQLException e) {
                LOGGER.warn("Error closing Snowflake connection", e);
            }
        }
    }
}
