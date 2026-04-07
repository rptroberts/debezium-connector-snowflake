# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
mvn clean test                    # Unit tests only (excludes @Tag("integration"))
mvn clean verify                  # Unit + integration tests (requires Snowflake env vars)
mvn clean package -DskipTests     # Build JAR without tests
mvn clean package -Passembly -DskipTests  # Build connector distribution archive
mvn clean install -Dquick         # Compile only, skip all tests
```

Run a single test class:
```bash
mvn test -Dtest=SnowflakeValueConvertersTest
```

Run a single integration test (requires env vars from `.env`):
```bash
mvn verify -Dtest=SnowflakeConnectorIT -DfailIfNoTests=false
```

Integration tests require these environment variables: `SNOWFLAKE_URL`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`, `SNOWFLAKE_WAREHOUSE`. A `.env` file exists (gitignored) with these values for the test Snowflake account.

## Architecture

This is a **standalone** Debezium source connector (not part of the Debezium monorepo). It follows the Kafka Connect SourceConnector SPI and Debezium conventions but manages its own dependency versions (debezium-core 3.1.0.Final, snowflake-jdbc 3.21.0, Kafka 3.7.0) because using `debezium-parent` as Maven parent causes BOM version resolution issues.

### CDC Pipeline Flow

```
SnowflakeConnector (entry point, config validation)
  └─ SnowflakeConnectorTask (extends SourceTask, not BaseSourceTask)
       └─ SnowflakeChangeEventSourceCoordinator (daemon thread)
            ├─ Phase 1: Snapshot (SELECT * AT(TIMESTAMP => ...))
            └─ Phase 2: Streaming (poll loop)
                 ├─ Stream mode: consumeStreamViaTemp() — atomic CTAS from Snowflake Stream
                 └─ Changes mode: queryChanges() — CHANGES clause with timestamp window
```

The coordinator runs snapshot + streaming in a background daemon thread, pushing `SourceRecord`s into a `LinkedBlockingQueue`. The task's `poll()` drains from this queue.

### Snowflake CDC Mechanics

Snowflake has no transaction log. CDC uses two mechanisms:

- **Stream mode** (default): Snowflake Streams track changes. Consumption is atomic via `CREATE TEMP TABLE AS SELECT * FROM stream` inside a transaction. Stream metadata columns (`METADATA$ACTION`, `METADATA$ISUPDATE`, `METADATA$ROW_ID`) are included in `SELECT *` — do NOT re-select them explicitly or you get duplicate column errors.
- **Changes mode**: Uses `CHANGES(INFORMATION => DEFAULT)` clause with timestamp-based AT/END window. Limited to time travel retention.

**UPDATE reconstruction**: Snowflake represents updates as DELETE+INSERT pairs with `METADATA$ISUPDATE=TRUE` and matching `METADATA$ROW_ID`. The coordinator groups these pairs and emits a single Debezium `u` event with `before` (DELETE row) and `after` (INSERT row).

### Key Design Decisions

- `SnowflakeConnectorTask` extends `SourceTask` directly (not Debezium's `BaseSourceTask`) to avoid complexity with Debezium's coordinator framework.
- `SnowflakeOffsetContext` implements `OffsetContext` directly (not `CommonOffsetContext`) because `CommonOffsetContext<T>` requires `T extends BaseSourceInfo`, not `AbstractSourceInfo`.
- `SnowflakeConnectorConfig.getSnapshotModeConfig()` is the local getter — `getSnapshotMode()` is the inherited abstract method returning `EnumeratedValue`.
- Snowflake JDBC must use `JDBC_QUERY_RESULT_FORMAT=JSON` to avoid Arrow/Netty `NoClassDefFoundError` in connector classloader isolation.
- `SnowflakeSourceInfoStructMaker` uses an `init(String, String, CommonConnectorConfig)` method, not constructor params.

## Test Structure

- **Unit tests** (`src/test/java`): `*Test.java` — run without Snowflake, excluded from `@Tag("integration")`
- **Integration tests**: `*IT.java` with `@Tag("integration")` — run via maven-failsafe-plugin, require real Snowflake account. Tests use `Assumptions.assumeTrue` to skip gracefully when env vars are missing.
- Integration tests create/drop a `DBZ_TEST_CDC` table and `_DBZ_STREAM_*` streams in setup/teardown.

## Testing After Changes

After any code change, always run the full test suite including integration tests:
```bash
set -a && source .env && set +a && mvn clean verify
```
This runs both unit tests (103) and integration/e2e tests (15) against a live Snowflake account. All 118 tests must pass before committing.

## Docker Compose

`docker-compose up -d` starts Zookeeper, Kafka, Schema Registry, Debezium Connect (with connector JAR volume-mounted), and Kafka UI (port 8080). Used for end-to-end manual testing with the full Kafka Connect runtime.
