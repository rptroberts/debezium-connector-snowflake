# Debezium Connector for Snowflake

A [Debezium](https://debezium.io/) source connector that captures Change Data Capture (CDC) events from Snowflake tables and emits them as standard Debezium change events to Apache Kafka.

## Overview

Snowflake doesn't expose transaction logs like traditional databases. This connector uses two CDC mechanisms:

- **Stream mode** (default): Uses [Snowflake Streams](https://docs.snowflake.com/en/user-guide/streams-intro) to track table changes with atomic consumption via temporary tables
- **Changes mode**: Uses the [CHANGES clause](https://docs.snowflake.com/en/sql-reference/constructs/changes) with timestamp-based tracking within Snowflake's time travel window

## Features

- Initial snapshot + streaming CDC
- Standard Debezium envelope format (`before`, `after`, `source`, `op`, `ts_ms`)
- UPDATE reconstruction from Snowflake's DELETE+INSERT pairs
- Multiple authentication methods (password, key pair, OAuth)
- Stream staleness detection and configurable handling
- Configurable polling interval and batch sizes

## Installation

### Maven Dependency (GitHub Packages)

Add the GitHub Packages repository and dependency to your `pom.xml`:

```xml
<repositories>
    <repository>
        <id>github</id>
        <url>https://maven.pkg.github.com/rptroberts/debezium-connector-snowflake</url>
    </repository>
</repositories>

<dependencies>
    <dependency>
        <groupId>io.debezium</groupId>
        <artifactId>debezium-connector-snowflake</artifactId>
        <version>0.1.0</version>
    </dependency>
</dependencies>
```

GitHub Packages requires authentication. Add your credentials to `~/.m2/settings.xml`:

```xml
<settings>
  <servers>
    <server>
      <id>github</id>
      <username>YOUR_GITHUB_USERNAME</username>
      <password>YOUR_GITHUB_TOKEN</password>
    </server>
  </servers>
</settings>
```

The token needs the `read:packages` scope. See [GitHub docs](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-apache-maven-registry) for details.

### Kafka Connect Plugin (from GitHub Releases)

Download the latest release archive from the [Releases page](https://github.com/rptroberts/debezium-connector-snowflake/releases) and extract it to your Kafka Connect `plugin.path`:

```bash
tar -xzf debezium-connector-snowflake-*.tar.gz -C /path/to/kafka/connect/plugins/
```

### Build from Source

```bash
git clone https://github.com/rptroberts/debezium-connector-snowflake.git
cd debezium-connector-snowflake
mvn clean package -Passembly -DskipTests
# Extract target/debezium-connector-snowflake-*.tar.gz to your plugin.path
```

## Quick Start

### Prerequisites

- Java 17+
- Apache Kafka with Kafka Connect
- Snowflake account with appropriate permissions

### Configure

Register the connector with Kafka Connect:

```json
{
  "name": "snowflake-source",
  "config": {
    "connector.class": "io.debezium.connector.snowflake.SnowflakeConnector",
    "topic.prefix": "snowflake",
    "snowflake.url": "https://<account>.snowflakecomputing.com",
    "snowflake.user": "<username>",
    "snowflake.password": "<password>",
    "snowflake.database": "<database>",
    "snowflake.schema": "<schema>",
    "snowflake.warehouse": "<warehouse>",
    "table.include.list": "TABLE1,TABLE2",
    "snowflake.cdc.mode": "stream"
  }
}
```

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connector-config.json
```

## Configuration

### Connection

| Property | Required | Default | Description |
|---|---|---|---|
| `snowflake.url` | Yes | | Snowflake account URL |
| `snowflake.user` | Yes | | Username |
| `snowflake.password` | No | | Password |
| `snowflake.private.key` | No | | PEM private key content |
| `snowflake.private.key.file` | No | | Path to PEM private key file |
| `snowflake.authenticator` | No | `snowflake` | Auth method |
| `snowflake.database` | Yes | | Database name |
| `snowflake.schema` | No | `PUBLIC` | Schema name |
| `snowflake.warehouse` | No | | Warehouse name |
| `snowflake.role` | No | | Role to assume |

### CDC Behavior

| Property | Default | Description |
|---|---|---|
| `snowflake.cdc.mode` | `stream` | `stream` or `changes` |
| `snapshot.mode` | `initial` | `initial`, `always`, `never`, `when_needed` |
| `poll.interval.ms` | `10000` | Polling interval in milliseconds |
| `table.include.list` | | Comma-separated table names to capture |
| `table.exclude.list` | | Tables to exclude |
| `snowflake.stream.prefix` | `_DBZ_STREAM_` | Prefix for auto-created streams |
| `snowflake.stream.type` | `STANDARD` | `STANDARD` or `APPEND_ONLY` |
| `snowflake.stream.max.rows.per.poll` | `10000` | Max rows per poll per table |
| `snowflake.stream.stale.handling.mode` | `fail` | `fail`, `recreate`, `skip` |

## CDC Modes

### Stream Mode (default)

Uses Snowflake Streams for CDC. Changes are consumed atomically via temporary tables:

1. `CREATE TEMP TABLE AS SELECT * FROM stream` (consumes and advances offset)
2. Read changes from temp table
3. Drop temp table

**Trade-off**: At-most-once delivery risk if connector crashes between stream consumption and Kafka commit. Use Kafka Connect exactly-once support (Kafka 3.3+) to mitigate.

### Changes Mode

Uses the `CHANGES` clause to query change data within a time window:

```sql
SELECT * FROM table CHANGES(INFORMATION => DEFAULT) AT(TIMESTAMP => ...) END(TIMESTAMP => ...)
```

**Trade-off**: Limited to time travel retention (default 1 day, max 90 days). No stream staleness concern.

## Development

### Local Environment

```bash
# Start Kafka infrastructure
docker-compose up -d

# Build
mvn clean install

# Run unit tests only
mvn test

# Run integration tests (requires Snowflake account)
export SNOWFLAKE_URL=https://myaccount.snowflakecomputing.com
export SNOWFLAKE_USER=myuser
export SNOWFLAKE_PASSWORD=mypassword
export SNOWFLAKE_DATABASE=mydb
export SNOWFLAKE_SCHEMA=PUBLIC
export SNOWFLAKE_WAREHOUSE=COMPUTE_WH
mvn verify
```

### Running with Docker Compose

1. Build the connector: `mvn clean package -DskipTests`
2. Start infrastructure: `docker-compose up -d`
3. Register connector via the Connect REST API
4. Inspect topics at http://localhost:8080 (Kafka UI)

## Event Format

Events follow the standard Debezium envelope format:

```json
{
  "before": null,
  "after": {"ID": 1, "NAME": "Alice", "VALUE": 100.50},
  "source": {
    "version": "0.1.0-SNAPSHOT",
    "connector": "snowflake",
    "name": "my-server",
    "ts_ms": 1234567890,
    "db": "MYDB",
    "schema": "PUBLIC",
    "table": "USERS"
  },
  "op": "c",
  "ts_ms": 1234567891
}
```

Operations: `r` (snapshot read), `c` (create), `u` (update), `d` (delete)

## License

Apache License 2.0
