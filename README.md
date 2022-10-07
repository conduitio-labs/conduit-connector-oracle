# Conduit Connector Oracle

## General

Oracle connector is one of [Conduit](https://github.com/ConduitIO/conduit) plugins. It provides both, a source and
a destination Oracle connector.

Connector uses [godror - Go DRiver for ORacle](https://godror.github.io/godror/). This driver is required to be
installed. See [Godror Installation](https://godror.github.io/godror/doc/installation.html) for more information.

## Prerequisites

- [Go](https://go.dev/) 1.18
- (optional) [golangci-lint](https://github.com/golangci/golangci-lint) 1.49.0
- (optional) [mock](https://github.com/golang/mock) 1.6.0

## How to build it

Run `make build`.

## Testing

Run `make test` to run all unit and integration tests, as well as an acceptance test. To pass the integration and
acceptance tests, set the Oracle database URL to the environment variables as an `ORACLE_URL`.

## Source

The Oracle Source connects to the database using the provided `url` and starts creating records for each table row and
each detected change. The first time Source runs,
it [makes a snapshot](https://docs.oracle.com/cd/A87860_01/doc/server.817/a76959/mview.htm), creates a tracking table,
and a trigger to track changes in the `table`, and launches Snapshot mode. Then, when all the records have been read,
Source switches to CDC mode. More information [inside the Change Data Capture section](#change-data-capture).

### Snapshot

The connector in the Snapshot mode makes a snapshot of target table, and reads all rows from the table in batches via
SELECT, FETCH NEXT and ORDER BY statements.

Example of a query:

```
SELECT {{columns...}}
FROM {{snapshot}}
ORDER BY {{orderingColumn}}
WHERE {{keyColumn}} > {{position.last_processed_val}}
FETCH NEXT {{batchSize}} ROWS ONLY;
```

When all records have been returned, the snapshot is deleted and the connector switches to the CDC mode.

### Change Data Capture

This connector implements CDC features for Oracle by adding a tracking table and a trigger to populate it. The tracking
table and trigger name have the same names as a target table wit the prefix `CONDUIT`. The tracking table has all the
same columns as the target table plus three additional columns:

| name                          | description                                          |
|-------------------------------|------------------------------------------------------|
| `CONDUIT_TRACKING_ID`         | Autoincrement index for the position.                |
| `CONDUIT_OPERATION_TYPE`      | Operation type: `insert`, `update`, or `delete`.     |
| `CONDUIT_TRACKING_CREATED_AT` | Date when the event was added to the tacking table.  |

Every time data is added, changed, or deleted from the target table, this event will be written to the tracking table.

Queries to retrieve change data from a tracking table are very similar to queries in a Snapshot mode, but
with `CONDUIT_TRACKING_ID` ordering column.

The Ack method collects the `CONDUIT_TRACKING_ID` of those records that have been successfully applied, in order to
remove them later in a batch from the tracking table (every 5 seconds or when the connector is closed).

### Position

Example of the position:

```
{
  "mode": "snapshot",
  "last_processed_val": 1
}
```

The `mode` field represents a mode of the iterator (`snapshot` or `cdc`).

The `last_processed_val` field represents the last processed element value, and it depends on the iterator mode. For the
Snapshot mode it is the value from `orderingColumn` column you chose. This means that the `orderingColumn` must contain
unique values and suitable for sorting, otherwise the snapshot won't work correctly. For the CDC mode it is the value
from `CONDUIT_TRACKING_ID` column of the tracking table (more
information [inside the Change Data Capture section](#change-data-capture)).

**Important**:

- if there is a need to change the columns in the target table, these changes must be made in the tracking table as
  well;
- if the tracking table was deleted, it will be recreated on the next start.

### Configuration Options

| name             | description                                                                                            | required | example                                     |
|------------------|--------------------------------------------------------------------------------------------------------|----------|---------------------------------------------|
| `url`            | string line for connection to Oracle                                                                   | **true** | `username/password@path:1521/my.domain.com` |
| `table`          | the name of a table in the database that the connector should write to                                 | **true** | `users`                                     |
| `keyColumn`      | column name records should use for their `Key` fields                                                  | **true** | `id`                                        |
| `orderingColumn` | column name of a column that the connector will use for ordering rows                                  | **true** | `created_at`                                |
| `columns`        | list of column names that should be included in each Record's payload, by default includes all columns | false    | `id,name,age`                               |
| `batchSize`      | size of rows batch. Min is 1 and max is 100000                                                         | false    | `100`                                       |

## Destination

The Oracle Destination takes a `sdk.Record` and parses it into a valid SQL query. The Destination is designed to
handle different payloads and keys. Because of this, each record is individually parsed and upserted.

### Table name

If a record contains a `table` property in its metadata, it will be inserted in that table, otherwise, it will fall back
to use the table configured in the connector. Thus, a Destination can support multiple tables in a single connector, as
long as the user has proper access to those tables.

### Upsert Behavior

If the target table already contains a record with the same key, the Destination will still upsert the new record value.
Since keys must be unique, this can lead to overwriting and potential data loss, so the keys must be correctly assigned
from the Source.

### Configuration Options

| name        | description                                                                        | required | example                                     |
|-------------|------------------------------------------------------------------------------------|----------|---------------------------------------------|
| `url`       | string line for connection to Oracle                                               | **true** | `username/password@path:1521/my.domain.com` |
| `table`     | the name of a table in the database that the connector should write to, by default | **true** | `users`                                     |
| `keyColumn` | column name used to detect if the target table already contains the record         | **true** | `id`                                        |

## Type convention

Type convention describes the conversion between Oracle to Go types.

| oracle        | go     | explanation                                                                                  |
|---------------|--------|----------------------------------------------------------------------------------------------|
| `NUMBER(1,0)` | `bool` | oracle does not support a boolean type, so the best practice is to keep the values as 0 or 1 |

## Known limitations

Changing a table name during a connector update can cause quite unexpected results. Therefore, it's highly not
recommended to do this.

Creating two source or destination connectors using the same table is not allowed.