# Conduit Connector Oracle

## General

Oracle connector is one of [Conduit](https://github.com/ConduitIO/conduit) plugins. It provides both, a source and
a destination Oracle connector.

Connector uses [godror - Go DRiver for ORacle](https://godror.github.io/godror/). This driver is required to be
installed. See [Godror Installation](https://godror.github.io/godror/doc/installation.html) for more information.

## Prerequisites

- [Go](https://go.dev/) 1.20
- (optional) [golangci-lint](https://github.com/golangci/golangci-lint) 1.50.1

## How to build it

Run `make build`.

## Testing

Run `make test` to run all unit and integration tests, as well as an acceptance test from the Conduit Connector SDK. To
run the integration and acceptance tests, set the Oracle database URL to the environment variables as an `ORACLE_URL`.

## Source

The Oracle Source connects to the database using the
provided [connection string](https://github.com/godror/godror#connection) `url` and starts creating records for each
table row and each detected change. The first time Source runs,
it [makes a snapshot](https://docs.oracle.com/cd/A87860_01/doc/server.817/a76959/mview.htm), creates a tracking table,
and a trigger to track changes in the `table`, and launches Snapshot mode. Then, when all the records have been read,
Source switches to CDC mode. More information [inside the Change Data Capture section](#change-data-capture).

### Snapshot

The connector in the Snapshot mode makes a
snapshot ([is a replica of a target table from a single point in time](https://docs.oracle.com/cd/A87860_01/doc/server.817/a76959/mview.htm#25271))
of a source table, and reads all rows from the table in batches via SELECT, FETCH NEXT and ORDER BY statements.

Example of a query:

```
SELECT {{columns...}}
FROM {{snapshot}}
ORDER BY {{orderingColumn}}
WHERE {{keyColumn}} > {{position.last_processed_val}}
FETCH NEXT {{batchSize}} ROWS ONLY;
```

This behavior is enabled by default, but can be turned off by adding `"snapshot": "false"` to the Source configuration.

When all records have been returned, the snapshot (a replica of a target table) is deleted and the connector switches to
the CDC mode.

### Change Data Capture

This connector implements CDC features for Oracle by adding a tracking table and a trigger to populate it. The tracking
table and trigger name have the same names as a source table with the prefix `CONDUIT`. The tracking table has all the
same columns as the source table plus three additional columns:

| name                          | description                                         |
|-------------------------------|-----------------------------------------------------|
| `CONDUIT_TRACKING_ID`         | Autoincrement index for the position.               |
| `CONDUIT_OPERATION_TYPE`      | Operation type: `insert`, `update`, or `delete`.    |
| `CONDUIT_TRACKING_CREATED_AT` | Date when the event was added to the tacking table. |

Every time data is added, changed, or deleted from the source table, this event will be written to the tracking table.

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

The `last_processed_val` field represents the last processed element value, and it depends on the iterator mode.

**Important**:

- if there is a need to change the columns in the source table, these changes must be made in the tracking table as
  well;
- if the tracking table was deleted, it will be recreated on the next start.

### Configuration Options

| Name             | Description                                                                                                                                                         | Required | Example                                     |
|------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|---------------------------------------------|
| `url`            | String line for connection to Oracle.                                                                                                                               | **true** | `username/password@path:1521/my.domain.com` |
| `table`          | The name of a table in the database that the connector should write to.                                                                                             | **true** | `users`                                     |
| `orderingColumn` | Column name that the connector will use for ordering rows. Column must contain unique values and suitable for sorting, otherwise the snapshot won't work correctly. | **true** | `created_at`                                |
| `keyColumns`     | Comma-separated list of column names to build the `sdk.Record.Key`. See more: [key handling](#key-handling).                                                        | false    | `id,name`                                   |
| `snapshot`       | Whether the connector will take a snapshot of the entire table before starting cdc mode. The default is `true`.                                                     | false    | `false`                                     |
| `columns`        | List of column names that should be included in each Record's payload, by default includes all columns.                                                             | false    | `id,name,age`                               |
| `batchSize`      | Size of rows batch. Min is 1 and max is 100000. The default is 1000.                                                                                                | false    | `100`                                       |
| `trackingPrefix` | A prefix added to the snapshot table, the tracking table and trigger name. The prefix will be upper-cased before being used. Default: `CONDUIT_`                    | false    | `custom_prefix_`                            |
| `snapshotTable`  | Snapshot table. Default: generated based on the prefix.                                                                                                             | false    | `custom_snapshot_table_`                    |
| `trackingTable`  | Name of the tracking table to be used in CDC. Default: generated based on the prefix.                                                                               | false    | `custom_tracking_table_`                    |
| `trigger`        | Name of the trigger to be used in CDC. Default: generated based on the prefix.                                                                                      | false    | `custom_trigger_`                           |

#### Key handling

The `keyColumns` is an optional field. If the field is empty, the connector makes a request to the database and uses the
received list of primary keys of the specified table. If the table does not contain primary keys, the connector uses the
value of the `orderingColumn` field as the `keyColumns` value.

## Destination

The Oracle Destination takes a `sdk.Record` and parses it into a valid SQL query. The Destination is designed to
handle different payloads and keys. Because of this, each record is individually parsed and upserted.

### Table name

If a record contains an `oracle.table` property in its metadata, it will be inserted in that table, otherwise, it will
fall back to use the table configured in the connector. Thus, a Destination can support multiple tables in a single
connector, as long as the user has proper access to those tables.

### Upsert Behavior

If the target table already contains a record with the same key, the Destination will still upsert the new record value.
Since keys must be unique, this can lead to overwriting and potential data loss, so the keys must be correctly assigned
from the Source.

### Configuration Options

| Name        | Description                                                                         | Required | Example                                     |
|-------------|-------------------------------------------------------------------------------------|----------|---------------------------------------------|
| `url`       | String line for connection to Oracle.                                               | **true** | `username/password@path:1521/my.domain.com` |
| `table`     | The name of a table in the database that the connector should write to, by default. | **true** | `users`                                     |
| `keyColumn` | Column name used to detect if the target table already contains the record.         | **true** | `id`                                        |

## Type convention

Type convention describes the conversion between Oracle to Go types.

| Oracle        | Go     | Explanation                                                                                   |
|---------------|--------|-----------------------------------------------------------------------------------------------|
| `NUMBER(1,0)` | `bool` | Oracle does not support a boolean type, so the best practice is to keep the values as 0 or 1. |

## Known limitations

Updating the configuration can cause completely unexpected results.
