# Conduit Connector Oracle

## General

Oracle connector is one of [Conduit](https://github.com/ConduitIO/conduit) plugins. It provides both, a source and
destination Oracle connector.

## Prerequisites

- [Go](https://go.dev/) 1.18
- (optional) [golangci-lint](https://github.com/golangci/golangci-lint) 1.49.0

## How to build it

Run `make build`.

## Testing

Run `make test` to run all unit and integration tests, as well as an acceptance test. To make integration and acceptance
tests pass, set the Oracle database URL to the environment variables as an `ORACLE_URL`.

## Source

The Oracle Source connects to the database using the provided `url` and starts creating records for each table row and
each change detected.
The first time Source runs, it creates an additional table and a trigger to track changes in the `table` and launches
Snapshot mode. Then, when all the records have been read, Source switches to CDC mode.

### Snapshot

The connector in the Snapshot mode reads all rows from the table in batches via SELECT with fetching and ordering.

Example of a query:

```
SELECT {{columns...}}
FROM {{table}}
ORDER BY {{orderingColumn}}
WHERE {{keyColumn}} > {{position.lastProcessdValue}}
FETCH NEXT {{batchSize}} ROWS ONLY;
```

When all records have been returned, the connector switches to the CDC mode.

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

The queries to get change data from the tracking table look pretty similar to queries in the Snapshot mode, but
with `CONDUIT_TRACKING_ID` ordering column.

The Ack method collects the `CONDUIT_TRACKING_ID` of those records that have been successfully applied, in order to
remove them later in a batch (every 5 seconds or when the connector is closed).

### Position

Example of the position:

```
{
  "mode": "snapshot",
  "last_processed_val": 1
}
```

`mode` is a mode of an iterator (`snapshot` and `cdc`);

`last_processed_val` is a last processed element value of an ordering column.

### Configuration Options

| name             | description                                                                         | required |
|------------------|-------------------------------------------------------------------------------------|----------|
| `url`            | String line for connection to DB2.                                                  | **true** |
| `table`          | The name of a table in the database that the connector should write to, by default. | **true** |
| `keyColumn`      | Column name records should use for their `Key` fields.                              | **true** |
| `orderingColumn` | Column name of a column that the connector will use for ordering rows.              | **true** |
| `columns`        | List of column names that should be included in each Record's payload.              | false    |
| `batchSize`      | Size of rows batch. Min is 1 and max is 100000.                                     | false    |

## Destination

The Oracle Destination takes a `record.Record` and parses it into a valid SQL query. The Destination is designed to
handle different payloads and keys. Because of this, each record is individually parses and upsertes.

### Table name

If a record contains a `table` property in its metadata it will be inserted in that table, otherwise it will fall back
to use the table configured in the connector. This way the Destination can support multiple tables in the same
connector, provided the user has proper access to those tables.

### Upsert Behavior

If the target table already contains a record with the same key, the Destination will upsert with its current received
values. Because Keys must be unique, this can overwrite and thus potentially lose data, so keys should be assigned
correctly from the Source.

### Configuration Options

| name        | description                                                                         | required |
|-------------|-------------------------------------------------------------------------------------|----------|
| `url`       | String line for connection to DB2.                                                  | **true** |
| `table`     | The name of a table in the database that the connector should write to, by default. | **true** |
| `keyColumn` | Column name uses to detect if the target table already contains the record.         | false    |