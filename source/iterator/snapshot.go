// Copyright © 2022 Meroxa, Inc. & Yalantis
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package iterator

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/conduitio-labs/conduit-connector-oracle/columntypes"
	"github.com/conduitio-labs/conduit-connector-oracle/repository"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"
)

const querySnapshotTable = "CREATE SNAPSHOT %s AS SELECT * FROM %s"

// Snapshot represents an implementation of a Snapshot iterator for Oracle.
type Snapshot struct {
	repo     *repository.Oracle
	position *Position

	// table represents a table name
	table string
	// snapshotTable represents a name of snapshot table
	snapshotTable string
	// trackingTable represents a tracking table name
	trackingTable string
	// trigger represents a trigger name for a trackingTable
	trigger string
	// orderingColumn represents a name of column what iterator use for sorting data
	orderingColumn string
	// keyColumns represents a name of the columns that iterator will use for setting key in record
	keyColumns []string
	// columns represents a list of table's columns for record payload.
	// if empty - will get all columns
	columns []string
	// batchSize represents a size of batch
	batchSize int

	rows *sqlx.Rows
	// columnTypes represents a columns' description from table
	columnTypes map[string]columntypes.ColumnDescription
}

// SnapshotParams represents an incoming params for the NewSnapshot function.
type SnapshotParams struct {
	Repo           *repository.Oracle
	Position       *Position
	Table          string
	SnapshotTable  string
	TrackingTable  string
	Trigger        string
	OrderingColumn string
	KeyColumns     []string
	Columns        []string
	BatchSize      int
	ColumnTypes    map[string]columntypes.ColumnDescription
}

// NewSnapshot creates a new instance of the Snapshot iterator.
func NewSnapshot(ctx context.Context, params SnapshotParams) (*Snapshot, error) {
	var err error

	iterator := &Snapshot{
		repo:           params.Repo,
		position:       params.Position,
		table:          params.Table,
		snapshotTable:  params.SnapshotTable,
		trackingTable:  params.TrackingTable,
		trigger:        params.Trigger,
		orderingColumn: params.OrderingColumn,
		keyColumns:     params.KeyColumns,
		columns:        params.Columns,
		batchSize:      params.BatchSize,
		columnTypes:    params.ColumnTypes,
	}

	tx, err := iterator.repo.DB.Begin()
	if err != nil {
		return nil, fmt.Errorf("begin db transaction: %w", err)
	}
	defer tx.Rollback() // nolint:errcheck,nolintlint

	// initialize a snapshot
	err = iterator.initSnapshotTable(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("initialize snapshot: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("commit db transaction: %w", err)
	}

	err = iterator.loadRows(ctx)
	if err != nil {
		return nil, fmt.Errorf("load rows: %w", err)
	}

	return iterator, nil
}

// HasNext returns a bool indicating whether the iterator has the next record to return or not.
func (iter *Snapshot) HasNext(ctx context.Context) (bool, error) {
	if iter.rows != nil && iter.rows.Next() {
		return true, nil
	}

	if err := iter.loadRows(ctx); err != nil {
		return false, fmt.Errorf("load rows: %w", err)
	}

	hasNext := iter.rows.Next()
	if !hasNext {
		// drop a snapshot
		err := iter.dropSnapshotTable(ctx)
		if err != nil {
			return hasNext, fmt.Errorf("drop snapshot: %w", err)
		}
	}

	return hasNext, nil
}

// Next returns the next record.
func (iter *Snapshot) Next(_ context.Context) (opencdc.Record, error) {
	row := make(map[string]any)
	if err := iter.rows.MapScan(row); err != nil {
		return opencdc.Record{}, fmt.Errorf("scan rows: %w", err)
	}

	transformedRow, err := columntypes.TransformRow(row, iter.columnTypes)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("transform row column types: %w", err)
	}

	if _, ok := transformedRow[iter.orderingColumn]; !ok {
		return opencdc.Record{}, errOrderingColumnIsNotExist
	}

	key := make(opencdc.StructuredData)
	for i := range iter.keyColumns {
		val, ok := transformedRow[iter.keyColumns[i]]
		if !ok {
			return opencdc.Record{}, fmt.Errorf("key column %q not found", iter.keyColumns[i])
		}

		key[iter.keyColumns[i]] = val
	}

	// set a new position into the variable,
	// to avoid saving position into the struct until we marshal the position
	position := &Position{
		Mode: ModeSnapshot,
		// set the value from iter.orderingColumn column you chose
		LastProcessedVal: transformedRow[iter.orderingColumn],
		SnapshotTable:    iter.snapshotTable,
		TrackingTable:    iter.trackingTable,
		Trigger:          iter.trigger,
	}

	convertedPosition, err := position.ToSDK()
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("convert position %w", err)
	}

	iter.position = position

	metadata := opencdc.Metadata{
		metadataTable: iter.table,
	}
	metadata.SetCreatedAt(time.Now())

	r := sdk.Util.Source.NewRecordSnapshot(
		convertedPosition,
		metadata,
		key,
		opencdc.StructuredData(transformedRow),
	)

	return r, nil
}

// Close closes database rows of Snapshot iterator.
func (iter *Snapshot) Close() error {
	if iter.rows != nil {
		return iter.rows.Close()
	}

	return nil
}

// checkIfTableExists checks if table exist.
func (iter *Snapshot) checkIfTableExists(ctx context.Context, tx *sql.Tx, table string) (bool, error) {
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(queryIfTableExists, table))
	if err != nil {
		return false, fmt.Errorf("request with check if the table already exists: %w", err)
	}
	defer rows.Close()

	exists := rows.Next()

	if rows.Err() != nil {
		return false, fmt.Errorf("table exists: %w", err)
	}

	return exists, nil
}

// initSnapshotTable creates a new snapshot table, if id does not exist.
func (iter *Snapshot) initSnapshotTable(ctx context.Context, tx *sql.Tx) error {
	exists, err := iter.checkIfTableExists(ctx, tx, iter.snapshotTable)
	if err != nil {
		return fmt.Errorf("check if table exists: %w", err)
	}

	if exists {
		return nil
	}

	// create a snapshot table
	_, err = tx.ExecContext(ctx, fmt.Sprintf(querySnapshotTable, iter.snapshotTable, iter.table))
	if err != nil {
		return fmt.Errorf("create snapshot: %w", err)
	}

	return nil
}

// dropSnapshotTable drops a snapshot, if it exists.
func (iter *Snapshot) dropSnapshotTable(ctx context.Context) error {
	tx, err := iter.repo.DB.Begin()
	if err != nil {
		return fmt.Errorf("begin db transaction: %w", err)
	}
	defer tx.Rollback() // nolint:errcheck,nolintlint

	exists, err := iter.checkIfTableExists(ctx, tx, iter.snapshotTable)
	if err != nil {
		return fmt.Errorf("check if table exists: %w", err)
	}

	if !exists {
		return nil
	}

	_, err = tx.ExecContext(ctx, fmt.Sprintf("DROP SNAPSHOT %s", iter.snapshotTable))
	if err != nil {
		return fmt.Errorf("exec drop snapshot: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("commit db transaction: %w", err)
	}

	return nil
}

// LoadRows selects a batch of rows from a database, based on the
// table, columns, orderingColumn, batchSize and the current position.
func (iter *Snapshot) loadRows(ctx context.Context) error {
	columns := "*"
	if len(iter.columns) > 0 {
		columns = strings.Join(iter.columns, ",")
	}

	whereClause := ""
	args := make([]any, 0)
	if iter.position != nil {
		whereClause = fmt.Sprintf(" WHERE %s > :1", iter.orderingColumn)
		args = append(args, iter.position.LastProcessedVal)
	}

	query := fmt.Sprintf(querySelectRowsFmt, columns, iter.snapshotTable, whereClause, iter.orderingColumn, iter.batchSize)

	rows, err := iter.repo.DB.QueryxContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("execute select query %q, %v: %w", query, args, err)
	}

	iter.rows = rows

	return nil
}
