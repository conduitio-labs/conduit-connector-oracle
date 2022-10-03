// Copyright © 2022 Meroxa, Inc.
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
	"hash/fnv"
	"strings"

	"github.com/conduitio-labs/conduit-connector-oracle/coltypes"
	"github.com/conduitio-labs/conduit-connector-oracle/repository"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"go.uber.org/multierr"
)

// Iterator represents an implementation of an iterator for Oracle.
type Iterator struct {
	repo     *repository.Oracle
	snapshot *Snapshot
	cdc      *CDC

	// table represents a table name
	table string
	// keyColumn represents a name of column what iterator use for setting key in record
	keyColumn string
	// orderingColumn represents a name of column what iterator use for sorting data
	orderingColumn string
	// columns represents a list of table's columns for record payload.
	// if empty - will get all columns
	columns []string
	// batchSize represents a size of batch
	batchSize int

	// columnTypes represents a columns' description from table
	columnTypes   map[string]coltypes.ColumnDescription
	hashedTable   uint32
	trackingTable string
	snapshotTable string
}

// Params represents an incoming iterator params for the New function.
type Params struct {
	Position       *Position
	URL            string
	Table          string
	KeyColumn      string
	OrderingColumn string
	Columns        []string
	BatchSize      int
}

// New creates a new instance of the iterator.
func New(ctx context.Context, params Params) (*Iterator, error) {
	var err error

	iterator := &Iterator{
		table:          params.Table,
		keyColumn:      params.KeyColumn,
		orderingColumn: params.OrderingColumn,
		columns:        params.Columns,
		batchSize:      params.BatchSize,
	}

	// hash the table name to use it as a postfix in the tracking table and snapshot,
	// because the maximum length of names (tables, triggers, etc.) is 30 characters
	h := fnv.New32a()
	h.Write([]byte(iterator.table))
	iterator.hashedTable = h.Sum32()

	iterator.snapshotTable = fmt.Sprintf("CONDUIT_SNAPSHOT_%d", iterator.hashedTable)
	iterator.trackingTable = fmt.Sprintf("CONDUIT_TRACKING_%d", iterator.hashedTable)

	iterator.repo, err = repository.New(params.URL)
	if err != nil {
		return nil, fmt.Errorf("new repository: %w", err)
	}

	// get column types of table for converting
	iterator.columnTypes, err = coltypes.GetColumnTypes(ctx, iterator.repo, params.Table)
	if err != nil {
		return nil, fmt.Errorf("get table column types: %w", err)
	}

	switch position := params.Position; {
	case position == nil || position.Mode == ModeSnapshot:
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

		// initialize tracking table
		err = iterator.initTrackingTable(ctx, tx)
		if err != nil {
			return nil, fmt.Errorf("initialize tracking table: %w", err)
		}

		err = tx.Commit()
		if err != nil {
			return nil, fmt.Errorf("commit db transaction: %w", err)
		}

		iterator.snapshot, err = NewSnapshot(ctx, SnapshotParams{
			Repo:           iterator.repo,
			Position:       params.Position,
			Table:          params.Table,
			SnapshotTable:  iterator.snapshotTable,
			KeyColumn:      params.KeyColumn,
			OrderingColumn: params.OrderingColumn,
			Columns:        params.Columns,
			BatchSize:      params.BatchSize,
			ColumnTypes:    iterator.columnTypes,
		})
		if err != nil {
			return nil, fmt.Errorf("init snapshot iterator: %w", err)
		}
	case position.Mode == ModeCDC:
		iterator.cdc, err = NewCDC(ctx, CDCParams{
			Repo:           iterator.repo,
			Position:       params.Position,
			Table:          params.Table,
			TrackingTable:  iterator.trackingTable,
			KeyColumn:      params.KeyColumn,
			OrderingColumn: params.OrderingColumn,
			Columns:        params.Columns,
			BatchSize:      params.BatchSize,
		})
		if err != nil {
			return nil, fmt.Errorf("init cdc iterator: %w", err)
		}

	default:
		return nil, fmt.Errorf("invalid position mode %q", params.Position.Mode)
	}

	return iterator, nil
}

// HasNext returns a bool indicating whether the iterator has the next record to return or not.
func (i *Iterator) HasNext(ctx context.Context) (bool, error) {
	switch {
	case i.snapshot != nil:
		hasNext, err := i.snapshot.HasNext(ctx)
		if err != nil {
			return false, fmt.Errorf("snapshot has next: %w", err)
		}

		if hasNext {
			return true, nil
		}

		if err = i.switchToCDCIterator(ctx); err != nil {
			return false, fmt.Errorf("switch to cdc iterator: %w", err)
		}

		// to check if the next record exists via CDC iterator
		fallthrough

	case i.cdc != nil:
		return i.cdc.HasNext(ctx)

	default:
		return false, nil
	}
}

// Next returns the next record.
func (i *Iterator) Next(ctx context.Context) (sdk.Record, error) {
	switch {
	case i.snapshot != nil:
		return i.snapshot.Next(ctx)

	case i.cdc != nil:
		return i.cdc.Next(ctx)

	default:
		return sdk.Record{}, errNoInitializedIterator
	}
}

// PushValueToDelete appends the last processed value to the slice to clear the tracking table in the future.
func (i *Iterator) PushValueToDelete(position sdk.Position) error {
	pos, err := ParseSDKPosition(position)
	if err != nil {
		return fmt.Errorf("parse position: %w", err)
	}

	if pos.Mode == ModeCDC {
		return i.cdc.pushValueToDelete(pos.LastProcessedVal)
	}

	return nil
}

// Close stops iterators and closes database connection.
func (i *Iterator) Close() (err error) {
	if i.snapshot != nil {
		err = i.snapshot.Close()
	}

	if i.cdc != nil {
		err = i.cdc.Close()
	}

	return multierr.Append(err, i.repo.Close())
}

// initSnapshotTable creates a new snapshot table, if id does not exist.
func (i *Iterator) initSnapshotTable(ctx context.Context, tx *sql.Tx) error {
	exists, err := i.checkIfTableExists(ctx, tx, i.snapshotTable)
	if err != nil {
		return fmt.Errorf("check if table exists: %w", err)
	}

	if exists {
		return nil
	}

	// create a snapshot table
	_, err = tx.ExecContext(ctx, fmt.Sprintf(querySnapshotTable, i.snapshotTable, i.table))
	if err != nil {
		return fmt.Errorf("create snapshot: %w", err)
	}

	return nil
}

// initTrackingTable creates a new tracking table and trigger, if they do not exist.
func (i *Iterator) initTrackingTable(ctx context.Context, tx *sql.Tx) error {
	exists, err := i.checkIfTableExists(ctx, tx, i.trackingTable)
	if err != nil {
		return fmt.Errorf("check if table exists: %w", err)
	}

	if exists {
		return nil
	}

	// create a copy of table
	_, err = tx.ExecContext(ctx, fmt.Sprintf(queryTableCopy, i.trackingTable, i.table, i.table))
	if err != nil {
		return fmt.Errorf("create copy of table: %w", err)
	}

	// add tracking columns to a tracking table
	_, err = tx.ExecContext(ctx, buildToExpandTrackingTableQuery(i.trackingTable))
	if err != nil {
		return fmt.Errorf("expand tracking table with conduit columns: %w", err)
	}

	// add trigger
	_, err = tx.ExecContext(ctx, buildCreateTriggerQuery(
		fmt.Sprintf("CONDUIT_%d", i.hashedTable),
		i.table,
		i.trackingTable,
		i.columnTypes,
		i.columns,
	))
	if err != nil {
		return fmt.Errorf("create trigger: %w", err)
	}

	return nil
}

// buildToExpandTrackingTableQuery returns a query to expand the tracking table.
func buildToExpandTrackingTableQuery(trackingTable string) string {
	return fmt.Sprintf(queryTrackingTableExtendWithConduitColumns, trackingTable, columnTrackingID, columnOperationType,
		columnOperationType, columnTimeCreatedAt, trackingTable, columnTrackingID)
}

// buildCreateTriggerQuery returns a create trigger query.
func buildCreateTriggerQuery(
	name string,
	table string,
	trackingTable string,
	columnTypes map[string]coltypes.ColumnDescription,
	columns []string,
) string {
	var columnNames []string

	if columns != nil {
		columnNames = append(columnNames, columns...)
	} else {
		for key := range columnTypes {
			columnNames = append(columnNames, key)
		}
	}

	newValues := make([]string, len(columnNames))
	oldValues := make([]string, len(columnNames))
	for i := range columnNames {
		newValues[i] = fmt.Sprintf("%s%s", referencingNew, columnNames[i])
		oldValues[i] = fmt.Sprintf("%s%s", referencingOld, columnNames[i])
	}

	insertOnInsertingOrUpdating := fmt.Sprintf(queryTriggerInsertPart, trackingTable,
		strings.Join(columnNames, ","), columnOperationType, strings.Join(newValues, ","))
	insertOnDeleting := fmt.Sprintf(queryTriggerInsertPart, trackingTable,
		strings.Join(columnNames, ","), columnOperationType, strings.Join(oldValues, ","))

	return fmt.Sprintf(queryTriggerCreate, name, table, insertOnInsertingOrUpdating, insertOnDeleting)
}

// dropSnapshotTable drops a snapshot, if it exists.
func (i *Iterator) dropSnapshotTable(ctx context.Context) error {
	tx, err := i.repo.DB.Begin()
	if err != nil {
		return fmt.Errorf("begin db transaction: %w", err)
	}
	defer tx.Rollback() // nolint:errcheck,nolintlint

	exists, err := i.checkIfTableExists(ctx, tx, i.snapshotTable)
	if err != nil {
		return fmt.Errorf("check if table exists: %w", err)
	}

	if !exists {
		return nil
	}

	_, err = tx.ExecContext(ctx, fmt.Sprintf("DROP SNAPSHOT %s", i.snapshotTable))
	if err != nil {
		return fmt.Errorf("exec drop snapshot: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("commit db transaction: %w", err)
	}

	return nil
}

// checkIfTableExists checks if table exist.
func (i *Iterator) checkIfTableExists(ctx context.Context, tx *sql.Tx, table string) (bool, error) {
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(queryIfTableExists, table))
	if err != nil {
		return false, fmt.Errorf("request with check if the table already exists: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		err = rows.Scan(&name)
		if err != nil {
			return false, fmt.Errorf("scan tables to check if the table already exists: %w", err)
		}

		if name == table {
			// table exists
			return true, nil
		}
	}

	return false, nil
}

// switchToCDCIterator stops Snapshot and initializes CDC iterator.
func (i *Iterator) switchToCDCIterator(ctx context.Context) error {
	err := i.snapshot.Close()
	if err != nil {
		return fmt.Errorf("stop snaphot iterator: %w", err)
	}

	i.snapshot = nil

	// drop a snapshot
	err = i.dropSnapshotTable(ctx)
	if err != nil {
		return fmt.Errorf("drop snapshot: %w", err)
	}

	i.cdc, err = NewCDC(ctx, CDCParams{
		Repo:           i.repo,
		Table:          i.table,
		TrackingTable:  i.trackingTable,
		KeyColumn:      i.keyColumn,
		OrderingColumn: i.orderingColumn,
		Columns:        i.columns,
		BatchSize:      i.batchSize,
	})
	if err != nil {
		return fmt.Errorf("new cdc iterator: %w", err)
	}

	return nil
}
