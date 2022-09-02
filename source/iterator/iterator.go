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
	"fmt"
	"hash/fnv"

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

	// columnTypes represents a columns' data from table
	columnTypes   map[string]coltypes.ColumnData
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

	// hash the table name to use it in tracking table and snapshot
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
		// initialize a snapshot
		err = iterator.initSnapshotTable(ctx)
		if err != nil {
			return nil, fmt.Errorf("initialize snapshot: %w", err)
		}

		// initialize tracking table
		err = iterator.initTrackingTable(ctx)
		if err != nil {
			return nil, fmt.Errorf("initialize tracking table: %w", err)
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

// initSnapshotTable formats snapshot name and returns if it's not the first start.
// If it is - creates a new snapshot.
func (i *Iterator) initSnapshotTable(ctx context.Context) error {
	tx, err := i.repo.DB.Begin()
	if err != nil {
		return fmt.Errorf("begin db transaction: %w", err)
	}
	defer tx.Rollback() // nolint:errcheck,nolintlint

	// check if the snapshot exists
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(queryTableIsExists, i.snapshotTable))
	if err != nil {
		return fmt.Errorf("request with check if the snapshot already exists: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		err = rows.Scan(&name)
		if err != nil {
			return fmt.Errorf("scan tables to check if the snapshot already exists: %w", err)
		}

		if name == i.snapshotTable {
			// table exists, initialization is not needed
			return nil
		}
	}

	// create a snapshot
	_, err = tx.ExecContext(ctx, fmt.Sprintf(querySnapshotTable, i.snapshotTable, i.table))
	if err != nil {
		return fmt.Errorf("create snapshot: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("commit db transaction: %w", err)
	}

	return nil
}

// initTrackingTable formats tracking table name and returns if it's not the first start.
// If it is - creates a new tracking table with trigger.
func (i *Iterator) initTrackingTable(ctx context.Context) error {
	tx, err := i.repo.DB.Begin()
	if err != nil {
		return fmt.Errorf("begin db transaction: %w", err)
	}
	defer tx.Rollback() // nolint:errcheck,nolintlint

	// check if the table exists
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(queryTableIsExists, i.trackingTable))
	if err != nil {
		return fmt.Errorf("request with check if the table exists: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		err = rows.Scan(&name)
		if err != nil {
			return fmt.Errorf("scan table name to check if the table exists: %w", err)
		}

		if name == i.trackingTable {
			// table exists, initialization is not needed
			return nil
		}
	}

	// create a copy of table
	_, err = tx.ExecContext(ctx, fmt.Sprintf(queryTableCopy, i.trackingTable, i.table, i.table))
	if err != nil {
		return fmt.Errorf("create copy of table: %w", err)
	}

	// add tracking columns to a tracking table
	_, err = tx.ExecContext(ctx, buildExpandTrackingTableQuery(i.trackingTable))
	if err != nil {
		return fmt.Errorf("expand tracking table with conduit columns: %w", err)
	}

	// add trigger
	_, err = tx.ExecContext(ctx, buildCreateTriggerQuery(buildCreateTriggerParams{
		name:          fmt.Sprintf("CONDUIT_%d", i.hashedTable),
		table:         i.table,
		trackingTable: i.trackingTable,
		columnTypes:   i.columnTypes,
		columns:       i.columns,
	}))
	if err != nil {
		return fmt.Errorf("create trigger: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("commit db transaction: %w", err)
	}

	return nil
}

// switchToCDCIterator stops Snapshot and initializes CDC iterator.
func (i *Iterator) switchToCDCIterator(ctx context.Context) error {
	err := i.snapshot.Close()
	if err != nil {
		return fmt.Errorf("stop snaphot iterator: %w", err)
	}

	i.snapshot = nil

	// drop a snapshot
	_, err = i.repo.DB.ExecContext(ctx, fmt.Sprintf("DROP SNAPSHOT %s", i.snapshotTable))
	if err != nil {
		return fmt.Errorf("exec drop snapshot: %w", err)
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
