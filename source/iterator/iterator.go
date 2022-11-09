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
	"fmt"

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
	// keyColumns represents a name of the columns that iterator will use for setting key in record
	keyColumns []string
	// orderingColumn represents a name of column what iterator use for sorting data
	orderingColumn string
	// columns represents a list of table's columns for record payload.
	// if empty - will get all columns
	columns []string
	// batchSize represents a size of batch
	batchSize int
}

// Params represents an incoming iterator params for the New function.
type Params struct {
	Position       *Position
	URL            string
	Table          string
	OrderingColumn string
	KeyColumns     []string
	Columns        []string
	BatchSize      int
}

// New creates a new instance of the iterator.
func New(ctx context.Context, params Params) (*Iterator, error) {
	var err error

	iterator := &Iterator{
		table:          params.Table,
		orderingColumn: params.OrderingColumn,
		keyColumns:     params.KeyColumns,
		columns:        params.Columns,
		batchSize:      params.BatchSize,
	}

	iterator.repo, err = repository.New(params.URL)
	if err != nil {
		return nil, fmt.Errorf("new repository: %w", err)
	}

	err = iterator.populateKeyColumns(ctx)
	if err != nil {
		return nil, fmt.Errorf("populate key columns: %w", err)
	}

	switch position := params.Position; {
	case position == nil || position.Mode == ModeSnapshot:
		iterator.snapshot, err = NewSnapshot(ctx, SnapshotParams{
			Repo:           iterator.repo,
			Position:       params.Position,
			Table:          params.Table,
			OrderingColumn: params.OrderingColumn,
			KeyColumns:     iterator.keyColumns,
			Columns:        params.Columns,
			BatchSize:      params.BatchSize,
		})
		if err != nil {
			return nil, fmt.Errorf("init snapshot iterator: %w", err)
		}
	case position.Mode == ModeCDC:
		iterator.cdc, err = NewCDC(ctx, CDCParams{
			Repo:           iterator.repo,
			Position:       params.Position,
			Table:          params.Table,
			OrderingColumn: params.OrderingColumn,
			KeyColumns:     iterator.keyColumns,
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
func (iter *Iterator) HasNext(ctx context.Context) (bool, error) {
	switch {
	case iter.snapshot != nil:
		hasNext, err := iter.snapshot.HasNext(ctx)
		if err != nil {
			return false, fmt.Errorf("snapshot has next: %w", err)
		}

		if hasNext {
			return true, nil
		}

		if err = iter.switchToCDCIterator(ctx); err != nil {
			return false, fmt.Errorf("switch to cdc iterator: %w", err)
		}

		// to check if the next record exists via CDC iterator
		fallthrough

	case iter.cdc != nil:
		return iter.cdc.HasNext(ctx)

	default:
		return false, nil
	}
}

// Next returns the next record.
func (iter *Iterator) Next(ctx context.Context) (sdk.Record, error) {
	switch {
	case iter.snapshot != nil:
		return iter.snapshot.Next(ctx)

	case iter.cdc != nil:
		return iter.cdc.Next(ctx)

	default:
		return sdk.Record{}, errNoInitializedIterator
	}
}

// PushValueToDelete appends the last processed value to the slice to clear the tracking table in the future.
func (iter *Iterator) PushValueToDelete(position sdk.Position) error {
	pos, err := ParseSDKPosition(position)
	if err != nil {
		return fmt.Errorf("parse position: %w", err)
	}

	if pos.Mode == ModeCDC {
		return iter.cdc.pushValueToDelete(pos.LastProcessedVal)
	}

	return nil
}

// Close stops iterators and closes database connection.
func (iter *Iterator) Close() (err error) {
	if iter.snapshot != nil {
		err = iter.snapshot.Close()
	}

	if iter.cdc != nil {
		err = iter.cdc.Close()
	}

	return multierr.Append(err, iter.repo.Close())
}

// switchToCDCIterator stops Snapshot and initializes CDC iterator.
func (iter *Iterator) switchToCDCIterator(ctx context.Context) error {
	err := iter.snapshot.Close()
	if err != nil {
		return fmt.Errorf("stop snaphot iterator: %w", err)
	}

	iter.snapshot = nil

	iter.cdc, err = NewCDC(ctx, CDCParams{
		Repo:           iter.repo,
		Table:          iter.table,
		OrderingColumn: iter.orderingColumn,
		KeyColumns:     iter.keyColumns,
		Columns:        iter.columns,
		BatchSize:      iter.batchSize,
	})
	if err != nil {
		return fmt.Errorf("new cdc iterator: %w", err)
	}

	return nil
}

func (iter *Iterator) populateKeyColumns(ctx context.Context) error {
	if len(iter.keyColumns) != 0 {
		return nil
	}

	iter.keyColumns = []string{iter.orderingColumn}

	rows, err := iter.repo.DB.QueryxContext(ctx, querySelectPrimaryKeys, iter.table)
	if err != nil {
		return fmt.Errorf("select primary keys: %w", err)
	}

	keyColumn := ""
	for rows.Next() {
		if err = rows.Scan(&keyColumn); err != nil {
			return fmt.Errorf("scan key column value: %w", err)
		}

		iter.keyColumns = append(iter.keyColumns, keyColumn)
	}

	return nil
}
