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

	"github.com/conduitio-labs/conduit-connector-oracle/coltypes"
	"github.com/conduitio-labs/conduit-connector-oracle/repository"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Iterator represents an implementation of an iterator for Oracle.
type Iterator struct {
	repo     *repository.Oracle
	snapshot *Snapshot
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
	var (
		iterator = new(Iterator)
		err      error
	)

	iterator.repo, err = repository.New(params.URL)
	if err != nil {
		return nil, fmt.Errorf("new repository: %w", err)
	}

	// get column types for converting.
	columnTypes, err := coltypes.GetColumnTypes(ctx, iterator.repo, params.Table)
	if err != nil {
		return nil, fmt.Errorf("get table column types: %w", err)
	}

	if params.Position == nil || params.Position.Mode == ModeSnapshot {
		iterator.snapshot, err = NewSnapshot(ctx, SnapshotParams{
			Repo:           iterator.repo,
			Position:       params.Position,
			Table:          params.Table,
			KeyColumn:      params.KeyColumn,
			OrderingColumn: params.OrderingColumn,
			Columns:        params.Columns,
			BatchSize:      params.BatchSize,
			ColumnTypes:    columnTypes,
		})
		if err != nil {
			return nil, fmt.Errorf("new snapshot: %w", err)
		}
	}

	return iterator, nil
}

// HasNext returns a bool indicating whether the iterator has the next record to return or not.
func (i Iterator) HasNext(ctx context.Context) (bool, error) {
	return i.snapshot.HasNext(ctx)
}

// Next returns the next record.
func (i Iterator) Next(ctx context.Context) (sdk.Record, error) {
	return i.snapshot.Next(ctx)
}

// Stop stops iterators.
func (i Iterator) Stop() error {
	err := i.snapshot.Stop()
	if err != nil {
		return fmt.Errorf("stop snapshot: %w", err)
	}

	return i.repo.Close()
}

// Ack check if record with position was recorded.
func (i Iterator) Ack(ctx context.Context, position sdk.Position) error {
	return i.snapshot.Ack(ctx, position)
}