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
	"encoding/json"
	"fmt"
	"time"

	"github.com/conduitio-labs/conduit-connector-oracle/coltypes"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/huandu/go-sqlbuilder"
	"github.com/jmoiron/sqlx"
)

// Snapshot represents an implementation of a Snapshot iterator for Oracle.
type Snapshot struct {
	db             *sqlx.DB
	position       *Position
	table          string
	keyColumn      string
	orderingColumn string
	columns        []string
	batchSize      int

	rows        *sqlx.Rows
	columnTypes map[string]coltypes.ColumnData
}

// SnapshotParams represents an incoming params for the NewSnapshot function.
type SnapshotParams struct {
	DB             *sqlx.DB
	Position       *Position
	Table          string
	KeyColumn      string
	OrderingColumn string
	Columns        []string
	BatchSize      int
	ColumnTypes    map[string]coltypes.ColumnData
}

// NewSnapshot creates a new instance of the Snapshot iterator.
func NewSnapshot(ctx context.Context, params SnapshotParams) (*Snapshot, error) {
	snapshot := &Snapshot{
		db:             params.DB,
		position:       params.Position,
		table:          params.Table,
		keyColumn:      params.KeyColumn,
		orderingColumn: params.OrderingColumn,
		columns:        params.Columns,
		batchSize:      params.BatchSize,
		columnTypes:    params.ColumnTypes,
	}

	err := snapshot.loadRows(ctx)
	if err != nil {
		return nil, fmt.Errorf("load rows: %w", err)
	}

	return snapshot, nil
}

// HasNext returns a bool indicating whether the iterator has the next record to return or not.
func (s *Snapshot) HasNext(ctx context.Context) (bool, error) {
	if s.rows != nil && s.rows.Next() {
		return true, nil
	}

	if err := s.loadRows(ctx); err != nil {
		return false, fmt.Errorf("load rows: %w", err)
	}

	return false, nil
}

// Next returns the next record.
func (s *Snapshot) Next(ctx context.Context) (sdk.Record, error) {
	row := make(map[string]any)
	if err := s.rows.MapScan(row); err != nil {
		return sdk.Record{}, fmt.Errorf("scan rows: %w", err)
	}

	transformedRow, err := coltypes.TransformRow(row, s.columnTypes)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("transform row column types: %w", err)
	}

	if _, ok := transformedRow[s.orderingColumn]; !ok {
		return sdk.Record{}, errOrderingColumnIsNotExist
	}

	pos := Position{
		Mode:             ModeSnapshot,
		LastProcessedVal: transformedRow[s.orderingColumn],
		//Time:             time.Now(),
	}

	convertedPosition, err := pos.convertToSDKPosition()
	if err != nil {
		return sdk.Record{}, fmt.Errorf("convert position %w", err)
	}

	if _, ok := transformedRow[s.keyColumn]; !ok {
		return sdk.Record{}, errKeyIsNotExist
	}

	transformedRowBytes, err := json.Marshal(transformedRow)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("marshal row: %w", err)
	}

	s.position = &pos

	return sdk.Record{
		Position: convertedPosition,
		Metadata: map[string]string{
			metadataTable:  s.table,
			metadataAction: string(actionInsert),
		},
		CreatedAt: time.Now(),
		Key: sdk.StructuredData{
			s.keyColumn: transformedRow[s.keyColumn],
		},
		Payload: sdk.RawData(transformedRowBytes),
	}, nil
}

// Ack check if record with position was recorded.
func (s *Snapshot) Ack(ctx context.Context, position sdk.Position) error {
	sdk.Logger(ctx).Debug().Str("position", string(position)).Msg("got ack")

	return nil
}

// Stop stops snapshot iterator.
func (s *Snapshot) Stop() error {
	if s.rows != nil {
		err := s.rows.Close()
		if err != nil {
			return err
		}
	}

	if s.db != nil {
		return s.db.Close()
	}

	return nil
}

// LoadRows selects a batch of rows from a database, based on the Iterator's
// table, columns, orderingColumn, batchSize and the current position.
func (s *Snapshot) loadRows(ctx context.Context) error {
	selectBuilder := sqlbuilder.NewSelectBuilder().
		From(s.table).
		OrderBy(s.orderingColumn)

	if len(s.columns) > 0 {
		selectBuilder.Select(s.columns...)
	} else {
		selectBuilder.Select("*")
	}

	if s.position != nil {
		selectBuilder.Where(
			selectBuilder.GreaterThan(s.orderingColumn, s.position.LastProcessedVal),
		)
	}

	query, err := sqlbuilder.DefaultFlavor.Interpolate(
		sqlbuilder.Buildf("%v FETCH NEXT %v ROWS ONLY", selectBuilder, s.batchSize).Build(),
	)
	if err != nil {
		return fmt.Errorf("interpolate arguments to SQL: %w", err)
	}

	rows, err := s.db.QueryxContext(ctx, query)
	if err != nil {
		return fmt.Errorf("execute select query: %s: %w", query, err)
	}

	s.rows = rows

	return nil
}
