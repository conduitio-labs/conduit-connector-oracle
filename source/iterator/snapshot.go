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
	"strings"
	"time"

	"github.com/conduitio-labs/conduit-connector-oracle/coltypes"
	"github.com/conduitio-labs/conduit-connector-oracle/repository"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"
)

// Snapshot represents an implementation of a Snapshot iterator for Oracle.
type Snapshot struct {
	repo     *repository.Oracle
	position *Position

	// table represents a table name
	table string
	// snapshotTable represents a name of snapshot table
	snapshotTable string
	// keyColumn represents a name of column what iterator use for setting key in record
	keyColumn string
	// orderingColumn represents a name of column what iterator use for sorting data
	orderingColumn string
	// columns represents a list of table's columns for record payload.
	// if empty - will get all columns
	columns []string
	// batchSize represents a size of batch
	batchSize int

	rows *sqlx.Rows
	// columnTypes represents a columns' description from table
	columnTypes map[string]coltypes.ColumnDescription
}

// SnapshotParams represents an incoming params for the NewSnapshot function.
type SnapshotParams struct {
	Repo           *repository.Oracle
	Position       *Position
	Table          string
	SnapshotTable  string
	KeyColumn      string
	OrderingColumn string
	Columns        []string
	BatchSize      int
	ColumnTypes    map[string]coltypes.ColumnDescription
}

// NewSnapshot creates a new instance of the Snapshot iterator.
func NewSnapshot(ctx context.Context, params SnapshotParams) (*Snapshot, error) {
	iterator := &Snapshot{
		repo:           params.Repo,
		position:       params.Position,
		table:          params.Table,
		snapshotTable:  params.SnapshotTable,
		keyColumn:      params.KeyColumn,
		orderingColumn: params.OrderingColumn,
		columns:        params.Columns,
		batchSize:      params.BatchSize,
		columnTypes:    params.ColumnTypes,
	}

	err := iterator.loadRows(ctx)
	if err != nil {
		return nil, fmt.Errorf("load rows: %w", err)
	}

	return iterator, nil
}

// HasNext returns a bool indicating whether the iterator has the next record to return or not.
func (i *Snapshot) HasNext(ctx context.Context) (bool, error) {
	if i.rows != nil && i.rows.Next() {
		return true, nil
	}

	if err := i.loadRows(ctx); err != nil {
		return false, fmt.Errorf("load rows: %w", err)
	}

	return i.rows.Next(), nil
}

// Next returns the next record.
func (i *Snapshot) Next(_ context.Context) (sdk.Record, error) {
	row := make(map[string]any)
	if err := i.rows.MapScan(row); err != nil {
		return sdk.Record{}, fmt.Errorf("scan rows: %w", err)
	}

	transformedRow, err := coltypes.TransformRow(row, i.columnTypes)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("transform row column types: %w", err)
	}

	if _, ok := transformedRow[i.orderingColumn]; !ok {
		return sdk.Record{}, errOrderingColumnIsNotExist
	}

	i.position = &Position{
		Mode: ModeSnapshot,
		// set the value from i.orderingColumn column you chose
		LastProcessedVal: transformedRow[i.orderingColumn],
	}

	convertedPosition, err := i.position.marshal()
	if err != nil {
		return sdk.Record{}, fmt.Errorf("convert position %w", err)
	}

	if _, ok := transformedRow[i.keyColumn]; !ok {
		return sdk.Record{}, errNoKey
	}

	transformedRowBytes, err := json.Marshal(transformedRow)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("marshal row: %w", err)
	}

	metadata := sdk.Metadata{
		metadataTable: i.table,
	}
	metadata.SetCreatedAt(time.Now())

	r := sdk.Util.Source.NewRecordSnapshot(
		convertedPosition,
		metadata,
		sdk.StructuredData{
			i.keyColumn: transformedRow[i.keyColumn],
		},
		sdk.RawData(transformedRowBytes),
	)

	return r, nil
}

// Close closes database rows of Snapshot iterator.
func (i *Snapshot) Close() error {
	if i.rows != nil {
		return i.rows.Close()
	}

	return nil
}

// LoadRows selects a batch of rows from a database, based on the
// table, columns, orderingColumn, batchSize and the current position.
func (i *Snapshot) loadRows(ctx context.Context) error {
	columns := "*"
	if len(i.columns) > 0 {
		columns = strings.Join(i.columns, ",")
	}

	whereClause := ""
	args := make([]any, 0)
	if i.position != nil {
		whereClause = fmt.Sprintf(" WHERE %s > :1", i.orderingColumn)
		args = append(args, i.position.LastProcessedVal)
	}

	query := fmt.Sprintf(querySelectRowsFmt, columns, i.snapshotTable, whereClause, i.orderingColumn, i.batchSize)

	rows, err := i.repo.DB.QueryxContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("execute select query %q, %v: %w", query, args, err)
	}

	i.rows = rows

	return nil
}
