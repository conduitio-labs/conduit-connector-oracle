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
	"github.com/conduitio-labs/conduit-connector-oracle/repository"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/huandu/go-sqlbuilder"
	"github.com/jmoiron/sqlx"
)

// CDC represents an implementation of a CDC iterator for Oracle.
type CDC struct {
	repo     *repository.Oracle
	position *Position

	// table represents a table name
	table string
	// trackingTable represents a tracking table name
	trackingTable string
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
	// columnTypes represents a columns' data from table
	columnTypes map[string]coltypes.ColumnData
}

// CDCParams represents an incoming params for the NewCDC function.
type CDCParams struct {
	Repo           *repository.Oracle
	Position       *Position
	Table          string
	TrackingTable  string
	KeyColumn      string
	OrderingColumn string
	Columns        []string
	BatchSize      int
}

// NewCDC creates a new instance of the CDC iterator.
func NewCDC(ctx context.Context, params CDCParams) (*CDC, error) {
	var err error

	iterator := &CDC{
		repo:           params.Repo,
		position:       params.Position,
		table:          params.Table,
		trackingTable:  params.TrackingTable,
		keyColumn:      params.KeyColumn,
		orderingColumn: params.OrderingColumn,
		columns:        params.Columns,
		batchSize:      params.BatchSize,
	}

	// get column types of tracking table for converting
	iterator.columnTypes, err = coltypes.GetColumnTypes(ctx, iterator.repo, iterator.trackingTable)
	if err != nil {
		return nil, fmt.Errorf("get tracking table column types: %w", err)
	}

	err = iterator.loadRows(ctx)
	if err != nil {
		return nil, fmt.Errorf("load rows: %w", err)
	}

	return iterator, nil
}

// HasNext returns a bool indicating whether the iterator has the next record to return or not.
func (i *CDC) HasNext(ctx context.Context) (bool, error) {
	if i.rows != nil && i.rows.Next() {
		return true, nil
	}

	if err := i.loadRows(ctx); err != nil {
		return false, fmt.Errorf("load rows: %w", err)
	}

	return i.rows.Next(), nil
}

// Next returns the next record.
func (i *CDC) Next(ctx context.Context) (sdk.Record, error) {
	row := make(map[string]any)
	if err := i.rows.MapScan(row); err != nil {
		return sdk.Record{}, fmt.Errorf("scan rows: %w", err)
	}

	transformedRow, err := coltypes.TransformRow(row, i.columnTypes)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("transform row column types: %w", err)
	}

	operationType, ok := transformedRow[columnOperationType].(string)
	if !ok {
		return sdk.Record{}, errWrongTrackingOperationType
	}

	i.position = &Position{
		Mode:             ModeCDC,
		LastProcessedVal: transformedRow[columnTrackingID],
	}

	convertedPosition, err := i.position.marshalPosition()
	if err != nil {
		return sdk.Record{}, fmt.Errorf("convert position %w", err)
	}

	if _, ok = transformedRow[i.keyColumn]; !ok {
		return sdk.Record{}, errKeyIsNotExist
	}

	// delete tracking columns
	delete(transformedRow, columnOperationType)
	delete(transformedRow, columnTrackingID)
	delete(transformedRow, columnTimeCreatedAt)

	transformedRowBytes, err := json.Marshal(transformedRow)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("marshal row: %w", err)
	}

	return sdk.Record{
		Position: convertedPosition,
		Metadata: map[string]string{
			metadataTable:  i.table,
			metadataAction: operationType,
		},
		CreatedAt: time.Now(),
		Key: sdk.StructuredData{
			i.keyColumn: transformedRow[i.keyColumn],
		},
		Payload: sdk.RawData(transformedRowBytes),
	}, nil
}

// Stop stops iterators.
func (i *CDC) Stop() error {
	if i.rows != nil {
		err := i.rows.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

// Ack check if record with position was recorded.
func (i *CDC) Ack(ctx context.Context, position sdk.Position) error {
	return nil
}

// loadRows selects a batch of rows from a database, based on the
// table, columns, orderingColumn, batchSize and the current position.
func (i *CDC) loadRows(ctx context.Context) error {
	selectBuilder := sqlbuilder.NewSelectBuilder().
		From(i.trackingTable).
		OrderBy(columnTrackingID)

	if len(i.columns) > 0 {
		selectBuilder.Select(append(i.columns,
			[]string{columnTrackingID, columnOperationType, columnTimeCreatedAt}...)...)
	} else {
		selectBuilder.Select("*")
	}

	if i.position != nil {
		selectBuilder.Where(
			selectBuilder.GreaterThan(columnTrackingID, i.position.LastProcessedVal),
		)
	}

	query, err := sqlbuilder.DefaultFlavor.Interpolate(
		sqlbuilder.Buildf("%v FETCH NEXT %v ROWS ONLY", selectBuilder, i.batchSize).Build(),
	)
	if err != nil {
		return fmt.Errorf("interpolate arguments to SQL: %w", err)
	}

	i.rows, err = i.repo.DB.QueryxContext(ctx, query)
	if err != nil {
		return fmt.Errorf("execute select query: %s: %w", query, err)
	}

	return nil
}
