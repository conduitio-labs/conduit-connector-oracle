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
	"sync"
	"time"

	"github.com/conduitio-labs/conduit-connector-oracle/coltypes"
	"github.com/conduitio-labs/conduit-connector-oracle/repository"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"
	"go.uber.org/multierr"
)

// CDC represents an implementation of a CDC iterator for Oracle.
type CDC struct {
	repo     *repository.Oracle
	position *Position

	// tableSrv service for clearing tracking table.
	tableSrv *trackingTableService

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
	// columnTypes represents a columns' description from table
	columnTypes map[string]coltypes.ColumnDescription
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

type trackingTableService struct {
	m sync.Mutex

	// channel for the stop signal
	stopCh chan struct{}
	// channel to notify that all queries are completed and the database connection can be closed
	canCloseCh chan struct{}
	// error channel
	errCh chan error
	// slice of identifiers to delete
	idsToDelete []any
}

// NewCDC creates a new instance of the CDC iterator.
func NewCDC(ctx context.Context, params CDCParams) (*CDC, error) {
	var err error

	iterator := &CDC{
		repo:           params.Repo,
		position:       params.Position,
		tableSrv:       newTrackingTableService(),
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

	// run clearing tracking table.
	go iterator.clearTrackingTable(ctx)

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

	position := &Position{
		Mode: ModeCDC,
		// set the value from columnTrackingID column of the tracking table
		LastProcessedVal: transformedRow[columnTrackingID],
	}

	convertedPosition, err := position.marshal()
	if err != nil {
		return sdk.Record{}, fmt.Errorf("convert position %w", err)
	}

	if _, ok = transformedRow[i.keyColumn]; !ok {
		return sdk.Record{}, errNoKey
	}

	// delete tracking columns
	delete(transformedRow, columnOperationType)
	delete(transformedRow, columnTrackingID)
	delete(transformedRow, columnTimeCreatedAt)

	transformedRowBytes, err := json.Marshal(transformedRow)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("marshal row: %w", err)
	}

	i.position = position

	metadata := sdk.Metadata{
		metadataTable: i.table,
	}
	metadata.SetCreatedAt(time.Now())

	switch operationType {
	case actionInsert:
		return sdk.Util.Source.NewRecordCreate(
			convertedPosition,
			metadata,
			sdk.StructuredData{
				i.keyColumn: transformedRow[i.keyColumn],
			},
			sdk.RawData(transformedRowBytes),
		), nil
	case actionUpdate:
		return sdk.Util.Source.NewRecordUpdate(
			convertedPosition,
			metadata,
			sdk.StructuredData{
				i.keyColumn: transformedRow[i.keyColumn],
			},
			nil,
			sdk.RawData(transformedRowBytes),
		), nil
	case actionDelete:
		return sdk.Util.Source.NewRecordDelete(
			convertedPosition,
			metadata,
			sdk.StructuredData{
				i.keyColumn: transformedRow[i.keyColumn],
			},
		), nil
	default:
		return sdk.Record{}, errWrongTrackingOperationType
	}
}

// Close closes database rows of CDC iterator.
func (i *CDC) Close() (err error) {
	// send a signal to stop clearing the tracking table
	i.tableSrv.stopCh <- struct{}{}

	if i.rows != nil {
		err = i.rows.Close()
	}

	select {
	// wait until clearing tracking table will be finished
	case <-i.tableSrv.canCloseCh:
	// or until time out
	case <-time.After(timeoutBeforeCloseDBSec * time.Second):
	}

	i.tableSrv.close()

	return
}

// loadRows selects a batch of rows from a database, based on the
// table, columns, orderingColumn, batchSize and the current position.
func (i *CDC) loadRows(ctx context.Context) error {
	columns := "*"
	if len(i.columns) > 0 {
		columns = strings.Join(append(i.columns,
			[]string{columnTrackingID, columnOperationType, columnTimeCreatedAt}...), ",")
	}

	whereClause := ""
	args := make([]any, 0)
	if i.position != nil {
		whereClause = fmt.Sprintf(" WHERE %s > :1", columnTrackingID)
		args = append(args, i.position.LastProcessedVal)
	}

	query := fmt.Sprintf(querySelectRowsFmt, columns, i.trackingTable, whereClause, columnTrackingID, i.batchSize)

	rows, err := i.repo.DB.QueryxContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("execute select query %q, %v: %w", query, args, err)
	}

	i.rows = rows

	return nil
}

// newTrackingTableService is a service to delete processed rows.
func newTrackingTableService() *trackingTableService {
	stopCh := make(chan struct{}, 1)
	canCloseCh := make(chan struct{}, 1)
	errCh := make(chan error, 1)
	trackingIDsForRemoving := make([]any, 0)

	return &trackingTableService{
		stopCh:      stopCh,
		canCloseCh:  canCloseCh,
		errCh:       errCh,
		idsToDelete: trackingIDsForRemoving,
	}
}

func (t *trackingTableService) close() {
	close(t.canCloseCh)
	close(t.errCh)
	close(t.stopCh)
}

// pushValueToDelete appends the last processed value to the slice to clear the tracking table in the future.
func (i *CDC) pushValueToDelete(lastProcessedVal any) (err error) {
	if len(i.tableSrv.errCh) > 0 {
		for e := range i.tableSrv.errCh {
			err = multierr.Append(err, e)
		}

		if err != nil {
			return fmt.Errorf("clear tracking table: %w", err)
		}
	}

	i.tableSrv.m.Lock()
	defer i.tableSrv.m.Unlock()

	if i.tableSrv.idsToDelete == nil {
		i.tableSrv.idsToDelete = make([]any, 0)
	}

	i.tableSrv.idsToDelete = append(i.tableSrv.idsToDelete, lastProcessedVal)

	return nil
}

func (i *CDC) clearTrackingTable(ctx context.Context) {
	for {
		select {
		// connector is stopping, clear table last time
		case <-i.tableSrv.stopCh:
			err := i.deleteTrackingTableRows(ctx)
			if err != nil {
				i.tableSrv.errCh <- err
			}

			// query finished, db can be closed.
			i.tableSrv.canCloseCh <- struct{}{}

			return

		case <-time.After(timeoutToClearTrackingTableSec * time.Second):
			err := i.deleteTrackingTableRows(ctx)
			if err != nil {
				i.tableSrv.errCh <- err

				return
			}
		}
	}
}

// deleteTrackingTableRows deletes processed rows from tracking table.
func (i *CDC) deleteTrackingTableRows(ctx context.Context) error {
	i.tableSrv.m.Lock()
	defer i.tableSrv.m.Unlock()

	if len(i.tableSrv.idsToDelete) == 0 {
		return nil
	}

	query := buildDeleteByIDsQuery(i.trackingTable, columnTrackingID, i.tableSrv.idsToDelete)

	_, err := i.repo.DB.ExecContext(ctx, query, i.tableSrv.idsToDelete...)
	if err != nil {
		return fmt.Errorf("execute delete query %q: %w", query, err)
	}

	i.tableSrv.idsToDelete = nil

	return nil
}

// buildDeleteByIDsQuery returns delete by id query.
func buildDeleteByIDsQuery(table, column string, ids []any) string {
	placeholders := make([]string, len(ids))
	for i := range ids {
		placeholders[i] = fmt.Sprintf(":%d", i+1)
	}

	return fmt.Sprintf(queryDeleteByIDs, table, column, strings.Join(placeholders, ","))
}
