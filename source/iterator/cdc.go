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
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/conduitio-labs/conduit-connector-oracle/columntypes"
	"github.com/conduitio-labs/conduit-connector-oracle/repository"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"
	"go.uber.org/multierr"
)

const (
	actionInsert = "insert"
	actionUpdate = "update"
	actionDelete = "delete"

	timeoutBeforeCloseDBSec        = 20
	timeoutToClearTrackingTableSec = 5

	queryDeleteByIDs = "DELETE FROM %s WHERE %s IN (%s)"
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
	// trigger represents a trigger name for a trackingTable.
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
	rows      *sqlx.Rows
	// columnTypes represents a columns' description from table
	columnTypes map[string]columntypes.ColumnDescription
}

// CDCParams represents an incoming params for the NewCDC function.
type CDCParams struct {
	Repo           *repository.Oracle
	Position       *Position
	Table          string
	TrackingTable  string
	Trigger        string
	OrderingColumn string
	KeyColumns     []string
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
		trigger:        params.Trigger,
		orderingColumn: params.OrderingColumn,
		keyColumns:     params.KeyColumns,
		columns:        params.Columns,
		batchSize:      params.BatchSize,
	}

	// get column types of tracking table for converting
	iterator.columnTypes, err = columntypes.GetColumnTypes(ctx, iterator.repo, iterator.trackingTable)
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
func (iter *CDC) HasNext(ctx context.Context) (bool, error) {
	if iter.rows != nil && iter.rows.Next() {
		return true, nil
	}

	if err := iter.loadRows(ctx); err != nil {
		return false, fmt.Errorf("load rows: %w", err)
	}

	return iter.rows.Next(), nil
}

// Next returns the next record.
func (iter *CDC) Next() (sdk.Record, error) {
	row := make(map[string]any)
	if err := iter.rows.MapScan(row); err != nil {
		return sdk.Record{}, fmt.Errorf("scan rows: %w", err)
	}

	transformedRow, err := columntypes.TransformRow(row, iter.columnTypes)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("transform row column types: %w", err)
	}

	operationType, ok := transformedRow[columnOperationType].(string)
	if !ok {
		return sdk.Record{}, errWrongTrackingOperationType
	}

	// set a new position into the variable,
	// to avoid saving position into the struct until we marshal the position
	position := &Position{
		Mode: ModeCDC,
		// set the value from columnTrackingID column of the tracking table
		LastProcessedVal: transformedRow[columnTrackingID],
		TrackingTable:    iter.trackingTable,
		Trigger:          iter.trigger,
	}

	convertedPosition, err := position.ToSDK()
	if err != nil {
		return sdk.Record{}, fmt.Errorf("convert position %w", err)
	}

	key := make(sdk.StructuredData)
	for i := range iter.keyColumns {
		val, ok := transformedRow[iter.keyColumns[i]]
		if !ok {
			return sdk.Record{}, fmt.Errorf("key column %q not found", iter.keyColumns[i])
		}

		key[iter.keyColumns[i]] = val
	}

	// delete tracking columns
	delete(transformedRow, columnOperationType)
	delete(transformedRow, columnTrackingID)
	delete(transformedRow, columnTimeCreatedAt)

	transformedRowBytes, err := json.Marshal(transformedRow)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("marshal row: %w", err)
	}

	iter.position = position

	metadata := sdk.Metadata{
		metadataTable: iter.table,
	}
	metadata.SetCreatedAt(time.Now())

	switch operationType {
	case actionInsert:
		return sdk.Util.Source.NewRecordCreate(
			convertedPosition,
			metadata,
			key,
			sdk.RawData(transformedRowBytes),
		), nil
	case actionUpdate:
		return sdk.Util.Source.NewRecordUpdate(
			convertedPosition,
			metadata,
			key,
			nil,
			sdk.RawData(transformedRowBytes),
		), nil
	case actionDelete:
		return sdk.Util.Source.NewRecordDelete(
			convertedPosition,
			metadata,
			key,
		), nil
	default:
		return sdk.Record{}, errWrongTrackingOperationType
	}
}

// Close closes database rows of CDC iterator.
func (iter *CDC) Close() (err error) {
	// send a signal to stop clearing the tracking table
	iter.tableSrv.stopCh <- struct{}{}

	if iter.rows != nil {
		err = iter.rows.Close()
	}

	select {
	// wait until clearing tracking table will be finished
	case <-iter.tableSrv.canCloseCh:
	// or until time out
	case <-time.After(timeoutBeforeCloseDBSec * time.Second):
	}

	iter.tableSrv.close()

	return
}

// loadRows selects a batch of rows from a database, based on the
// table, columns, orderingColumn, batchSize and the current position.
func (iter *CDC) loadRows(ctx context.Context) error {
	columns := "*"
	if len(iter.columns) > 0 {
		columns = strings.Join(append(iter.columns,
			[]string{columnTrackingID, columnOperationType, columnTimeCreatedAt}...), ",")
	}

	whereClause := ""
	args := make([]any, 0)
	if iter.position != nil {
		whereClause = fmt.Sprintf(" WHERE %s > :1", columnTrackingID)
		args = append(args, iter.position.LastProcessedVal)
	}

	query := fmt.Sprintf(querySelectRowsFmt, columns, iter.trackingTable, whereClause, columnTrackingID, iter.batchSize)

	rows, err := iter.repo.DB.QueryxContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("execute select query %q, %v: %w", query, args, err)
	}

	iter.rows = rows

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
func (iter *CDC) pushValueToDelete(lastProcessedVal any) (err error) {
	if len(iter.tableSrv.errCh) > 0 {
		for e := range iter.tableSrv.errCh {
			err = multierr.Append(err, e)
		}

		if err != nil {
			return fmt.Errorf("clear tracking table: %w", err)
		}
	}

	iter.tableSrv.m.Lock()
	defer iter.tableSrv.m.Unlock()

	if iter.tableSrv.idsToDelete == nil {
		iter.tableSrv.idsToDelete = make([]any, 0)
	}

	iter.tableSrv.idsToDelete = append(iter.tableSrv.idsToDelete, lastProcessedVal)

	return nil
}

func (iter *CDC) clearTrackingTable(ctx context.Context) {
	for {
		select {
		// connector is stopping, clear table last time
		case <-iter.tableSrv.stopCh:
			err := iter.deleteTrackingTableRows(ctx)
			if err != nil {
				iter.tableSrv.errCh <- err
			}

			// query finished, db can be closed.
			iter.tableSrv.canCloseCh <- struct{}{}

			return

		case <-time.After(timeoutToClearTrackingTableSec * time.Second):
			err := iter.deleteTrackingTableRows(ctx)
			if err != nil {
				iter.tableSrv.errCh <- err

				return
			}
		}
	}
}

// deleteTrackingTableRows deletes processed rows from tracking table.
func (iter *CDC) deleteTrackingTableRows(ctx context.Context) error {
	iter.tableSrv.m.Lock()
	defer iter.tableSrv.m.Unlock()

	if len(iter.tableSrv.idsToDelete) == 0 {
		return nil
	}

	query := iter.buildDeleteByIDsQuery()

	_, err := iter.repo.DB.ExecContext(ctx, query, iter.tableSrv.idsToDelete...)
	if err != nil {
		return fmt.Errorf("execute delete query %q: %w", query, err)
	}

	iter.tableSrv.idsToDelete = nil

	return nil
}

// buildDeleteByIDsQuery returns delete by id query.
func (iter *CDC) buildDeleteByIDsQuery() string {
	placeholders := make([]string, len(iter.tableSrv.idsToDelete))
	for i := range iter.tableSrv.idsToDelete {
		placeholders[i] = fmt.Sprintf(":%d", i+1)
	}

	return fmt.Sprintf(queryDeleteByIDs, iter.trackingTable, columnTrackingID, strings.Join(placeholders, ","))
}
