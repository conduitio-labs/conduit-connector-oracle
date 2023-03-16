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
	"math/rand"
	"strings"

	"github.com/conduitio-labs/conduit-connector-oracle/columntypes"
	"github.com/conduitio-labs/conduit-connector-oracle/repository"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"go.uber.org/multierr"
)

const (
	metadataTable = "oracle.table"

	columnTrackingID    = "CONDUIT_TRACKING_ID"
	columnOperationType = "CONDUIT_OPERATION_TYPE"
	columnTimeCreatedAt = "CONDUIT_TRACKING_CREATED_AT"

	queryIfTableExists     = "SELECT * FROM user_tables WHERE table_name='%s'"
	querySelectRowsFmt     = "SELECT %s FROM %s%s ORDER BY %s ASC FETCH NEXT %d ROWS ONLY"
	queryTableCopy         = "CREATE TABLE %s AS SELECT * FROM %s WHERE 1=2 UNION ALL SELECT * FROM %s WHERE 1=2"
	queryTriggerInsertPart = "INSERT INTO %s (%s, %s) VALUES (%s, transaction_type);"
	querySelectPrimaryKeys = `
	SELECT 
		cols.column_name 
	FROM 
		all_constraints cons, 
		all_cons_columns cols 
	WHERE 
		cols.table_name = :1 
		AND cons.constraint_type = 'P' 
		AND cons.constraint_name = cols.constraint_name 
		AND cons.owner = cols.owner 
	ORDER BY 
		cols.table_name, 
		cols.POSITION`
	queryTrackingTableExtendWithConduitColumns = `
	ALTER TABLE %s 
	ADD (
		%s NUMBER GENERATED BY DEFAULT ON NULL AS IDENTITY, 
		%s VARCHAR2(6) NOT NULL CHECK(%s IN ('insert', 'update', 'delete')), 
		%s DATE DEFAULT SYSDATE,
		CONSTRAINT %s UNIQUE (%s)
	)`
	queryTriggerCreate = `
	CREATE OR REPLACE TRIGGER %s 
		AFTER INSERT
		OR UPDATE 
		OR DELETE ON %s FOR EACH ROW DECLARE transaction_type VARCHAR2(6);
		BEGIN transaction_type := CASE 
			WHEN INSERTING THEN 'insert'
			WHEN UPDATING THEN 'update'
			WHEN DELETING THEN 'delete'
		END;
		IF INSERTING OR UPDATING THEN %s ELSE %s END IF;
	END;`

	referencingNew = ":NEW."
	referencingOld = ":OLD."
)

// Iterator represents an implementation of an iterator for Oracle.
type Iterator struct {
	repo     *repository.Oracle
	snapshot *Snapshot
	cdc      *CDC

	// table represents a table name.
	table string
	// trackingTable represents a tracking table name.
	trackingTable string
	// trigger represents a trigger name for a trackingTable.
	trigger string
	// keyColumns represents a name of the columns that iterator will use for setting key in record.
	keyColumns []string
	// orderingColumn represents a name of column what iterator use for sorting data.
	orderingColumn string
	// columns represents a list of table's columns for record payload.
	// if empty - will get all columns.
	columns []string
	// batchSize represents a size of batch.
	batchSize int
	// columnTypes represents a columns' description from the tracking table.
	columnTypes map[string]columntypes.ColumnDescription
}

// Params represents an incoming iterator params for the New function.
type Params struct {
	Position       *Position
	URL            string
	Table          string
	OrderingColumn string
	KeyColumns     []string
	Snapshot       bool
	Columns        []string
	BatchSize      int
}

// HelperTables returns the helper tables and triggers we use:
// the snapshot table, the tracking table and the trigger name.
func (p Params) HelperTables() (string, string, string) {
	// hash the table name to use it as a postfix in the tracking table and snapshot,
	// because the maximum length of names (tables, triggers, etc.) is 30 characters
	if p.Position != nil {
		return p.Position.SnapshotTable, p.Position.TrackingTable, p.Position.Trigger
	}
	id := rand.Int63()
	if id < 0 {
		id = -id
	}

	snapshot := fmt.Sprintf("CONDUIT_SNAPSHOT_%d", id)
	tracking := fmt.Sprintf("CONDUIT_TRACKING_%d", id)
	trigger := fmt.Sprintf("CONDUIT_%d", id)
	return snapshot, tracking, trigger
}

// New creates a new instance of the iterator.
func New(ctx context.Context, params Params) (*Iterator, error) {
	var err error

	snapshotTable, trackingTable, trigger := params.HelperTables()
	sdk.Logger(ctx).Debug().
		Str("snapshot_table", snapshotTable).
		Str("tracking_table", trackingTable).
		Str("trigger_name", trigger).
		Msg("creating new iterator")

	iterator := &Iterator{
		table:          params.Table,
		trackingTable:  trackingTable,
		trigger:        trigger,
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

	// get column types of the table for converting
	iterator.columnTypes, err = columntypes.GetColumnTypes(ctx, iterator.repo, iterator.table)
	if err != nil {
		return nil, fmt.Errorf("get table column types: %w", err)
	}

	err = iterator.setupCDC(ctx)
	if err != nil {
		return nil, fmt.Errorf("setup cdc: %w", err)
	}

	if params.Snapshot && (params.Position == nil || params.Position.Mode == ModeSnapshot) {
		iterator.snapshot, err = NewSnapshot(ctx, SnapshotParams{
			Repo:           iterator.repo,
			Position:       params.Position,
			Table:          params.Table,
			SnapshotTable:  snapshotTable,
			TrackingTable:  trackingTable,
			Trigger:        trigger,
			OrderingColumn: params.OrderingColumn,
			KeyColumns:     iterator.keyColumns,
			Columns:        params.Columns,
			BatchSize:      params.BatchSize,
			ColumnTypes:    iterator.columnTypes,
		})
		if err != nil {
			return nil, fmt.Errorf("init snapshot iterator: %w", err)
		}
	} else {
		iterator.cdc, err = NewCDC(ctx, CDCParams{
			Repo:           iterator.repo,
			Position:       params.Position,
			Table:          params.Table,
			TrackingTable:  trackingTable,
			Trigger:        trigger,
			OrderingColumn: params.OrderingColumn,
			KeyColumns:     iterator.keyColumns,
			Columns:        params.Columns,
			BatchSize:      params.BatchSize,
		})
		if err != nil {
			return nil, fmt.Errorf("init cdc iterator: %w", err)
		}
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
		TrackingTable:  iter.trackingTable,
		Trigger:        iter.trigger,
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

	rows, err := iter.repo.DB.QueryxContext(ctx, querySelectPrimaryKeys, iter.table)
	if err != nil {
		return fmt.Errorf("select primary keys: %w", err)
	}
	defer rows.Close()

	keyColumn := ""
	for rows.Next() {
		if err = rows.Scan(&keyColumn); err != nil {
			return fmt.Errorf("scan key column value: %w", err)
		}

		iter.keyColumns = append(iter.keyColumns, keyColumn)
	}

	if len(iter.keyColumns) != 0 {
		return nil
	}

	iter.keyColumns = []string{iter.orderingColumn}

	return nil
}

// setupCDC creates a new tracking table, if it does not exist,
// and creates a trigger for it.
func (iter *Iterator) setupCDC(ctx context.Context) error {
	tx, err := iter.repo.DB.Begin()
	if err != nil {
		return fmt.Errorf("begin db transaction: %w", err)
	}
	defer tx.Rollback() // nolint:errcheck,nolintlint

	// create tracking table
	err = iter.initTrackingTable(ctx, tx)
	if err != nil {
		return fmt.Errorf("create tracking table: %w", err)
	}

	// create trigger
	err = iter.createTrigger(ctx, tx)
	if err != nil {
		return fmt.Errorf("create trigger: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("commit db transaction: %w", err)
	}

	return nil
}

// initTrackingTable creates a new tracking table, if it does not exist.
func (iter *Iterator) initTrackingTable(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(queryIfTableExists, iter.trackingTable))
	if err != nil {
		return fmt.Errorf("check if the table %q already exists: %w", iter.trackingTable, err)
	}
	defer rows.Close()

	// return if the tracking table already exists
	if rows.Next() {
		return nil
	}

	// create a copy of table
	_, err = tx.ExecContext(ctx, fmt.Sprintf(queryTableCopy, iter.trackingTable, iter.table, iter.table))
	if err != nil {
		return fmt.Errorf("create copy of table: %w", err)
	}

	// add tracking columns to a tracking table
	_, err = tx.ExecContext(ctx, fmt.Sprintf(queryTrackingTableExtendWithConduitColumns, iter.trackingTable,
		columnTrackingID, columnOperationType, columnOperationType, columnTimeCreatedAt, iter.trackingTable,
		columnTrackingID))
	if err != nil {
		return fmt.Errorf("expand tracking table with conduit columns: %w", err)
	}

	return nil
}

// buildCreateTriggerQuery creates a trigger for the tracking table.
func (iter *Iterator) createTrigger(ctx context.Context, tx *sql.Tx) error {
	var columnNames []string

	if iter.columns != nil {
		columnNames = append(columnNames, iter.columns...)
	} else {
		for key := range iter.columnTypes {
			columnNames = append(columnNames, key)
		}
	}

	newValues := make([]string, len(columnNames))
	oldValues := make([]string, len(columnNames))
	for i := range columnNames {
		newValues[i] = fmt.Sprintf("%s%s", referencingNew, columnNames[i])
		oldValues[i] = fmt.Sprintf("%s%s", referencingOld, columnNames[i])
	}

	insertOnInsertingOrUpdating := fmt.Sprintf(queryTriggerInsertPart, iter.trackingTable,
		strings.Join(columnNames, ","), columnOperationType, strings.Join(newValues, ","))
	insertOnDeleting := fmt.Sprintf(queryTriggerInsertPart, iter.trackingTable,
		strings.Join(columnNames, ","), columnOperationType, strings.Join(oldValues, ","))

	_, err := tx.ExecContext(ctx,
		fmt.Sprintf(queryTriggerCreate, iter.trigger, iter.table, insertOnInsertingOrUpdating, insertOnDeleting))
	if err != nil {
		return fmt.Errorf("create trigger: %w", err)
	}

	return nil
}
