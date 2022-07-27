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

package writer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/conduitio-labs/conduit-connector-oracle/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/huandu/go-sqlbuilder"
)

const (
	// metadata related.
	metadataTable  = "table"
	metadataAction = "action"

	// action names.
	actionUpdate = "update"
	actionDelete = "delete"
)

// Writer implements a writer logic for Oracle destination.
type Writer struct {
	db  *sql.DB
	cfg config.Destination
}

// New creates new instance of the Writer.
func New(db *sql.DB, cfg config.Destination) *Writer {
	return &Writer{
		db:  db,
		cfg: cfg,
	}
}

// Write writes a sdk.Record into a Destination.
func (w *Writer) Write(ctx context.Context, record sdk.Record) error {
	switch record.Metadata[metadataAction] {
	case actionUpdate:
		return w.update(ctx, record)
	case actionDelete:
		return w.delete(ctx, record)
	default:
		return w.insert(ctx, record)
	}
}

// Close closes the underlying db connection.
func (w *Writer) Close(ctx context.Context) error {
	return w.db.Close()
}

// inserts a record.
func (w *Writer) insert(ctx context.Context, record sdk.Record) error {
	tableName := w.getTableName(record.Metadata)

	payload, err := w.structurizeData(record.Payload)
	if err != nil {
		return fmt.Errorf("structurize payload: %w", err)
	}

	// if payload is empty return empty payload error
	if payload == nil {
		return errEmptyPayload
	}

	columns, values := w.extractColumnsAndValues(payload)

	w.convertValues(values)

	query, err := w.buildInsertQuery(tableName, columns, values)
	if err != nil {
		return fmt.Errorf("build insert query: %w", err)
	}

	_, err = w.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("exec insert: %w", err)
	}

	return nil
}

// updates a record.
func (w *Writer) update(ctx context.Context, record sdk.Record) error {
	tableName := w.getTableName(record.Metadata)

	payload, err := w.structurizeData(record.Payload)
	if err != nil {
		return fmt.Errorf("structurize payload: %w", err)
	}

	// if payload is empty return empty payload error
	if payload == nil {
		return errEmptyPayload
	}

	key, err := w.structurizeData(record.Key)
	if err != nil {
		return fmt.Errorf("structurize key during upsert: %v", err)
	}

	keyColumn, err := w.getKeyColumn(key)
	if err != nil {
		return fmt.Errorf("get key column: %w", err)
	}

	// return an error if we didn't find a value for the key
	keyValue, ok := key[keyColumn]
	if !ok {
		return errEmptyKey
	}

	columns, values := w.extractColumnsAndValues(payload)

	w.convertValues(values)

	query, err := w.buildUpdateQuery(tableName, keyColumn, keyValue, columns, values)
	if err != nil {
		return fmt.Errorf("build upsert query: %w", err)
	}

	_, err = w.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("exec upsert: %w", err)
	}

	return nil
}

// deletes records by a key. First it looks in the sdk.Record.Key,
// if it doesn't find a key there it will use the default configured value for a key.
func (w *Writer) delete(ctx context.Context, record sdk.Record) error {
	tableName := w.getTableName(record.Metadata)

	key, err := w.structurizeData(record.Key)
	if err != nil {
		return fmt.Errorf("structurize key: %w", err)
	}

	keyColumn, err := w.getKeyColumn(key)
	if err != nil {
		return fmt.Errorf("get key column: %w", err)
	}

	// return an error if we didn't find a value for the key
	keyValue, ok := key[keyColumn]
	if !ok {
		return errEmptyKey
	}

	query, err := w.buildDeleteQuery(tableName, keyColumn, keyValue)
	if err != nil {
		return fmt.Errorf("build delete query: %w", err)
	}

	_, err = w.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("exec delete: %w", err)
	}

	return nil
}

// generates an SQL INSERT statement query.
func (w *Writer) buildInsertQuery(table string, columns []string, values []any) (string, error) {
	if len(columns) != len(values) {
		return "", errColumnsValuesLenMismatch
	}

	ib := sqlbuilder.NewInsertBuilder()

	ib.InsertInto(table)
	ib.Cols(columns...)
	ib.Values(values...)

	sql, args := ib.Build()

	query, err := sqlbuilder.DefaultFlavor.Interpolate(sql, args)
	if err != nil {
		return "", fmt.Errorf("interpolate arguments to SQL: %w", err)
	}

	return query, nil
}

// generates an SQL UPDATE statement query.
func (w *Writer) buildUpdateQuery(
	table string, keyColumn string, keyValue any, columns []string, values []any,
) (string, error) {
	if len(columns) != len(values) {
		return "", errColumnsValuesLenMismatch
	}

	ub := sqlbuilder.NewUpdateBuilder()

	ub.Update(table)
	ub.Where(
		ub.Equal(keyColumn, keyValue),
	)

	assignments := make([]string, len(columns))
	for i := 0; i < len(columns); i++ {
		assignments[i] = ub.Assign(columns[i], values[i])
	}
	ub.Set(assignments...)

	query, err := sqlbuilder.DefaultFlavor.Interpolate(ub.Build())
	if err != nil {
		return "", fmt.Errorf("interpolate arguments to SQL: %w", err)
	}

	return query, nil
}

// generates an SQL DELETE statement query.
func (w *Writer) buildDeleteQuery(table string, keyColumn string, keyValue any) (string, error) {
	db := sqlbuilder.NewDeleteBuilder()

	db.DeleteFrom(table)
	db.Where(
		db.Equal(keyColumn, keyValue),
	)

	query, err := sqlbuilder.DefaultFlavor.Interpolate(db.Build())
	if err != nil {
		return "", fmt.Errorf("interpolate arguments to SQL: %w", err)
	}

	return query, nil
}

// returns either the record metadata value for the table
// or the default configured value for the table.
func (w *Writer) getTableName(metadata map[string]string) string {
	tableName, ok := metadata[metadataTable]
	if !ok {
		return w.cfg.Table
	}

	return strings.ToLower(tableName)
}

// returns either the first key within the Key structured data
// or the default key configured value for key.
func (w *Writer) getKeyColumn(key sdk.StructuredData) (string, error) {
	if len(key) > 1 {
		return "", errCompositeKeysNotSupported
	}

	for k := range key {
		return k, nil
	}

	return w.cfg.KeyColumn, nil
}

// converts sdk.Data to sdk.StructuredData.
func (w *Writer) structurizeData(data sdk.Data) (sdk.StructuredData, error) {
	if data == nil || len(data.Bytes()) == 0 {
		return nil, nil
	}

	structuredData := make(sdk.StructuredData)
	if err := json.Unmarshal(data.Bytes(), &structuredData); err != nil {
		return nil, fmt.Errorf("unmarshal data into structured data: %w", err)
	}

	return structuredData, nil
}

// turns the payload into slices of columns and values for use in queries to Oracle.
func (w *Writer) extractColumnsAndValues(payload sdk.StructuredData) ([]string, []any) {
	var (
		columns = make([]string, 0, len(payload))
		values  = make([]any, 0, len(payload))
	)

	for key, value := range payload {
		columns = append(columns, key)
		values = append(values, value)
	}

	return columns, values
}

// converts values to right Oracle's types.
func (w *Writer) convertValues(values []any) {
	for i := range values {
		// convert the boolean type to the Oracle type NUMBER(1,0)
		if values[i] != nil && reflect.TypeOf(values[i]).Kind() == reflect.Bool {
			if values[i].(bool) {
				values[i] = 1
			} else {
				values[i] = 0
			}
		}
	}
}
