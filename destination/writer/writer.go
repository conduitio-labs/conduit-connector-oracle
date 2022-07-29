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
	actionDelete = "delete"

	// upsert sql format.
	upsertFmt = "MERGE INTO %s USING DUAL ON (%s = ?) " +
		"WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)"
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
	case actionDelete:
		return w.delete(ctx, record)
	default:
		return w.upsert(ctx, record)
	}
}

// Close closes the underlying db connection.
func (w *Writer) Close(ctx context.Context) error {
	return w.db.Close()
}

// insert or update a record.
func (w *Writer) upsert(ctx context.Context, record sdk.Record) error {
	tableName := w.getTableName(record.Metadata)

	payload, err := w.structurizeData(record.Payload)
	if err != nil {
		return fmt.Errorf("structurize payload: %w", err)
	}

	// if payload is empty return empty payload error
	if payload == nil {
		return ErrEmptyPayload
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

	query, err := w.buildUpsertQuery(tableName, keyColumn, keyValue, columns, values)
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

// generates an SQL INSERT or UPDATE statement query via MERGE.
func (w *Writer) buildUpsertQuery(
	table, keyColumn string,
	keyValue any,
	columns []string,
	values []any,
) (string, error) {
	if len(columns) != len(values) {
		return "", errColumnsValuesLenMismatch
	}

	err := w.encodeValues(values)
	if err != nil {
		return "", fmt.Errorf("convert values: %w", err)
	}

	// arguments for the query
	args := make([]any, 0, len(values)*2)

	// append a key value as an argument
	args = append(args, keyValue)

	updateData := make([]string, 0, len(columns))
	for i := 0; i < len(columns); i++ {
		if columns[i] == keyColumn {
			continue
		}

		updateData = append(updateData, columns[i]+" = ?")

		// append all values for update (except the keyValue for update)
		args = append(args, values[i])
	}

	// append all values and question marks for insert
	placeholders := make([]string, len(values))
	for i := range values {
		args = append(args, values[i])
		placeholders[i] = "?"
	}

	sql := fmt.Sprintf(upsertFmt, table, keyColumn,
		strings.Join(updateData, ", "), strings.Join(columns, ", "), strings.Join(placeholders, ", "))

	query, err := sqlbuilder.DefaultFlavor.Interpolate(sql, args)
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

	return tableName
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
		i = 0

		columns = make([]string, len(payload))
		values  = make([]any, len(payload))
	)

	for key, value := range payload {
		columns[i] = key
		values[i] = value

		i++
	}

	return columns, values
}

// encodes values to right Oracle's types.
func (w *Writer) encodeValues(values []any) error {
	for i := range values {
		if values[i] != nil {
			switch reflect.TypeOf(values[i]).Kind() {
			case reflect.Bool: // convert the boolean type to the int, for the NUMBER(1,0) Oracle type
				if values[i].(bool) {
					values[i] = 1
				} else {
					values[i] = 0
				}
			case reflect.Map: // convert the map type to the string, for the VARCHAR2 Oracle type
				bs, err := json.Marshal(values[i])
				if err != nil {
					return fmt.Errorf("marshal map: %w", err)
				}

				values[i] = string(bs)
			}
		}
	}

	return nil
}
