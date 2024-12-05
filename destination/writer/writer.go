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

package writer

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/conduitio-labs/conduit-connector-oracle/columntypes"
	"github.com/conduitio-labs/conduit-connector-oracle/repository"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (
	upsertFmt = "MERGE INTO %s USING DUAL ON (%s = %s) " +
		"WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)"
	deleteFmt = "DELETE FROM %s WHERE %s = :1"
	coma      = ","
	equal     = "="

	metadataTable = "oracle.table"
)

// Writer implements a writer logic for Oracle destination.
type Writer struct {
	repo        *repository.Oracle
	table       string
	keyColumn   string
	columnTypes map[string]columntypes.ColumnDescription
}

// Params is an incoming params for the New function.
type Params struct {
	Repo      *repository.Oracle
	Table     string
	KeyColumn string
}

// New creates new instance of the Writer.
func New(ctx context.Context, params Params) (*Writer, error) {
	writer := &Writer{
		repo:      params.Repo,
		table:     params.Table,
		keyColumn: params.KeyColumn,
	}

	columnTypes, err := columntypes.GetColumnTypes(ctx, writer.repo, writer.table)
	if err != nil {
		return nil, fmt.Errorf("get column types: %w", err)
	}
	writer.columnTypes = columnTypes

	return writer, nil
}

// Write writes a opencdc.Record into a Destination.
func (w *Writer) Write(ctx context.Context, record opencdc.Record) error {
	switch record.Operation {
	case opencdc.OperationDelete:
		return w.delete(ctx, record)
	default:
		return w.upsert(ctx, record)
	}
}

// insert or update a record.
func (w *Writer) upsert(ctx context.Context, record opencdc.Record) error {
	tableName := w.getTableName(record.Metadata)

	payload, err := w.structurizeData(record.Payload.After)
	if err != nil {
		return fmt.Errorf("structurize payload: %w", err)
	}

	// if payload is empty return empty payload error
	if payload == nil {
		return ErrEmptyPayload
	}

	key, err := w.structurizeData(record.Key)
	if err != nil {
		// if the key is not structured, we simply ignore it
		// we'll try to insert just a payload in this case
		sdk.Logger(ctx).Debug().Msgf("structurize key during upsert: %v", err)
	}

	keyColumn, err := w.getKeyColumn(key)
	if err != nil {
		return fmt.Errorf("get key column: %w", err)
	}

	// if the record doesn't contain the key, insert the key if it's not empty
	if _, ok := payload[keyColumn]; !ok {
		if _, ok := key[keyColumn]; ok {
			payload[keyColumn] = key[keyColumn]
		}
	}

	query, args, err := w.buildUpsertQuery(tableName, keyColumn, payload)
	if err != nil {
		return fmt.Errorf("build upsert query: %w", err)
	}

	_, err = w.repo.DB.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("exec upsert %q, %v: %w", query, args, err)
	}

	return nil
}

// deletes records by a key. First it looks in the opencdc.Record.Key,
// if it doesn't find a key there it will use the default configured value for a key.
func (w *Writer) delete(ctx context.Context, record opencdc.Record) error {
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

	query := fmt.Sprintf(deleteFmt, tableName, keyColumn)

	_, err = w.repo.DB.ExecContext(ctx, query, keyValue)
	if err != nil {
		return fmt.Errorf("exec delete %q, %v: %w", query, keyValue, err)
	}

	return nil
}

// generates an SQL INSERT or UPDATE statement query via MERGE.
func (w *Writer) buildUpsertQuery(
	table, keyColumn string,
	payload opencdc.StructuredData,
) (string, []any, error) {
	columns, placeholdersMap, argsMap, err := w.extractPayload(payload)
	if err != nil {
		return "", nil, err
	}

	// arguments for the query
	args := make([]any, 0, len(columns)*2)

	// append a key value as an argument
	args = append(args, payload[keyColumn])

	updateData := make([]string, 0, len(columns))
	placeholders := make([]string, 0, len(columns))
	insertArgs := make([]any, 0, len(columns))

	for i := range columns {
		placeholders = append(placeholders, placeholdersMap[columns[i]])

		// append all arguments for insert
		insertArgs = append(insertArgs, argsMap[columns[i]])

		if columns[i] == keyColumn {
			continue
		}

		updateData = append(updateData, columns[i]+equal+placeholdersMap[columns[i]])

		// append all arguments for update (except the keyValue)
		args = append(args, argsMap[columns[i]])
	}

	// append insert arguments to the end
	args = append(args, insertArgs...)

	query := fmt.Sprintf(upsertFmt, table, keyColumn, columntypes.QueryPlaceholder,
		strings.Join(updateData, coma), strings.Join(columns, coma), strings.Join(placeholders, coma))

	// replace all strings '{placeholder}' to the Oracle's format (:1, :2, ...)
	for i := 0; i < len(args); i++ {
		query = strings.Replace(query, columntypes.QueryPlaceholder[1:], strconv.Itoa(i+1), 1)
	}

	return query, args, nil
}

// returns either the record metadata value for the table
// or the default configured value for the table.
func (w *Writer) getTableName(metadata map[string]string) string {
	tableName, ok := metadata[metadataTable]
	if !ok {
		return w.table
	}

	return tableName
}

// returns either the first key within the Key structured data
// or the default key configured value for key.
func (w *Writer) getKeyColumn(key opencdc.StructuredData) (string, error) {
	if len(key) > 1 {
		return "", errCompositeKeysNotSupported
	}

	for k := range key {
		return k, nil
	}

	return w.keyColumn, nil
}

// converts opencdc.Data to opencdc.StructuredData.
func (w *Writer) structurizeData(data opencdc.Data) (opencdc.StructuredData, error) {
	if data == nil || len(data.Bytes()) == 0 {
		return nil, nil
	}

	unmarshalledData := make(opencdc.StructuredData)
	if err := json.Unmarshal(data.Bytes(), &unmarshalledData); err != nil {
		return nil, fmt.Errorf("unmarshal %q into structured data: %w", string(data.Bytes()), err)
	}

	structuredData := make(opencdc.StructuredData, len(unmarshalledData))
	for k, v := range unmarshalledData {
		structuredData[strings.ToUpper(k)] = v
	}

	return structuredData, nil
}

// turns the payload into slice of columns, a map of placeholders, and a map of arguments.
func (w *Writer) extractPayload(payload opencdc.StructuredData) ([]string, map[string]string, map[string]any, error) {
	var (
		i               = 0
		columns         = make([]string, len(payload))
		placeholdersMap = make(map[string]string, len(payload))
		argsMap         = make(map[string]any, len(payload))

		err error
	)

	for key, value := range payload {
		columns[i] = key

		placeholdersMap[key], argsMap[key], err = columntypes.FormatData(w.columnTypes, key, value)
		if err != nil {
			return nil, nil, nil, err
		}

		i++
	}

	sort.Strings(columns)

	return columns, placeholdersMap, argsMap, nil
}
