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

package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/huandu/go-sqlbuilder"
)

const (
	GodrorDriver = "godror"

	// upsert sql format.
	upsertFmt = "MERGE INTO %s USING DUAL ON (%s = ?) " +
		"WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)"
)

type Oracle struct {
	db *sql.DB
}

func New(dataSourceName string) (*Oracle, error) {
	db, err := sql.Open(GodrorDriver, dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("open connection: %w", err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}

	return &Oracle{db: db}, nil
}

func (o Oracle) Close() error {
	return o.db.Close()
}

func (o Oracle) Upsert(
	ctx context.Context,
	table, keyColumn string,
	keyValue any,
	columns []string,
	values []any,
) error {
	query, err := o.buildUpsertQuery(table, keyColumn, keyValue, columns, values)
	if err != nil {
		return fmt.Errorf("build upsert query: %w", err)
	}

	_, err = o.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("exec upsert: %w", err)
	}

	return nil
}

// Delete deletes data from the table.
func (o Oracle) Delete(ctx context.Context, table, keyColumn string, keyValue any) error {
	db := sqlbuilder.NewDeleteBuilder()

	db.DeleteFrom(table)
	db.Where(
		db.Equal(keyColumn, keyValue),
	)

	query, err := o.buildDeleteQuery(table, keyColumn, keyValue)
	if err != nil {
		return fmt.Errorf("build delete query: %w", err)
	}

	_, err = o.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("exec delete: %w", err)
	}

	return nil
}

func (o Oracle) buildUpsertQuery(
	table, keyColumn string,
	keyValue any,
	columns []string,
	values []any,
) (string, error) {
	if len(columns) != len(values) {
		return "", errColumnsValuesLenMismatch
	}

	err := o.encodeValues(values)
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

// Delete deletes data from the table.
func (o Oracle) buildDeleteQuery(table, keyColumn string, keyValue any) (string, error) {
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

// encodes values to right Oracle's types.
func (o Oracle) encodeValues(values []any) error {
	for i := range values {
		if values[i] != nil {
			switch reflect.TypeOf(values[i]).Kind() {
			case reflect.Bool: // convert the boolean type to the int, for the NUMBER(1,0) Oracle type
				if values[i].(bool) {
					values[i] = 1
				} else {
					values[i] = 0
				}
			case reflect.Map, reflect.Slice: // convert the map type to the string, for the VARCHAR2 Oracle type
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
