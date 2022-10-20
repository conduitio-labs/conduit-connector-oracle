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

package coltypes

import (
	"context"
	"fmt"
	"strconv"

	"github.com/conduitio-labs/conduit-connector-oracle/repository"
	"github.com/godror/godror"
)

const (
	// oracle's data types.
	oracleTypeNumber = "NUMBER"
)

// queryColumnDescription is a query that selects columns' description by the table name.
var queryColumnDescription = `
SELECT 
    COLUMN_NAME as NAME,
    DATA_TYPE as TYPE,
    DATA_PRECISION as PRECISION,
    DATA_SCALE as SCALE
FROM ALL_TAB_COLUMNS
WHERE TABLE_NAME ='%s'
`

// ColumnDescription describes columns.
type ColumnDescription struct {
	Type      string
	Precision *int
	Scale     *int
}

// TransformRow converts row map values to appropriate Go types, based on the columnTypes.
func TransformRow(row map[string]any, columnTypes map[string]ColumnDescription) (map[string]any, error) {
	result := make(map[string]any, len(row))

	for key, value := range row {
		if value == nil {
			result[key] = nil

			continue
		}

		if columnTypes[key].Type == oracleTypeNumber {
			v, err := godror.Num.ConvertValue(value)
			if err != nil {
				return nil, fmt.Errorf("convert the oracle number type to the interface: %w", err)
			}

			value, err = strconv.Atoi(v.(string))
			if err != nil {
				return nil, fmt.Errorf("convert oracle an interface to the int: %w", err)
			}

			// if the type is NUMBER(1,0) takes it as a boolean type
			// (precision is 1, scale is 0)
			if columnTypes[key].Precision != nil && *columnTypes[key].Precision == 1 &&
				columnTypes[key].Scale != nil && *columnTypes[key].Scale == 0 {
				if value == 1 {
					result[key] = true
				} else {
					result[key] = false
				}

				continue
			}
		}

		result[key] = value
	}

	return result, nil
}

// GetColumnTypes returns a map containing all table's columns and their database data.
func GetColumnTypes(
	ctx context.Context,
	repo *repository.Oracle,
	tableName string,
) (map[string]ColumnDescription, error) {
	var columnName string

	rows, err := repo.DB.QueryxContext(ctx, fmt.Sprintf(queryColumnDescription, tableName))
	if err != nil {
		return nil, fmt.Errorf("query column types: %w", err)
	}

	columnTypes := make(map[string]ColumnDescription)
	for rows.Next() {
		columnDescription := ColumnDescription{}

		if err = rows.Scan(&columnName,
			&columnDescription.Type, &columnDescription.Precision, &columnDescription.Scale); err != nil {
			return nil, fmt.Errorf("scan rows: %w", err)
		}

		columnTypes[columnName] = columnDescription
	}

	return columnTypes, nil
}
