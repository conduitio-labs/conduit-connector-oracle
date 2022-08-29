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

const numberType = "NUMBER"

// queryColumnData is a query that selects columns' data by the table name.
var queryColumnData = `
SELECT 
    COLUMN_NAME as NAME,
    DATA_TYPE as TYPE,
    DATA_PRECISION as PRECISION,
    DATA_SCALE as SCALE
FROM ALL_TAB_COLUMNS
WHERE TABLE_NAME ='%s'
`

// ColumnData represents a columns' data.
type ColumnData struct {
	Type      string
	Precision *int
	Scale     *int
}

// TransformRow converts row map values to appropriate Go types, based on the columnTypes.
func TransformRow(row map[string]any, columnTypes map[string]ColumnData) (map[string]any, error) {
	result := make(map[string]any, len(row))

	for key, value := range row {
		if value == nil {
			result[key] = nil

			continue
		}

		data := columnTypes[key]
		if data.Type == numberType {
			valStr, err := godror.Num.ConvertValue(value)
			if err != nil {
				return nil, fmt.Errorf("convert oracle number type to string: %w", err)
			}

			value, err = strconv.Atoi(valStr.(string))
			if err != nil {
				return nil, fmt.Errorf("convert oracle number type to int: %w", err)
			}

			if data.Precision != nil && *data.Precision == 1 && data.Scale != nil && *data.Scale == 0 {
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
func GetColumnTypes(ctx context.Context, repo *repository.Oracle, tableName string) (map[string]ColumnData, error) {
	var columnName string

	rows, err := repo.DB.QueryxContext(ctx, fmt.Sprintf(queryColumnData, tableName))
	if err != nil {
		return nil, fmt.Errorf("query column types: %w", err)
	}

	columnTypes := make(map[string]ColumnData)
	for rows.Next() {
		columnData := ColumnData{}

		if er := rows.Scan(&columnName, &columnData.Type, &columnData.Precision, &columnData.Scale); er != nil {
			return nil, fmt.Errorf("scan rows: %w", er)
		}

		columnTypes[columnName] = columnData
	}

	return columnTypes, nil
}
