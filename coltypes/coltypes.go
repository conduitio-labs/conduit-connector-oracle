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
	"strings"
	"time"

	"github.com/conduitio-labs/conduit-connector-oracle/repository"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/godror/godror"
)

const (
	// oracle's data types.
	numberType = "NUMBER"
	date       = "DATE"
	timestamp  = "TIMESTAMP"

	// oracle's date and timestamp layouts.
	timeLayout = "2006-01-02 15:04:05"
)

var (
	timeLayouts = []string{time.RFC3339, time.RFC3339Nano, time.Layout, time.ANSIC, time.UnixDate, time.RubyDate,
		time.RFC822, time.RFC822Z, time.RFC850, time.RFC1123, time.RFC1123Z, time.RFC3339, time.RFC3339,
		time.RFC3339Nano, time.Kitchen, time.Stamp, time.StampMilli, time.StampMicro, time.StampNano}
)

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

		if columnTypes[key].Type == numberType {
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

// ConvertStructureData converts a sdk.StructureData values to a proper database types.
func ConvertStructureData(
	columnTypes map[string]ColumnData,
	data sdk.StructuredData,
) (sdk.StructuredData, error) {
	result := make(sdk.StructuredData, len(data))

	for key, value := range data {
		if value == nil {
			result[key] = nil

			continue
		}

		switch datatype := columnTypes[strings.ToUpper(key)].Type; {
		case datatype == date:
			valueTime, ok := value.(time.Time)
			if ok {
				result[key] = valueTime.Format(timeLayout)

				continue
			}

			valueStr, ok := value.(string)
			if ok {
				val, err := parseToTime(valueStr)
				if err != nil {
					return nil, fmt.Errorf("convert value %q to time.Time: %w", valueStr, err)
				}

				result[key] = val.Format(timeLayout)

				continue
			}

			return nil, fmt.Errorf("parse value %q of type date", value)

		case strings.Contains(datatype, timestamp):
			valueStr, ok := value.(string)
			if ok {
				valueInt64, err := strconv.ParseInt(valueStr, 0, 64)
				if err != nil {
					return nil, fmt.Errorf("convert value %q to int64: %w", valueStr, err)
				}

				result[key] = strings.ToUpper(time.Unix(valueInt64, 0).Format(timeLayout))

				continue
			}

			valueFloat64, ok := value.(float64)
			if ok {
				result[key] = strings.ToUpper(time.Unix(int64(valueFloat64), 0).Format(timeLayout))

				continue
			}

			valueInt, ok := value.(int)
			if ok {
				result[key] = strings.ToUpper(time.Unix(int64(valueInt), 0).Format(timeLayout))

				continue
			}

			return nil, fmt.Errorf("parse value %q of type timestamp", value)

		default:
			result[key] = value
		}
	}

	return result, nil
}

func parseToTime(val string) (time.Time, error) {
	for i := range timeLayouts {
		timeValue, err := time.Parse(timeLayouts[i], val)
		if err != nil {
			continue
		}

		return timeValue, nil
	}

	return time.Time{}, fmt.Errorf("%s - %w", val, errInvalidTimeLayout)
}
