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
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/conduitio-labs/conduit-connector-oracle/repository"
	"github.com/godror/godror"
)

const (
	// QueryPlaceholder is a temporary placeholder to replace with the Oracle's placeholders.
	QueryPlaceholder = ":{placeholder}"

	// oracle's data types.
	oracleTypeNumber    = "NUMBER"
	oracleTypeDate      = "DATE"
	oracleTypeTimestamp = "TIMESTAMP"
	oracleTypeBlob      = "BLOB"

	// oracle's date and timestamp layouts.
	oracleLayoutTime = "2006-01-02 15:04:05"
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

// FormatData formats the data into Oracle types
// and wraps the placeholder in a special function, if necessary.
func FormatData(
	columnTypes map[string]ColumnData,
	key string, value any,
) (string, any, error) {
	if value == nil {
		return QueryPlaceholder, nil, nil
	}

	switch oracleColumnData := columnTypes[strings.ToUpper(key)]; {
	// if the column type is a NUMBER, precision is 1, scale is 0, and value is a boolean,
	// convert it to the integer, when the "true" value is 1, and "false" is 0
	case oracleColumnData.Type == oracleTypeNumber &&
		oracleColumnData.Precision != nil && *oracleColumnData.Precision == 1 &&
		oracleColumnData.Scale != nil && *oracleColumnData.Scale == 0:
		valueBool, ok := value.(bool)
		if !ok {
			return QueryPlaceholder, value, nil
		}

		if valueBool {
			return QueryPlaceholder, 1, nil
		}

		return QueryPlaceholder, 0, nil

	case oracleColumnData.Type == oracleTypeDate, strings.Contains(oracleColumnData.Type, oracleTypeTimestamp):
		v, err := formatDate(value)
		if err != nil {
			return "", nil, err
		}

		return fmt.Sprintf("TO_DATE(%s, 'YYYY-MM-DD HH24:MI:SS')", QueryPlaceholder), v, nil

	case strings.Contains(oracleColumnData.Type, oracleTypeBlob):
		v, err := formatBlob(value)
		if err != nil {
			return "", nil, err
		}

		return fmt.Sprintf("utl_raw.cast_to_raw(%s)", QueryPlaceholder), v, nil
	}

	switch reflect.TypeOf(value).Kind() {
	case reflect.Map, reflect.Slice: // convert the map type to the string, for the VARCHAR2 Oracle type
		bs, err := json.Marshal(value)
		if err != nil {
			return "", nil, fmt.Errorf("marshal map or slice: %w", err)
		}

		return QueryPlaceholder, string(bs), nil
	}

	return QueryPlaceholder, value, nil
}

func formatDate(value interface{}) (string, error) {
	valueTime, ok := value.(time.Time)
	if ok {
		return valueTime.Format(oracleLayoutTime), nil
	}

	valueStr, ok := value.(string)
	if ok {
		val, err := parseTime(valueStr)
		if err != nil {
			return "", fmt.Errorf("convert value %q to time.Time: %w", valueStr, err)
		}

		return val.Format(oracleLayoutTime), nil
	}

	return "", fmt.Errorf("format value %q of type date", value)
}

func formatBlob(value interface{}) ([]byte, error) {
	valueBytes, ok := value.([]byte)
	if ok {
		return valueBytes, nil
	}

	valueStr, ok := value.(string)
	if ok {
		return []byte(valueStr), nil
	}

	return nil, fmt.Errorf("format value %q of type []byte", value)
}

func parseTime(val string) (time.Time, error) {
	for i := range timeLayouts {
		timeValue, err := time.Parse(timeLayouts[i], val)
		if err != nil {
			continue
		}

		return timeValue, nil
	}

	return time.Time{}, fmt.Errorf("%s - %w", val, errInvalidTimeLayout)
}
