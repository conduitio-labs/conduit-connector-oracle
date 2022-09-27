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
	"testing"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func TestFormatData(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	const expectedTime = "1989-10-04 13:14:15"

	columnTypes := make(map[string]ColumnData)
	payload := make(sdk.StructuredData)
	expectedPlaceholder := make(map[string]interface{})
	expectedArg := map[string]any{}

	key := "DATA_MAP"
	payload[key] = map[string]interface{}{
		"int_val":    123,
		"bool_val":   true,
		"string_val": "test",
	}
	expectedPlaceholder[key] = ":{placeholder}"
	expectedArg[key] = `{"bool_val":true,"int_val":123,"string_val":"test"}`

	key = "DATA_SLICE"
	payload[key] = []interface{}{123, true, "test"}
	expectedPlaceholder[key] = ":{placeholder}"
	expectedArg[key] = `[123,true,"test"]`

	key = "IS_ACTIVE_TRUE"
	precision, scale := 1, 0
	columnTypes[key] = ColumnData{
		Type:      "NUMBER",
		Precision: &precision,
		Scale:     &scale,
	}
	payload[key] = true
	expectedPlaceholder[key] = ":{placeholder}"
	expectedArg[key] = 1

	key = "IS_ACTIVE_FALSE"
	precision, scale = 1, 0
	columnTypes[key] = ColumnData{
		Type:      "NUMBER",
		Precision: &precision,
		Scale:     &scale,
	}
	payload[key] = false
	expectedPlaceholder[key] = ":{placeholder}"
	expectedArg[key] = 0

	key = "CREATED_AT"
	columnTypes[key] = ColumnData{
		Type: "DATE",
	}
	payload[key] = time.Date(1989, 10, 04, 13, 14, 15, 0, time.UTC)
	expectedPlaceholder[key] = "TO_DATE(:{placeholder}, 'YYYY-MM-DD HH24:MI:SS')"
	expectedArg[key] = expectedTime

	key = "CREATED_AT_STRING"
	columnTypes[key] = ColumnData{
		Type: "DATE",
	}
	payload[key] = "Wed, 04 Oct 1989 13:14:15 UTC"
	expectedPlaceholder[key] = "TO_DATE(:{placeholder}, 'YYYY-MM-DD HH24:MI:SS')"
	expectedArg[key] = expectedTime

	for k, v := range payload {
		placeholder, arg, err := FormatData(columnTypes, k, v)
		is.NoErr(err)
		is.Equal(placeholder, expectedPlaceholder[k])
		is.Equal(arg, expectedArg[k])
	}
}
