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

func TestConvertStructureData(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	const expectedTime = "1989-10-04 13:14:15"

	columnTypes := make(map[string]ColumnData)
	payload := make(sdk.StructuredData)
	expected := make(map[string]interface{})

	key := "DATA_MAP"
	payload[key] = map[string]interface{}{
		"int_val":    123,
		"bool_val":   true,
		"string_val": "test",
	}
	expected[key] = `{"bool_val":true,"int_val":123,"string_val":"test"}`

	key = "DATA_SLICE"
	payload[key] = []interface{}{123, true, "test"}
	expected[key] = `[123,true,"test"]`

	key = "IS_ACTIVE_TRUE"
	precision, scale := 1, 0
	columnTypes[key] = ColumnData{
		Type:      "NUMBER",
		Precision: &precision,
		Scale:     &scale,
	}
	payload[key] = true
	expected[key] = 1

	key = "IS_ACTIVE_FALSE"
	precision, scale = 1, 0
	columnTypes[key] = ColumnData{
		Type:      "NUMBER",
		Precision: &precision,
		Scale:     &scale,
	}
	payload[key] = false
	expected[key] = 0

	key = "CREATED_AT"
	columnTypes[key] = ColumnData{
		Type: "DATE",
	}
	payload[key] = time.Date(1989, 10, 04, 13, 14, 15, 0, time.UTC)
	expected[key] = expectedTime

	key = "CREATED_AT_STRING"
	columnTypes[key] = ColumnData{
		Type: "DATE",
	}
	payload[key] = "Wed, 04 Oct 1989 13:14:15 UTC"
	expected[key] = expectedTime

	key = "TS_INT"
	columnTypes[key] = ColumnData{
		Type: "TIMESTAMP(6)",
	}
	payload[key] = 623510055
	expected[key] = expectedTime

	key = "TS_FLOAT64"
	columnTypes[key] = ColumnData{
		Type: "TIMESTAMP(6)",
	}
	payload[key] = float64(623510055)
	expected[key] = expectedTime

	key = "TS_STRING"
	columnTypes[key] = ColumnData{
		Type: "TIMESTAMP(6)",
	}
	payload[key] = "623510055"
	expected[key] = expectedTime

	got, err := ConvertStructureData(columnTypes, payload)
	is.NoErr(err)
	is.True(got != nil)
	is.Equal(len(payload), len(got))
	for k, v := range got {
		is.Equal(expected[k], v)
	}
}
