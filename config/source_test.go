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

package config

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestParseSource(t *testing.T) {
	tests := []struct {
		name string
		in   map[string]string
		want Source
		err  error
	}{
		{
			name: "success_required_values",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumn:      "id",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: strings.ToUpper(testTable),
				},
				OrderingColumn: "ID",
				KeyColumn:      "ID",
				BatchSize:      defaultBatchSize,
			},
		},
		{
			name: "success_custom_batchSize",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumn:      "id",
				BatchSize:      "100",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: strings.ToUpper(testTable),
				},
				OrderingColumn: "ID",
				KeyColumn:      "ID",
				BatchSize:      100,
			},
		},
		{
			name: "success_batchSize_max",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumn:      "id",
				BatchSize:      "100000",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: strings.ToUpper(testTable),
				},
				OrderingColumn: "ID",
				KeyColumn:      "ID",
				BatchSize:      100000,
			},
		},
		{
			name: "success_batchSize_min",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumn:      "id",
				BatchSize:      "1",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: strings.ToUpper(testTable),
				},
				OrderingColumn: "ID",
				KeyColumn:      "ID",
				BatchSize:      1,
			},
		},
		{
			name: "success_custom_columns",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumn:      "id",
				Columns:        "id, name,age",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: strings.ToUpper(testTable),
				},
				OrderingColumn: "ID",
				KeyColumn:      "ID",
				BatchSize:      defaultBatchSize,
				Columns:        []string{"ID", "NAME", "AGE"},
			},
		},
		{
			name: "failure_required_orderingColumn",
			in: map[string]string{
				URL:       testURL,
				Table:     testTable,
				KeyColumn: "id",
				Columns:   "id,name,age",
			},
			err: errors.New(`"orderingColumn" value must be set`),
		},
		{
			name: "failure_required_keyColumn",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				Columns:        "id,name,age",
				OrderingColumn: "id",
			},
			err: errors.New(`"keyColumn" value must be set`),
		},
		{
			name: "failure_invalid_batchSize",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumn:      "id",
				BatchSize:      "a",
			},
			err: errors.New(`parse BatchSize: strconv.Atoi: parsing "a": invalid syntax`),
		},
		{
			name: "failure_missed_orderingColumn_in_columns",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumn:      "name",
				Columns:        "name,age",
			},
			err: errColumnInclude(),
		},
		{
			name: "failure_missed_keyColumn_in_columns",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumn:      "name",
				Columns:        "id,age",
			},
			err: errColumnInclude(),
		},
		{
			name: "failure_batchSize_is_too_big",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumn:      "id",
				BatchSize:      "100001",
			},
			err: errOutOfRange(BatchSize),
		},
		{
			name: "failure_batchSize_is_zero",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumn:      "id",
				BatchSize:      "0",
			},
			err: errOutOfRange(BatchSize),
		},
		{
			name: "failure_batchSize_is_negative",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumn:      "id",
				BatchSize:      "-1",
			},
			err: errOutOfRange(BatchSize),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseSource(tt.in)
			if err != nil {
				if tt.err == nil {
					t.Errorf("unexpected error: %s", err.Error())

					return
				}

				if err.Error() != tt.err.Error() {
					t.Errorf("unexpected error, got: %s, want: %s", err.Error(), tt.err.Error())

					return
				}

				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got: %v, want: %v", got, tt.want)
			}
		})
	}
}
