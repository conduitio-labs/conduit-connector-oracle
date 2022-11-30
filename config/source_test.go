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
	"fmt"
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
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: strings.ToUpper(testTable),
				},
				OrderingColumn: "ID",
				Snapshot:       defaultSnapshot,
				BatchSize:      defaultBatchSize,
			},
		},
		{
			name: "success_keyColumn_has_one_key",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumns:     "id",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: strings.ToUpper(testTable),
				},
				OrderingColumn: "ID",
				KeyColumns:     []string{"ID"},
				Snapshot:       defaultSnapshot,
				BatchSize:      defaultBatchSize,
			},
		},
		{
			name: "success_keyColumn_has_two_keys",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumns:     "id,name",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: strings.ToUpper(testTable),
				},
				OrderingColumn: "ID",
				KeyColumns:     []string{"ID", "NAME"},
				Snapshot:       defaultSnapshot,
				BatchSize:      defaultBatchSize,
			},
		},
		{
			name: "success_keyColumn_space_between_keys",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumns:     "id, name",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: strings.ToUpper(testTable),
				},
				OrderingColumn: "ID",
				KeyColumns:     []string{"ID", "NAME"},
				Snapshot:       defaultSnapshot,
				BatchSize:      defaultBatchSize,
			},
		},
		{
			name: "success_keyColumn_starts_with_space",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumns:     " id,name",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: strings.ToUpper(testTable),
				},
				OrderingColumn: "ID",
				KeyColumns:     []string{"ID", "NAME"},
				Snapshot:       defaultSnapshot,
				BatchSize:      defaultBatchSize,
			},
		},
		{
			name: "success_keyColumn_ends_with_space",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumns:     "id,name ",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: strings.ToUpper(testTable),
				},
				OrderingColumn: "ID",
				KeyColumns:     []string{"ID", "NAME"},
				Snapshot:       defaultSnapshot,
				BatchSize:      defaultBatchSize,
			},
		},
		{
			name: "success_keyColumn_space_between_keys_before_comma",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumns:     "id ,name ",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: strings.ToUpper(testTable),
				},
				OrderingColumn: "ID",
				KeyColumns:     []string{"ID", "NAME"},
				Snapshot:       defaultSnapshot,
				BatchSize:      defaultBatchSize,
			},
		},
		{
			name: "success_keyColumn_two_spaces",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumns:     "id,  name ",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: strings.ToUpper(testTable),
				},
				OrderingColumn: "ID",
				KeyColumns:     []string{"ID", "NAME"},
				Snapshot:       defaultSnapshot,
				BatchSize:      defaultBatchSize,
			},
		},
		{
			name: "success_snapshot_is_false",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				Snapshot:       "false",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: strings.ToUpper(testTable),
				},
				OrderingColumn: "ID",
				Snapshot:       false,
				BatchSize:      defaultBatchSize,
			},
		},
		{
			name: "success_custom_batchSize",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumns:     "id",
				BatchSize:      "100",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: strings.ToUpper(testTable),
				},
				OrderingColumn: "ID",
				KeyColumns:     []string{"ID"},
				Snapshot:       defaultSnapshot,
				BatchSize:      100,
			},
		},
		{
			name: "success_batchSize_max",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumns:     "id",
				BatchSize:      "100000",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: strings.ToUpper(testTable),
				},
				OrderingColumn: "ID",
				KeyColumns:     []string{"ID"},
				Snapshot:       defaultSnapshot,
				BatchSize:      100000,
			},
		},
		{
			name: "success_batchSize_min",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumns:     "id",
				BatchSize:      "1",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: strings.ToUpper(testTable),
				},
				OrderingColumn: "ID",
				KeyColumns:     []string{"ID"},
				Snapshot:       defaultSnapshot,
				BatchSize:      1,
			},
		},
		{
			name: "success_custom_columns",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumns:     "id",
				Columns:        "id, name,age",
			},
			want: Source{
				Configuration: Configuration{
					URL:   testURL,
					Table: strings.ToUpper(testTable),
				},
				OrderingColumn: "ID",
				KeyColumns:     []string{"ID"},
				Snapshot:       defaultSnapshot,
				BatchSize:      defaultBatchSize,
				Columns:        []string{"ID", "NAME", "AGE"},
			},
		},
		{
			name: "failure_required_orderingColumn",
			in: map[string]string{
				URL:        testURL,
				Table:      testTable,
				KeyColumns: "id",
				Columns:    "id,name,age",
			},
			err: errors.New(`"orderingColumn" value must be set`),
		},
		{
			name: "failure_invalid_snapshot",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				Snapshot:       "test",
			},
			err: errors.New(`parse "snapshot": strconv.ParseBool: parsing "test": invalid syntax`),
		},
		{
			name: "failure_invalid_batchSize",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumns:     "id",
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
				KeyColumns:     "name",
				Columns:        "name,age",
			},
			err: fmt.Errorf("columns must include %q", OrderingColumn),
		},
		{
			name: "failure_missed_keyColumn_in_columns",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumns:     "name",
				Columns:        "id,age",
			},
			err: fmt.Errorf("columns must include all %q", KeyColumns),
		},
		{
			name: "failure_batchSize_is_too_big",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumns:     "id",
				BatchSize:      "100001",
			},
			err: fmt.Errorf("%q is out of range", BatchSize),
		},
		{
			name: "failure_batchSize_is_zero",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumns:     "id",
				BatchSize:      "0",
			},
			err: fmt.Errorf("%q is out of range", BatchSize),
		},
		{
			name: "failure_batchSize_is_negative",
			in: map[string]string{
				URL:            testURL,
				Table:          testTable,
				OrderingColumn: "id",
				KeyColumns:     "id",
				BatchSize:      "-1",
			},
			err: fmt.Errorf("%q is out of range", BatchSize),
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
