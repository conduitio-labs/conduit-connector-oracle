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

package writer

import (
	"testing"
	"time"
)

func TestWriter_buildUpsertQuery(t *testing.T) {
	t.Parallel()

	type args struct {
		table     string
		keyColumn string
		keyValue  any
		columns   []string
		values    []any
	}

	tests := []struct {
		name string
		args args
		want string
		err  error
	}{
		{
			name: "success",
			args: args{
				table:     "users",
				keyColumn: "id",
				keyValue:  "someId",
				columns:   []string{"col_string", "col_int", "col_ts", "col_map", "col_slice"},
				values: []any{
					"John", 42, time.Unix(1257894000, 0).UTC(), "{\"key\":\"value\"}", "[1,\"test\"]",
				},
			},
			want: "MERGE INTO users USING DUAL ON (id = 'someId') " +
				"WHEN MATCHED THEN " +
				"UPDATE SET col_string = 'John', col_int = 42, col_ts = '2009-11-10 23:00:00', " +
				"col_map = '{\\\"key\\\":\\\"value\\\"}', col_slice = '[1,\\\"test\\\"]' " +
				"WHEN NOT MATCHED THEN " +
				"INSERT (col_string, col_int, col_ts, col_map, col_slice) " +
				"VALUES ('John', 42, '2009-11-10 23:00:00', '{\\\"key\\\":\\\"value\\\"}', '[1,\\\"test\\\"]')",
		},
		{
			name: "mismatch columns and values length",
			args: args{
				table:   "users",
				columns: []string{"name"},
				values:  []any{"John", 42},
			},
			err: errColumnsValuesLenMismatch,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w := &Writer{}
			got, err := w.buildUpsertQuery(
				tt.args.table, tt.args.keyColumn, tt.args.keyValue, tt.args.columns, tt.args.values)
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

			if got != tt.want {
				t.Errorf("got: %v, want: %v", got, tt.want)
			}
		})
	}
}

func TestWriter_buildDeleteQuery(t *testing.T) {
	t.Parallel()

	type args struct {
		table     string
		keyColumn string
		keyValue  any
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "success",
			args: args{
				table:     "users",
				keyColumn: "id",
				keyValue:  1,
			},
			want: "DELETE FROM users WHERE id = 1",
		},
	}
	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w := &Writer{}
			got, err := w.buildDeleteQuery(tt.args.table, tt.args.keyColumn, tt.args.keyValue)
			if err != nil {
				t.Errorf("unexpected error: %s", err.Error())

				return
			}

			if got != tt.want {
				t.Errorf("got: %v, want: %v", got, tt.want)
			}
		})
	}
}
