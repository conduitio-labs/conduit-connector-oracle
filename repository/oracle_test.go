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
	"reflect"
	"testing"
	"time"
)

func TestOracle_buildUpsertQuery(t *testing.T) {
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
				columns:   []string{"name", "age", "created", "metadata"},
				values:    []any{"John", 42, time.Unix(1257894000, 0).UTC(), map[string]string{"action": "insert"}},
			},
			want: "MERGE INTO users USING DUAL ON (id = 'someId') " +
				"WHEN MATCHED THEN " +
				"UPDATE SET name = 'John', age = 42, created = '2009-11-10 23:00:00', " +
				"metadata = '{\\\"action\\\":\\\"insert\\\"}' " +
				"WHEN NOT MATCHED THEN " +
				"INSERT (name, age, created, metadata) " +
				"VALUES ('John', 42, '2009-11-10 23:00:00', '{\\\"action\\\":\\\"insert\\\"}')",
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

			got, err := new(Oracle).buildUpsertQuery(
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

func TestOracle_buildDeleteQuery(t *testing.T) {
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

			got, err := new(Oracle).buildDeleteQuery(tt.args.table, tt.args.keyColumn, tt.args.keyValue)
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

func TestOracle_convertValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		values []any
		want   []any
	}{
		{
			name:   "success with true value",
			values: []any{"John", true},
			want:   []any{"John", 1},
		},
		{
			name:   "success with false value",
			values: []any{"John", false},
			want:   []any{"John", 0},
		},
		{
			name:   "success with map type",
			values: []any{"John", map[string]string{"action": "insert"}},
			want:   []any{"John", "{\"action\":\"insert\"}"},
		},
	}
	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := new(Oracle).encodeValues(tt.values)
			if err != nil {
				t.Errorf("unexpected error %s", err.Error())
			}

			if len(tt.values) != len(tt.want) {
				t.Error("values length must stay the same")

				return
			}

			if !reflect.DeepEqual(tt.values, tt.want) {
				t.Errorf("got: %v, want: %v", tt.values, tt.want)
			}
		})
	}
}
