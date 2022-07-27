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
	"reflect"
	"testing"
	"time"
)

func TestWriter_buildInsertQuery(t *testing.T) {
	t.Parallel()

	type args struct {
		table   string
		columns []string
		values  []any
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
				table:   "users",
				columns: []string{"name", "age", "created_at"},
				values:  []any{"John", 42, time.Unix(1257894000, 0).UTC()},
			},
			want: "INSERT INTO users (name, age, created_at) VALUES ('John', 42, '2009-11-10 23:00:00')",
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
			got, err := w.buildInsertQuery(tt.args.table, tt.args.columns, tt.args.values)
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

func TestWriter_buildUpdateQuery(t *testing.T) {
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
				keyValue:  1,
				columns:   []string{"name", "age", "created_at"},
				values:    []any{"John", 42, time.Unix(1257894000, 0).UTC()},
			},
			want: "UPDATE users SET name = 'John', age = 42, created_at = '2009-11-10 23:00:00' WHERE id = 1",
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
			got, err := w.buildUpdateQuery(
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

func TestWriter_convertValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		values []any
		want   []any
	}{
		{
			name:   "success with true value",
			values: []any{"John", 42, true},
			want:   []any{"John", 42, 1},
		},
		{
			name:   "success with false value",
			values: []any{"John", 42, false},
			want:   []any{"John", 42, 0},
		},
	}
	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w := &Writer{}
			w.convertValues(tt.values)

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
