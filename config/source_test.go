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

package config

import (
	"errors"
	"reflect"
	"testing"

	"github.com/conduitio-labs/conduit-connector-oracle/models"
)

func TestParseSource(t *testing.T) {
	tests := []struct {
		name string
		in   map[string]string
		want Source
		err  error
	}{
		{
			name: "valid config",
			in: map[string]string{
				models.ConfigURL:            "test_user/test_pass_123@localhost:1521/db_name",
				models.ConfigTable:          "test_table",
				models.ConfigOrderingColumn: "id",
				models.ConfigKeyColumn:      "id",
			},
			want: Source{
				General: General{
					URL:   "test_user/test_pass_123@localhost:1521/db_name",
					Table: "test_table",
				},
				KeyColumn:      "id",
				OrderingColumn: "id",
				BatchSize:      defaultBatchSize,
			},
		},
		{
			name: "valid config, custom batch size",
			in: map[string]string{
				models.ConfigURL:            "test_user/test_pass_123@localhost:1521/db_name",
				models.ConfigTable:          "test_table",
				models.ConfigOrderingColumn: "id",
				models.ConfigKeyColumn:      "id",
				models.ConfigBatchSize:      "100",
			},
			want: Source{
				General: General{
					URL:   "test_user/test_pass_123@localhost:1521/db_name",
					Table: "test_table",
				},
				KeyColumn:      "id",
				OrderingColumn: "id",
				BatchSize:      100,
			},
		},
		{
			name: "valid config, custom batch size",
			in: map[string]string{
				models.ConfigURL:            "test_user/test_pass_123@localhost:1521/db_name",
				models.ConfigTable:          "test_table",
				models.ConfigOrderingColumn: "id",
				models.ConfigKeyColumn:      "id",
				models.ConfigBatchSize:      "100",
			},
			want: Source{
				General: General{
					URL:   "test_user/test_pass_123@localhost:1521/db_name",
					Table: "test_table",
				},
				KeyColumn:      "id",
				OrderingColumn: "id",
				BatchSize:      100,
			},
		},
		{
			name: "valid config, custom columns",
			in: map[string]string{
				models.ConfigURL:            "test_user/test_pass_123@localhost:1521/db_name",
				models.ConfigTable:          "test_table",
				models.ConfigOrderingColumn: "id",
				models.ConfigKeyColumn:      "id",
				models.ConfigColumns:        "id,name,age",
			},
			want: Source{
				General: General{
					URL:   "test_user/test_pass_123@localhost:1521/db_name",
					Table: "test_table",
				},
				KeyColumn:      "id",
				OrderingColumn: "id",
				BatchSize:      defaultBatchSize,
				Columns:        []string{"id", "name", "age"},
			},
		},
		{
			name: "invalid config, missed ordering column",
			in: map[string]string{
				models.ConfigURL:       "test_user/test_pass_123@localhost:1521/db_name",
				models.ConfigTable:     "test_table",
				models.ConfigKeyColumn: "id",
				models.ConfigColumns:   "id,name,age",
			},
			err: errors.New(`"orderingColumn" value must be set`),
		},
		{
			name: "invalid config, missed key",
			in: map[string]string{
				models.ConfigURL:            "test_user/test_pass_123@localhost:1521/db_name",
				models.ConfigTable:          "test_table",
				models.ConfigColumns:        "id,name,age",
				models.ConfigOrderingColumn: "id",
			},
			err: errors.New(`"keyColumn" value must be set`),
		},
		{
			name: "invalid config, invalid batch size",
			in: map[string]string{
				models.ConfigURL:            "test_user/test_pass_123@localhost:1521/db_name",
				models.ConfigTable:          "test_table",
				models.ConfigOrderingColumn: "id",
				models.ConfigKeyColumn:      "id",
				models.ConfigBatchSize:      "a",
			},
			err: errors.New(`parse batchSize: strconv.Atoi: parsing "a": invalid syntax`),
		},
		{
			name: "invalid config, missed orderingColumn in columns",
			in: map[string]string{
				models.ConfigURL:            "test_user/test_pass_123@localhost:1521/db_name",
				models.ConfigTable:          "test_table",
				models.ConfigOrderingColumn: "id",
				models.ConfigKeyColumn:      "name",
				models.ConfigColumns:        "name,age",
			},
			err: errors.New(`columns must includes orderingColumn and keyColumn`),
		},
		{
			name: "invalid config, missed keyColumn in columns",
			in: map[string]string{
				models.ConfigURL:            "test_user/test_pass_123@localhost:1521/db_name",
				models.ConfigTable:          "test_table",
				models.ConfigOrderingColumn: "id",
				models.ConfigKeyColumn:      "name",
				models.ConfigColumns:        "id,age",
			},
			err: errors.New(`columns must includes orderingColumn and keyColumn`),
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
