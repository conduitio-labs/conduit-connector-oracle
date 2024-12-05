// Copyright © 2024 Meroxa, Inc. & Yalantis
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
	"fmt"
	"testing"

	"github.com/conduitio-labs/conduit-connector-oracle/common"
	"github.com/matryer/is"
)

const (
	testValueURL         = "oracle://username:password@endpoint:1521/service"
	testValueTable       = "TEST_TABLE"
	testValueOrderingCol = "ID"
	testLongString       = `this_is_a_very_long_string_which_exceeds_max_config_string_limit_
						abcdefghijklmnopqrstuvwxyz_zyxwvutsrqponmlkjihgfedcba_xxxxxxxx`
)

func TestConfig_Init(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		in       Config
		expected Config
	}{
		{
			name: "uppercase names",
			in: Config{
				Configuration: common.Configuration{
					URL:   testValueURL,
					Table: testValueTable,
				},
				OrderingColumn: testValueOrderingCol,
				KeyColumns:     []string{"name", "email"},
				Columns:        []string{"id", "name", "email"},
			},
			expected: Config{
				Configuration: common.Configuration{
					URL:   testValueURL,
					Table: testValueTable,
				},
				OrderingColumn: testValueOrderingCol,
				KeyColumns:     []string{"NAME", "EMAIL"},
				Columns:        []string{"ID", "NAME", "EMAIL"},
			},
		},
		{
			name: "lowercase names",
			in: Config{
				Configuration: common.Configuration{
					URL:   testValueURL,
					Table: "test_table",
				},
				OrderingColumn: "id",
				KeyColumns:     []string{"name", "email"},
				Columns:        []string{"id", "name", "email"},
			},
			expected: Config{
				Configuration: common.Configuration{
					URL:   testValueURL,
					Table: testValueTable,
				},
				OrderingColumn: testValueOrderingCol,
				KeyColumns:     []string{"NAME", "EMAIL"},
				Columns:        []string{"ID", "NAME", "EMAIL"},
			},
		},
		{
			name: "mixed case names",
			in: Config{
				Configuration: common.Configuration{
					URL:   testValueURL,
					Table: "Test_Table",
				},
				OrderingColumn: "Id",
				KeyColumns:     []string{"Name", "Email"},
				Columns:        []string{"Id", "Name", "Email"},
			},
			expected: Config{
				Configuration: common.Configuration{
					URL:   testValueURL,
					Table: testValueTable,
				},
				OrderingColumn: testValueOrderingCol,
				KeyColumns:     []string{"NAME", "EMAIL"},
				Columns:        []string{"ID", "NAME", "EMAIL"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			is := is.New(t)
			result := tt.in.Init()

			// Check uppercase conversions
			is.Equal(result.Table, tt.expected.Table)
			is.Equal(result.OrderingColumn, tt.expected.OrderingColumn)
			is.Equal(result.KeyColumns, tt.expected.KeyColumns)
			is.Equal(result.Columns, tt.expected.Columns)

			// Check that helper objects are set
			is.True(result.SnapshotTable != "")
			is.True(result.TrackingTable != "")
			is.True(result.Trigger != "")
		})
	}
}

func TestValidateConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      *Config
		wantErr error
	}{
		{
			name: "success_valid_config",
			in: &Config{
				Configuration: common.Configuration{
					URL:   testValueURL,
					Table: testValueTable,
				},
				OrderingColumn: testValueOrderingCol,
				KeyColumns:     []string{"NAME", "EMAIL"},
				Columns:        []string{"ID", "NAME", "EMAIL"},
				Snapshot:       true,
				BatchSize:      1000,
			},
			wantErr: nil,
		},
		{
			name: "failure_invalid_snapshot_table",
			in: &Config{
				Configuration: common.Configuration{
					URL:   testValueURL,
					Table: testValueTable,
				},
				SnapshotTable:  "Invalid-Table",
				OrderingColumn: testValueOrderingCol,
				KeyColumns:     []string{"NAME"},
				Columns:        []string{"ID", "NAME"},
			},
			wantErr: common.NewInvalidOracleObjectError(ConfigSnapshotTable),
		},
		{
			name: "failure_long_snapshot_table",
			in: &Config{
				Configuration: common.Configuration{
					URL:   testValueURL,
					Table: testValueTable,
				},
				SnapshotTable:  testLongString,
				OrderingColumn: testValueOrderingCol,
				KeyColumns:     []string{"NAME"},
				Columns:        []string{"ID", "NAME"},
			},
			wantErr: common.NewOutOfRangeError(ConfigSnapshotTable),
		},
		{
			name: "failure_ordering_column_not_in_columns",
			in: &Config{
				Configuration: common.Configuration{
					URL:   testValueURL,
					Table: testValueTable,
				},
				OrderingColumn: testValueOrderingCol,
				KeyColumns:     []string{"NAME"},
				Columns:        []string{"NAME", "EMAIL"},
			},
			wantErr: fmt.Errorf("columns must include %q", ConfigOrderingColumn),
		},
		{
			name: "failure_key_column_not_in_columns",
			in: &Config{
				Configuration: common.Configuration{
					URL:   testValueURL,
					Table: testValueTable,
				},
				OrderingColumn: "ID",
				KeyColumns:     []string{"NAME", "PHONE"},
				Columns:        []string{"ID", "NAME", "EMAIL"},
			},
			wantErr: fmt.Errorf("columns must include all %q", ConfigKeyColumns),
		},
		{
			name: "failure_empty_key_column",
			in: &Config{
				Configuration: common.Configuration{
					URL:   testValueURL,
					Table: testValueTable,
				},
				OrderingColumn: "ID",
				KeyColumns:     []string{""},
				Columns:        []string{"ID", "NAME", "EMAIL"},
			},
			wantErr: fmt.Errorf("invalid %q", ConfigKeyColumns),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			is := is.New(t)

			err := tt.in.Validate()
			if tt.wantErr == nil {
				is.NoErr(err)
			} else {
				is.True(err != nil)
				is.Equal(err.Error(), tt.wantErr.Error())
			}
		})
	}
}
