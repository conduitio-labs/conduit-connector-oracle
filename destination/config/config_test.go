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
	"testing"

	"github.com/conduitio-labs/conduit-connector-oracle/common"
	"github.com/matryer/is"
)

const (
	testValueURL    = "oracle://username:password@endpoint:1521/service"
	testValueTable  = "TEST_TABLE"
	testValueKeyCol = "ID"
	testLongString  = `this_is_a_very_long_string_which_exceeds_max_config_string_limit_
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
			name: "table and key column already uppercase",
			in: Config{
				Configuration: common.Configuration{
					URL:   testValueURL,
					Table: testValueTable,
				},
				KeyColumn: testValueKeyCol,
			},
			expected: Config{
				Configuration: common.Configuration{
					URL:   testValueURL,
					Table: testValueTable,
				},
				KeyColumn: testValueKeyCol,
			},
		},
		{
			name: "table and key column in lowercase",
			in: Config{
				Configuration: common.Configuration{
					URL:   testValueURL,
					Table: "test_table",
				},
				KeyColumn: "id",
			},
			expected: Config{
				Configuration: common.Configuration{
					URL:   testValueURL,
					Table: testValueTable,
				},
				KeyColumn: testValueKeyCol,
			},
		},
		{
			name: "table and key column mixed case",
			in: Config{
				Configuration: common.Configuration{
					URL:   testValueURL,
					Table: "Test_Table",
				},
				KeyColumn: "Id",
			},
			expected: Config{
				Configuration: common.Configuration{
					URL:   testValueURL,
					Table: testValueTable,
				},
				KeyColumn: testValueKeyCol,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			is := is.New(t)
			result := tt.in.Init()
			is.Equal(result.Table, tt.expected.Table)
			is.Equal(result.KeyColumn, tt.expected.KeyColumn)
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
				KeyColumn: testValueKeyCol,
			},
			wantErr: nil,
		},
		{
			name: "failure_table_exceeds_max_length",
			in: &Config{
				Configuration: common.Configuration{
					URL:   testValueURL,
					Table: testLongString,
				},
				KeyColumn: testValueKeyCol,
			},
			wantErr: common.NewOutOfRangeError(ConfigTable),
		},
		{
			name: "failure_key_column_exceeds_max_length",
			in: &Config{
				Configuration: common.Configuration{
					URL:   testValueURL,
					Table: testValueTable,
				},
				KeyColumn: testLongString,
			},
			wantErr: common.NewOutOfRangeError(ConfigKeyColumn),
		},
		{
			name: "failure_table_invalid_oracle_object",
			in: &Config{
				Configuration: common.Configuration{
					URL:   testValueURL,
					Table: "Invalid-Table",
				},
				KeyColumn: testValueKeyCol,
			},
			wantErr: common.NewInvalidOracleObjectError(ConfigTable),
		},
		{
			name: "failure_key_column_invalid_oracle_object",
			in: &Config{
				Configuration: common.Configuration{
					URL:   testValueURL,
					Table: testValueTable,
				},
				KeyColumn: "Invalid-Column",
			},
			wantErr: common.NewInvalidOracleObjectError(ConfigKeyColumn),
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
