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
	"reflect"
	"testing"

	"github.com/conduitio-labs/conduit-connector-oracle/config/validator"
	"github.com/conduitio-labs/conduit-connector-oracle/models"
)

func TestParseDestination(t *testing.T) {
	tests := []struct {
		name        string
		in          map[string]string
		want        Destination
		wantErr     bool
		expectedErr string
	}{
		{
			name: "valid config",
			in: map[string]string{
				models.ConfigURL:   "test_user/test_pass_123@localhost:1521/db_name",
				models.ConfigTable: "test_table",
			},
			want: Destination{
				General: General{
					URL:   "test_user/test_pass_123@localhost:1521/db_name",
					Table: "test_table",
				},
			},
		},
		{
			name: "valid config with a key column",
			in: map[string]string{
				models.ConfigURL:       "test_user/test_pass_123@localhost:1521/db_name",
				models.ConfigTable:     "test_table",
				models.ConfigKeyColumn: "test_column",
			},
			want: Destination{
				General: General{
					URL:   "test_user/test_pass_123@localhost:1521/db_name",
					Table: "test_table",
				},
				KeyColumn: "test_column",
			},
		},
		{
			name: "key column is one symbol length",
			in: map[string]string{
				models.ConfigURL:       "test_user/test_pass_123@localhost:1521/db_name",
				models.ConfigTable:     "test_table",
				models.ConfigKeyColumn: "t",
			},
			want: Destination{
				General: General{
					URL:   "test_user/test_pass_123@localhost:1521/db_name",
					Table: "test_table",
				},
				KeyColumn: "t",
			},
		},
		{
			name: "key column is too long",
			in: map[string]string{
				models.ConfigURL:   "test_user/test_pass_123@localhost:1521/db_name",
				models.ConfigTable: "test_table",
				models.ConfigKeyColumn: "test_column_test_column_test_column_test_column_test_column_" +
					"test_column_test_column_test_column_test_column_test_column_test_colu",
			},
			wantErr:     true,
			expectedErr: validator.OutOfRangeErr(models.ConfigKeyColumn).Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseDestination(tt.in)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("parse error = \"%s\", wantErr %t", err.Error(), tt.wantErr)

					return
				}

				if err.Error() != tt.expectedErr {
					t.Errorf("expected error \"%s\", got \"%s\"", tt.expectedErr, err.Error())

					return
				}

				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parse = %v, want %v", got, tt.want)
			}
		})
	}
}
