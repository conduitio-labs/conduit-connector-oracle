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

	"github.com/conduitio/conduit-connector-oracle/config/validator"
	"github.com/conduitio/conduit-connector-oracle/models"
	"go.uber.org/multierr"
)

func TestParseGeneral(t *testing.T) {
	tests := []struct {
		name        string
		in          map[string]string
		want        General
		wantErr     bool
		expectedErr string
	}{
		{
			name: "valid config",
			in: map[string]string{
				models.ConfigURL:   "test_user/test_pass_123@localhost:1521/db_name",
				models.ConfigTable: "test_table",
			},
			want: General{
				URL:   "test_user/test_pass_123@localhost:1521/db_name",
				Table: "test_table",
			},
		},
		{
			name: "url is required",
			in: map[string]string{
				models.ConfigTable: "test_table",
			},
			wantErr:     true,
			expectedErr: validator.RequiredErr(models.ConfigURL).Error(),
		},
		{
			name:    "a couple required fields are empty (a password and an url)",
			in:      map[string]string{},
			wantErr: true,
			expectedErr: multierr.Combine(validator.RequiredErr(models.ConfigURL),
				validator.RequiredErr(models.ConfigTable)).Error(),
		},
		{
			name: "table begins with a number",
			in: map[string]string{
				models.ConfigURL:   "test_user/test_pass_123@localhost:1521/db_name",
				models.ConfigTable: "1_test_table",
			},
			wantErr:     true,
			expectedErr: validator.InvalidOracleObjectErr(models.ConfigTable).Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseGeneral(tt.in)
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
