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
				models.ConfigUsername: "test_user",
				models.ConfigPassword: "test_pass_123",
				models.ConfigURL:      "localhost:1521/ORCLCDB",
				models.ConfigTable:    "test_table",
			},
			want: General{
				Username: "test_user",
				Password: "test_pass_123",
				URL:      "localhost:1521/ORCLCDB",
				Table:    "test_table",
			},
		},
		{
			name: "valid config with the shorter username",
			in: map[string]string{
				models.ConfigUsername: "t",
				models.ConfigPassword: "test_pass_123",
				models.ConfigURL:      "localhost:1521/ORCLCDB",
				models.ConfigTable:    "test_table",
			},
			want: General{
				Username: "t",
				Password: "test_pass_123",
				URL:      "localhost:1521/ORCLCDB",
				Table:    "test_table",
			},
		},
		{
			name: "username is required",
			in: map[string]string{
				models.ConfigPassword: "test_pass_123",
				models.ConfigURL:      "localhost:1521/ORCLCDB",
				models.ConfigTable:    "test_table",
			},
			wantErr:     true,
			expectedErr: validator.RequiredErr(models.ConfigUsername).Error(),
		},
		{
			name: "password is required",
			in: map[string]string{
				models.ConfigUsername: "test_user",
				models.ConfigURL:      "localhost:1521/ORCLCDB",
				models.ConfigTable:    "test_table",
			},
			wantErr:     true,
			expectedErr: validator.RequiredErr(models.ConfigPassword).Error(),
		},
		{
			name: "url is required",
			in: map[string]string{
				models.ConfigUsername: "test_user",
				models.ConfigPassword: "test_pass_123",
				models.ConfigTable:    "test_table",
			},
			wantErr:     true,
			expectedErr: validator.RequiredErr(models.ConfigURL).Error(),
		},
		{
			name: "table is required",
			in: map[string]string{
				models.ConfigUsername: "test_user",
				models.ConfigPassword: "test_pass_123",
				models.ConfigURL:      "localhost:1521/ORCLCDB",
			},
			wantErr:     true,
			expectedErr: validator.RequiredErr(models.ConfigTable).Error(),
		},
		{
			name: "a couple required fields are empty (a password and an url)",
			in: map[string]string{
				models.ConfigUsername: "test_user",
				models.ConfigTable:    "test_table",
			},
			wantErr: true,
			expectedErr: multierr.Combine(validator.RequiredErr(models.ConfigPassword),
				validator.RequiredErr(models.ConfigURL)).Error(),
		},
		{
			name: "username is too long",
			in: map[string]string{
				models.ConfigUsername: "test_user_test_user_test_user_test_user_test_user_test_user_test_" +
					"user_test_user_test_user_test_user_test_user_test_user_test_user",
				models.ConfigPassword: "test_pass_123",
				models.ConfigURL:      "localhost:1521/ORCLCDB",
				models.ConfigTable:    "test_table",
			},
			wantErr:     true,
			expectedErr: validator.OutOfRangeErr(models.ConfigUsername).Error(),
		},
		{
			name: "password is too long",
			in: map[string]string{
				models.ConfigUsername: "test_user",
				models.ConfigPassword: "test_pass_123_test_pass_123_tes",
				models.ConfigURL:      "localhost:1521/ORCLCDB",
				models.ConfigTable:    "test_table",
			},
			wantErr:     true,
			expectedErr: validator.OutOfRangeErr(models.ConfigPassword).Error(),
		},
		{
			name: "username is too long",
			in: map[string]string{
				models.ConfigUsername: "test_user",
				models.ConfigPassword: "test_pass_123",
				models.ConfigURL:      "localhost:1521/ORCLCDB",
				models.ConfigTable: "test_table_test_table_test_table_test_table_test_table_test_table_" +
					"test_table_test_table_test_table_test_table_test_table_test_tab",
			},
			wantErr:     true,
			expectedErr: validator.OutOfRangeErr(models.ConfigTable).Error(),
		},
		{
			name: "username contains unsupported characters",
			in: map[string]string{
				models.ConfigUsername: "test+user",
				models.ConfigPassword: "test_pass_123",
				models.ConfigURL:      "localhost:1521/ORCLCDB",
				models.ConfigTable:    "test_table",
			},
			wantErr:     true,
			expectedErr: validator.InvalidOracleObjectErr(models.ConfigUsername).Error(),
		},
		{
			name: "username begins with a number",
			in: map[string]string{
				models.ConfigUsername: "1_test_user",
				models.ConfigPassword: "test_pass_123",
				models.ConfigURL:      "localhost:1521/ORCLCDB",
				models.ConfigTable:    "test_table",
			},
			wantErr:     true,
			expectedErr: validator.InvalidOracleObjectErr(models.ConfigUsername).Error(),
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
