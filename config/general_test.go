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
	"go.uber.org/multierr"
)

func TestParseGeneral(t *testing.T) {
	tests := []struct {
		name string
		in   map[string]string
		want General
		err  error
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
			err: validator.RequiredErr(models.ConfigURL),
		},
		{
			name: "a couple required fields are empty (a password and an url)",
			in:   map[string]string{},
			err:  multierr.Combine(validator.RequiredErr(models.ConfigURL), validator.RequiredErr(models.ConfigTable)),
		},
		{
			name: "table begins with a number",
			in: map[string]string{
				models.ConfigURL:   "test_user/test_pass_123@localhost:1521/db_name",
				models.ConfigTable: "1_test_table",
			},
			err: validator.InvalidOracleObjectErr(models.ConfigTable),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseGeneral(tt.in)
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
