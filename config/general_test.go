// Copyright © 2022 Meroxa, Inc. & Yalantis
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

	"go.uber.org/multierr"
)

func TestParseGeneral(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   map[string]string
		want General
		err  error
	}{
		{
			name: "valid config",
			in: map[string]string{
				URL:       "test_user/test_pass_123@localhost:1521/db_name",
				Table:     "test_table",
				KeyColumn: "id",
			},
			want: General{
				URL:       "test_user/test_pass_123@localhost:1521/db_name",
				Table:     "TEST_TABLE",
				KeyColumn: "ID",
			},
		},
		{
			name: "URL is required",
			in: map[string]string{
				Table:     "test_table",
				KeyColumn: "id",
			},
			err: errRequired(URL),
		},
		{
			name: "table is required",
			in: map[string]string{
				URL:       "test_user/test_pass_123@localhost:1521/db_name",
				KeyColumn: "id",
			},
			err: errRequired(Table),
		},
		{
			name: "keyColumn is required",
			in: map[string]string{
				URL:   "test_user/test_pass_123@localhost:1521/db_name",
				Table: "test_table",
			},
			err: errRequired(KeyColumn),
		},
		{
			name: "a couple required fields are empty (a password and an URL)",
			in: map[string]string{
				KeyColumn: "id",
			},
			err: multierr.Combine(errRequired(URL), errRequired(Table)),
		},
		{
			name: "table begins with a number",
			in: map[string]string{
				URL:       "test_user/test_pass_123@localhost:1521/db_name",
				Table:     "1_test_table",
				KeyColumn: "id",
			},
			err: errInvalidOracleObject(Table),
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := Parse(tt.in)
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
