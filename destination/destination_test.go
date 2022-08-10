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

package destination

import (
	"context"
	"reflect"
	"testing"

	"github.com/conduitio-labs/conduit-connector-oracle/config"
	"github.com/conduitio-labs/conduit-connector-oracle/config/validator"
	"github.com/conduitio-labs/conduit-connector-oracle/destination/mock"
	"github.com/conduitio-labs/conduit-connector-oracle/destination/writer"
	"github.com/conduitio-labs/conduit-connector-oracle/models"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/golang/mock/gomock"
	"github.com/matryer/is"
)

func TestDestination_Configure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   map[string]string
		want config.Destination
		err  error
	}{
		{
			name: "valid config",
			in: map[string]string{
				models.ConfigURL:   "test_user/test_pass_123@localhost:1521/db_name",
				models.ConfigTable: "test_table",
			},
			want: config.Destination{
				General: config.General{
					URL:   "test_user/test_pass_123@localhost:1521/db_name",
					Table: "TEST_TABLE",
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
			want: config.Destination{
				General: config.General{
					URL:   "test_user/test_pass_123@localhost:1521/db_name",
					Table: "TEST_TABLE",
				},
				KeyColumn: "TEST_COLUMN",
			},
		},
		{
			name: "key column is one symbol length",
			in: map[string]string{
				models.ConfigURL:       "test_user/test_pass_123@localhost:1521/db_name",
				models.ConfigTable:     "test_table",
				models.ConfigKeyColumn: "t",
			},
			want: config.Destination{
				General: config.General{
					URL:   "test_user/test_pass_123@localhost:1521/db_name",
					Table: "TEST_TABLE",
				},
				KeyColumn: "T",
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
			err: validator.OutOfRangeErr(models.ConfigKeyColumn),
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := config.ParseDestination(tt.in)
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

func TestDestination_Write(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		is := is.New(t)

		ctrl := gomock.NewController(t)
		ctx := context.Background()

		record := sdk.Record{
			Position: sdk.Position("1.0"),
			Metadata: map[string]string{
				"action": "insert",
			},
			Key: sdk.StructuredData{
				"id": 1,
			},
			Payload: sdk.StructuredData{
				"id":   1,
				"name": "Void",
			},
		}

		w := mock.NewMockWriter(ctrl)
		w.EXPECT().Write(ctx, record).Return(nil)

		d := Destination{
			writer: w,
		}

		err := d.Write(ctx, record)
		is.NoErr(err)
	})

	t.Run("fail, empty payload", func(t *testing.T) {
		t.Parallel()

		is := is.New(t)

		ctrl := gomock.NewController(t)
		ctx := context.Background()

		record := sdk.Record{
			Position: sdk.Position("1.0"),
			Metadata: map[string]string{
				"action": "insert",
			},
			Key: sdk.StructuredData{
				"id": 1,
			},
		}

		w := mock.NewMockWriter(ctrl)
		w.EXPECT().Write(ctx, record).Return(writer.ErrEmptyPayload)

		d := Destination{
			writer: w,
		}

		err := d.Write(ctx, record)
		is.Equal(err != nil, true)
		is.Equal(err, writer.ErrEmptyPayload)
	})
}

func TestDestination_Teardown(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		is := is.New(t)

		ctrl := gomock.NewController(t)
		ctx := context.Background()

		d := Destination{
			writer: mock.NewMockWriter(ctrl),
		}

		err := d.Teardown(ctx)
		is.NoErr(err)
	})

	t.Run("success, writer is nil", func(t *testing.T) {
		t.Parallel()

		is := is.New(t)

		ctx := context.Background()

		d := Destination{
			writer: nil,
		}

		err := d.Teardown(ctx)
		is.NoErr(err)
	})
}
