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
	"testing"
	"time"

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
		err  error
	}{
		{
			name: "success case",
			in: map[string]string{
				models.ConfigURL:   "test_user/test_pass_123@localhost:1521/db_name",
				models.ConfigTable: "test_table",
			},
		},
		{
			name: "failure case (key column is too long)",
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

			d := new(Destination)

			err := d.Configure(context.Background(), tt.in)
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

		metadata := sdk.Metadata{}
		metadata.SetCreatedAt(time.Now())

		records := make([]sdk.Record, 2)
		records[0] = sdk.Record{
			Position: sdk.Position("1.0"),
			Metadata: metadata,
			Key: sdk.StructuredData{
				"id": 1,
			},
			Payload: sdk.Change{
				After: sdk.StructuredData{
					"id":   1,
					"name": "John",
				},
			},
		}
		records[1] = sdk.Record{
			Position: sdk.Position("1.0"),
			Metadata: metadata,
			Key: sdk.StructuredData{
				"id": 2,
			},
			Payload: sdk.Change{
				After: sdk.StructuredData{
					"id":   2,
					"name": "Sam",
				},
			},
		}

		w := mock.NewMockWriter(ctrl)
		for i := range records {
			w.EXPECT().Write(ctx, records[i]).Return(nil)
		}

		d := Destination{
			writer: w,
		}

		n, err := d.Write(ctx, records)
		is.NoErr(err)
		is.Equal(n, len(records))
	})

	t.Run("failure, empty payload", func(t *testing.T) {
		t.Parallel()

		is := is.New(t)

		ctrl := gomock.NewController(t)
		ctx := context.Background()

		record := sdk.Record{
			Position: sdk.Position("1.0"),
			Key: sdk.StructuredData{
				"id": 1,
			},
		}

		w := mock.NewMockWriter(ctrl)
		w.EXPECT().Write(ctx, record).Return(writer.ErrEmptyPayload)

		d := Destination{
			writer: w,
		}

		n, err := d.Write(ctx, []sdk.Record{record})
		is.Equal(err != nil, true)
		is.Equal(err, writer.ErrEmptyPayload)
		is.Equal(n, 0)
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
