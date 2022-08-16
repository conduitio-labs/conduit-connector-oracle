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

package source

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-oracle/config/validator"
	"github.com/conduitio-labs/conduit-connector-oracle/models"
	"github.com/conduitio-labs/conduit-connector-oracle/source/mock"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/golang/mock/gomock"
	"github.com/matryer/is"
)

func TestSource_Configure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   map[string]string
		err  error
	}{
		{
			name: "success case",
			in: map[string]string{
				models.ConfigURL:            "test_user/test_pass_123@localhost:1521/db_name",
				models.ConfigTable:          "test_table",
				models.ConfigKeyColumn:      "id",
				models.ConfigOrderingColumn: "created_at",
			},
		},
		{
			name: "failure case (ordering column is not exist)",
			in: map[string]string{
				models.ConfigURL:       "test_user/test_pass_123@localhost:1521/db_name",
				models.ConfigTable:     "test_table",
				models.ConfigKeyColumn: "id",
			},
			err: validator.RequiredErr(models.ConfigOrderingColumn),
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := new(Source)

			err := s.Configure(context.Background(), tt.in)
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

func TestSource_Read(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		is := is.New(t)

		ctrl := gomock.NewController(t)
		ctx := context.Background()

		st := make(sdk.StructuredData)
		st["key"] = "value"

		record := sdk.Record{
			Position:  sdk.Position(`{"last_processed_element_value": 1}`),
			Metadata:  nil,
			CreatedAt: time.Time{},
			Key:       st,
			Payload:   st,
		}

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().HasNext(ctx).Return(true, nil)
		it.EXPECT().Next(ctx).Return(record, nil)

		s := Source{
			iterator: it,
		}

		r, err := s.Read(ctx)
		is.NoErr(err)

		is.Equal(r, record)
	})

	t.Run("failure has next", func(t *testing.T) {
		t.Parallel()

		is := is.New(t)

		ctrl := gomock.NewController(t)
		ctx := context.Background()

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().HasNext(ctx).Return(true, errors.New("get data: fail"))

		s := Source{
			iterator: it,
		}

		_, err := s.Read(ctx)
		is.Equal(err != nil, true)
	})

	t.Run("failure next", func(t *testing.T) {
		t.Parallel()

		is := is.New(t)

		ctrl := gomock.NewController(t)
		ctx := context.Background()

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().HasNext(ctx).Return(true, nil)
		it.EXPECT().Next(ctx).Return(sdk.Record{}, errors.New("key is not exist"))

		s := Source{
			iterator: it,
		}

		_, err := s.Read(ctx)
		is.Equal(err != nil, true)
	})
}

func TestSource_Teardown(t *testing.T) {
	t.Run("success teadown", func(t *testing.T) {
		t.Parallel()

		is := is.New(t)

		ctrl := gomock.NewController(t)
		ctx := context.Background()

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().Stop().Return(nil)

		s := Source{
			iterator: it,
		}

		err := s.Teardown(ctx)
		is.NoErr(err)
	})

	t.Run("failure teardown", func(t *testing.T) {
		t.Parallel()

		is := is.New(t)

		ctrl := gomock.NewController(t)
		ctx := context.Background()

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().Stop().Return(errors.New("some error"))

		s := Source{
			iterator: it,
		}

		err := s.Teardown(ctx)
		is.Equal(err != nil, true)
	})
}
