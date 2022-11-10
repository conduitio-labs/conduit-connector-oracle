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

package destination

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-oracle/config"
	"github.com/conduitio-labs/conduit-connector-oracle/destination/mock"
	"github.com/conduitio-labs/conduit-connector-oracle/destination/writer"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/golang/mock/gomock"
	"github.com/matryer/is"
)

var (
	testURL   = "test_user/test_pass_123@localhost:1521/db_name"
	testTable = "test_table"
)

func TestDestination_Configure_success(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	d := Destination{}

	err := d.Configure(context.Background(), map[string]string{
		config.URL:       testURL,
		config.Table:     testTable,
		config.KeyColumn: "id",
	})
	is.NoErr(err)
	is.Equal(d.cfg, config.Destination{
		Configuration: config.Configuration{
			URL:   testURL,
			Table: strings.ToUpper(testTable),
		},
		KeyColumn: strings.ToUpper("id"),
	})
}

func TestDestination_Configure_failure(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	d := Destination{}

	err := d.Configure(context.Background(), map[string]string{
		config.URL: testURL,
	})
	is.Equal(err.Error(), `parse general config: "table" value must be set`)
}

func TestDestination_Write_success(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	metadata := sdk.Metadata{}
	metadata.SetCreatedAt(time.Now())

	records := make([]sdk.Record, 2)
	records[0] = sdk.Record{
		Operation: sdk.OperationSnapshot,
		Metadata:  metadata,
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
		Position:  sdk.Position("snapshot.1"),
		Operation: sdk.OperationSnapshot,
		Metadata:  metadata,
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
}

func TestDestination_Write_failure(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	record := sdk.Record{
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
	is.Equal(err, writer.ErrEmptyPayload)
	is.Equal(n, 0)
}

func TestDestination_Teardown_success(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	d := Destination{
		writer: mock.NewMockWriter(ctrl),
	}

	err := d.Teardown(ctx)
	is.NoErr(err)
}

func TestDestination_Teardown_successNilWriter(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	d := Destination{
		writer: nil,
	}

	err := d.Teardown(context.Background())
	is.NoErr(err)
}
