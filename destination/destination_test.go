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

	"github.com/conduitio-labs/conduit-connector-oracle/common"
	"github.com/conduitio-labs/conduit-connector-oracle/destination/mock"
	"github.com/conduitio-labs/conduit-connector-oracle/destination/writer"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
	"go.uber.org/mock/gomock"
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
		ConfigUrl:       testURL,
		ConfigTable:     testTable,
		ConfigKeyColumn: "id",
	})
	is.NoErr(err)
	is.Equal(d.cfg, Config{
		Configuration: common.Configuration{
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
		ConfigUrl:   testURL,
		ConfigTable: testTable,
	})

	is.Equal(err.Error(), `config invalid: error validating "keyColumn": required parameter is not provided`)
}

func TestDestination_Write_success(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	metadata := opencdc.Metadata{}
	metadata.SetCreatedAt(time.Now())

	records := make([]opencdc.Record, 2)
	records[0] = opencdc.Record{
		Operation: opencdc.OperationSnapshot,
		Metadata:  metadata,
		Key: opencdc.StructuredData{
			"id": 1,
		},
		Payload: opencdc.Change{
			After: opencdc.StructuredData{
				"id":   1,
				"name": "John",
			},
		},
	}
	records[1] = opencdc.Record{
		Position:  opencdc.Position("snapshot.1"),
		Operation: opencdc.OperationSnapshot,
		Metadata:  metadata,
		Key: opencdc.StructuredData{
			"id": 2,
		},
		Payload: opencdc.Change{
			After: opencdc.StructuredData{
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

	record := opencdc.Record{
		Key: opencdc.StructuredData{
			"id": 1,
		},
	}

	w := mock.NewMockWriter(ctrl)
	w.EXPECT().Write(ctx, record).Return(writer.ErrEmptyPayload)

	d := Destination{
		writer: w,
	}

	n, err := d.Write(ctx, []opencdc.Record{record})
	is.Equal(err, writer.ErrEmptyPayload)
	is.Equal(n, 0)
}

func TestDestination_Teardown_success(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)

	d := Destination{
		writer: mock.NewMockWriter(ctrl),
	}

	err := d.Teardown(context.Background())
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
