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

package source

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/conduitio-labs/conduit-connector-oracle/config"
	"github.com/conduitio-labs/conduit-connector-oracle/source/mock"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/golang/mock/gomock"
	"github.com/matryer/is"
)

var (
	testURL   = "test_user/test_pass_123@localhost:1521/db_name"
	testTable = "test_table"
)

func TestSource_Configure_success(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	s := Source{}

	cfgMap := map[string]string{
		config.URL:            testURL,
		config.Table:          testTable,
		config.KeyColumns:     "id",
		config.OrderingColumn: "created_at",
		config.SnapshotTable:  "table_s",
		config.TrackingTable:  "table_t",
		config.Trigger:        "trigger",
	}
	err := s.Configure(context.Background(), cfgMap)
	is.NoErr(err)
	is.Equal(s.config, config.Source{
		Configuration: config.Configuration{
			URL:   testURL,
			Table: strings.ToUpper(testTable),
		},
		SnapshotTable:  strings.ToUpper(cfgMap[config.SnapshotTable]),
		TrackingTable:  strings.ToUpper(cfgMap[config.TrackingTable]),
		Trigger:        strings.ToUpper(cfgMap[config.Trigger]),
		OrderingColumn: strings.ToUpper(cfgMap[config.OrderingColumn]),
		KeyColumns:     []string{strings.ToUpper(cfgMap[config.KeyColumns])},
		Snapshot:       true,
		BatchSize:      1000,
	})
}

func TestSource_Configure_failure(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	s := Source{}

	err := s.Configure(context.Background(), map[string]string{
		config.URL:        testURL,
		config.Table:      testTable,
		config.KeyColumns: "id",
	})
	is.Equal(err.Error(), `"orderingColumn" value must be set`)
}

func TestSource_Read_success(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	st := make(opencdc.StructuredData)
	st["key"] = "value"

	record := opencdc.Record{
		Position: opencdc.Position(`{"last_processed_element_value": 1}`),
		Metadata: nil,
		Key:      st,
		Payload:  opencdc.Change{After: st},
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
}

func TestSource_Read_failureHasNext(t *testing.T) {
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
	is.Equal(err.Error(), "has next: get data: fail")
}

func TestSource_Read_failureNext(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	it := mock.NewMockIterator(ctrl)
	it.EXPECT().HasNext(ctx).Return(true, nil)
	it.EXPECT().Next(ctx).Return(opencdc.Record{}, errors.New("key is not exist"))

	s := Source{
		iterator: it,
	}

	_, err := s.Read(ctx)
	is.Equal(err.Error(), "next: key is not exist")
}

func TestSource_Teardown_success(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)

	it := mock.NewMockIterator(ctrl)
	it.EXPECT().Close().Return(nil)

	s := Source{
		iterator: it,
	}

	err := s.Teardown(context.Background())
	is.NoErr(err)
}

func TestSource_Teardown_failure(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)

	it := mock.NewMockIterator(ctrl)
	it.EXPECT().Close().Return(errors.New("some error"))

	s := Source{
		iterator: it,
	}

	err := s.Teardown(context.Background())
	is.Equal(err.Error(), "stops iterators and closes database connection: some error")
}
