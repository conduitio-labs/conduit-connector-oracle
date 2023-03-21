// Copyright © 2023 Meroxa, Inc.
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

package iterator

import (
	"testing"

	"github.com/conduitio-labs/conduit-connector-oracle/config"
	"github.com/matryer/is"
)

func TestParams_New_NoPosition(t *testing.T) {
	is := is.New(t)

	cfg, err := config.ParseSource(map[string]string{
		config.URL:            "url",
		config.Table:          "table",
		config.OrderingColumn: "column",

		config.SnapshotTable: "TABLE_S",
		config.TrackingTable: "TABLE_T",
		config.Trigger:       "TRIGGER",
	})
	is.NoErr(err)
	underTest := NewParams(nil, cfg)
	is.Equal(config.SnapshotTable, underTest.SnapshotTable)
	is.Equal(config.TrackingTable, underTest.TrackingTable)
	is.Equal(config.Trigger, underTest.Trigger)
}

func TestParams_New_WithPosition(t *testing.T) {
	is := is.New(t)

	pos := &Position{
		SnapshotTable: "table3",
		TrackingTable: "table1",
		Trigger:       "trigger2",
	}
	cfg, err := config.ParseSource(map[string]string{
		config.URL:            "url",
		config.Table:          "table",
		config.OrderingColumn: "column",

		config.SnapshotTable: "table_s",
		config.TrackingTable: "table_t",
		config.Trigger:       "trigger",
	})
	is.NoErr(err)
	underTest := NewParams(pos, cfg)

	is.Equal(pos.SnapshotTable, underTest.SnapshotTable)
	is.Equal(pos.TrackingTable, underTest.TrackingTable)
	is.Equal(pos.Trigger, underTest.Trigger)
}
