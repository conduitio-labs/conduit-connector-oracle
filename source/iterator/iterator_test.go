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
	"strings"
	"testing"

	"github.com/conduitio-labs/conduit-connector-oracle/config"
	"github.com/matryer/is"
)

func TestParams_HelperTables_NoPosition(t *testing.T) {
	is := is.New(t)

	underTest := NewParams(nil, config.Source{TrackingPrefix: config.DefaultTrackingPrefix})

	checkHelperObject(is, underTest.SnapshotTable, "CONDUIT_SNAPSHOT_")
	checkHelperObject(is, underTest.TrackingTable, "CONDUIT_TRACKING_")
	checkHelperObject(is, underTest.Trigger, "CONDUIT_")
}

func TestParams_HelperObject_WithPosition(t *testing.T) {
	is := is.New(t)

	pos := &Position{
		SnapshotTable: "table3",
		TrackingTable: "table1",
		Trigger:       "trigger2",
	}
	underTest := NewParams(pos, config.Source{})

	is.Equal(pos.SnapshotTable, underTest.SnapshotTable)
	is.Equal(pos.TrackingTable, underTest.TrackingTable)
	is.Equal(pos.Trigger, underTest.Trigger)
}

func checkHelperObject(is *is.I, s string, prefix string) {
	is.True(strings.HasPrefix(s, prefix))
	is.True(len(s) < 30)
}
