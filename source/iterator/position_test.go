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

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func TestPosition_ParseSDKPosition(t *testing.T) {
	is := is.New(t)
	pos, err := ParseSDKPosition(nil)
	is.NoErr(err)
	is.True(pos == nil)
}

func TestPosition_ParseSDKPosition_Invalid(t *testing.T) {
	is := is.New(t)
	pos, err := ParseSDKPosition(sdk.Position("nil"))
	is.True(err != nil)
	is.True(pos == nil)
}

func TestPosition_ParseSDKPosition_BackAndForth(t *testing.T) {
	is := is.New(t)
	original := &Position{
		Mode:             ModeSnapshot,
		LastProcessedVal: float64(123),
		TrackingTable:    "abc",
		Trigger:          "def",
		SnapshotTable:    "ghi",
	}
	sdkPos, err := original.ToSDK()
	is.NoErr(err)

	pos, err := ParseSDKPosition(sdkPos)
	is.NoErr(err)
	is.Equal(original, pos)
}
