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

package iterator

import (
	"encoding/json"
	"fmt"

	"github.com/conduitio/conduit-commons/opencdc"
)

// Mode defines an iterator mode.
type Mode string

const (
	ModeSnapshot = "snapshot"
	ModeCDC      = "cdc"
)

// Position represents Oracle position.
type Position struct {
	// Mode represents current iterator mode.
	Mode Mode `json:"mode"`

	// LastProcessedVal represents the last processed value from ordering column.
	LastProcessedVal any    `json:"last_processed_val"`
	TrackingTable    string `json:"tracking_table"`
	Trigger          string `json:"trigger"`
	SnapshotTable    string `json:"snapshot_table"`
}

// ParseSDKPosition parses opencdc.Position and returns Position.
func ParseSDKPosition(position opencdc.Position) (*Position, error) {
	var pos Position

	if position == nil {
		return nil, nil
	}

	if err := json.Unmarshal(position, &pos); err != nil {
		return nil, fmt.Errorf("unmarshal opencdc.Position into Position: %w", err)
	}

	return &pos, nil
}

// ToSDK marshals Position and returns opencdc.Position or an error.
func (p Position) ToSDK() (opencdc.Position, error) {
	bytes, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("marshal position: %w", err)
	}

	return bytes, nil
}
