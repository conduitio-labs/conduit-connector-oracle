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

package config

import (
	"fmt"
	"strings"
)

// KeyColumn is the configuration name of the key column.
const KeyColumn = "keyColumn"

// Destination is a destination configuration needed to connect to Oracle database.
type Destination struct {
	Configuration

	// KeyColumn is the column name uses to detect if the target table already contains the record.
	KeyColumn string `validate:"required,lte=128,oracle"`
}

// ParseDestination parses a destination configuration.
func ParseDestination(cfg map[string]string) (Destination, error) {
	config, err := parseConfiguration(cfg)
	if err != nil {
		return Destination{}, fmt.Errorf("parse general config: %w", err)
	}

	destinationConfig := Destination{
		Configuration: config,
		KeyColumn:     strings.ToUpper(cfg[KeyColumn]),
	}

	err = validate(destinationConfig)
	if err != nil {
		return Destination{}, err
	}

	return destinationConfig, nil
}
