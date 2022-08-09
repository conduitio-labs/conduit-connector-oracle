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

package config

import (
	"strings"

	"github.com/conduitio-labs/conduit-connector-oracle/config/validator"
	"github.com/conduitio-labs/conduit-connector-oracle/models"
)

// A Destination represents a destination configuration needed for the destination connector.
type Destination struct {
	General
	// KeyColumn is a column name that records should use for their Key fields (source)
	// or used to detect if the target table already contains the record (destination).
	KeyColumn string `validate:"lte=128,omitempty,oracle"`
}

// ParseDestination parses destination configuration into a configuration Destination struct.
func ParseDestination(cfg map[string]string) (Destination, error) {
	config, err := parseGeneral(cfg)
	if err != nil {
		return Destination{}, err
	}

	destinationConfig := Destination{
		General:   config,
		KeyColumn: strings.ToUpper(cfg[models.ConfigKeyColumn]),
	}

	err = validator.Validate(destinationConfig)
	if err != nil {
		return Destination{}, err
	}

	return destinationConfig, nil
}
