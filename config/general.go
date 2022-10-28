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
	"strings"
)

const (
	// URL is the configuration name of the url.
	URL = "url"
	// Table is the configuration name of the table.
	Table = "table"
	// KeyColumn is the configuration name of the key column.
	KeyColumn = "keyColumn"
)

// A General represents a general configuration needed to connect to Oracle database.
type General struct {
	// URL is the configuration of the connection string to connect to Oracle database.
	URL string `json:"url" validate:"required"`
	// Table is the configuration of the table name.
	Table string `json:"table" validate:"required,lte=128,oracle"`
	// KeyColumn is a column name that records should use for their `Key` fields (source) or
	// is a column name uses to detect if the target table already contains the record (destination).
	KeyColumn string `validate:"required,lte=128,oracle"`
}

// Parse parses general configuration.
func Parse(cfg map[string]string) (General, error) {
	config := General{
		URL:       cfg[URL],
		Table:     strings.ToUpper(cfg[Table]),
		KeyColumn: strings.ToUpper(cfg[KeyColumn]),
	}

	err := validate(config)
	if err != nil {
		return General{}, err
	}

	return config, nil
}
