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
)

// A Configuration represents a general configuration needed to connect to Oracle database.
type Configuration struct {
	// URL is the connection string to connect to Oracle database.
	URL string `json:"url" validate:"required"`
	// Table is table name of the table in Oracle that the connector should write to.
	Table string `json:"table" validate:"required,lte=128,oracle"`
}

// parses general configuration.
func parseConfiguration(cfg map[string]string) (Configuration, error) {
	config := Configuration{
		URL:   cfg[URL],
		Table: strings.ToUpper(cfg[Table]),
	}

	err := validate(config)
	if err != nil {
		return Configuration{}, err
	}

	return config, nil
}
