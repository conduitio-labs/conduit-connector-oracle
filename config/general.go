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
	"github.com/conduitio/conduit-connector-oracle/config/validator"
	"github.com/conduitio/conduit-connector-oracle/models"
)

// A General represents a general configuration needed to connect to Oracle database.
type General struct {
	// Username is the configuration of the username to connect to Oracle database.
	Username string `json:"username" validate:"required,lte=128,oracle"`

	// Password is the configuration of the password to connect to Oracle database.
	Password string `json:"password" validate:"required,lte=30,oracle"`

	// URL is the configuration of the path to the Oracle database.
	URL string `json:"url" validate:"required"`

	// Table is the configuration of the table.
	Table string `json:"table" validate:"required,lte=128,oracle"`
}

func parseGeneral(cfg map[string]string) (General, error) {
	config := General{
		Username: cfg[models.ConfigUsername],
		Password: cfg[models.ConfigPassword],
		URL:      cfg[models.ConfigURL],
		Table:    cfg[models.ConfigTable],
	}

	err := validator.Validate(config)
	if err != nil {
		return General{}, err
	}

	return config, nil
}
