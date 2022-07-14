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

package oracle

import (
	"github.com/conduitio/conduit-connector-oracle/models"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Specification returns specification of the connector.
func Specification() sdk.Specification {
	return sdk.Specification{
		Name:    "oracle",
		Summary: "Oracle source and destination plugin for Conduit, written in Go.",
		Description: "Oracle connector is one of Conduit plugins. " +
			"It provides a source and a destination Oracle connector.",
		Version:      "v0.1.0",
		Author:       "Meroxa, Inc.",
		SourceParams: map[string]sdk.Parameter{},
		DestinationParams: map[string]sdk.Parameter{
			models.ConfigURL: {
				Default:     "",
				Required:    true,
				Description: "The connection string to connect to Oracle database.",
			},
			models.ConfigTable: {
				Default:     "",
				Required:    true,
				Description: "The table name of the table in Oracle that the connector should write to, by default.",
			},
			models.ConfigKeyColumn: {
				Default:     "",
				Required:    false,
				Description: "A column name that used to detect if the target table already contains the record.",
			}},
	}
}
