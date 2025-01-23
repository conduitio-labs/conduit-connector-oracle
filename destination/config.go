// Copyright © 2024 Meroxa, Inc. & Yalantis
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

//go:generate paramgen -output=paramgen.go Config

package destination

import (
	"strings"

	"github.com/conduitio-labs/conduit-connector-oracle/common"
)

// Config is a destination configuration needed to connect to Oracle database.
type Config struct {
	common.Configuration

	// KeyColumn is the column name uses to detect if the target table already contains the record.
	KeyColumn string `json:"keyColumn" validate:"required"`
}

// Init sets uppercase "table" and "keyColumn" name.
func (c Config) Init() Config {
	c.Table = strings.ToUpper(c.Table)
	c.KeyColumn = strings.ToUpper(c.KeyColumn)

	return c
}

// Validate executes manual validations beyond what is defined in struct tags.
func (c Config) Validate() error {
	// handling "lte=127 " and "oracle" validations for c.Table
	if len(c.Table) > common.MaxConfigStringLength {
		return common.NewOutOfRangeError(ConfigTable)
	}
	if !common.IsOracleObjectValid(c.Table) {
		return common.NewInvalidOracleObjectError(ConfigTable)
	}

	// handling "lte=127 " and "oracle" validations for c.KeyColumn
	if len(c.KeyColumn) > common.MaxConfigStringLength {
		return common.NewOutOfRangeError(ConfigKeyColumn)
	}
	if !common.IsOracleObjectValid(c.KeyColumn) {
		return common.NewInvalidOracleObjectError(ConfigKeyColumn)
	}

	return nil
}
