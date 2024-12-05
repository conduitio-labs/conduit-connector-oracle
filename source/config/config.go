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

package config

import (
	"fmt"
	"strings"

	"github.com/conduitio-labs/conduit-connector-oracle/common"
	"golang.org/x/exp/rand"
)

// Config represents a source configuration.
type Config struct {
	common.Configuration
	// SnapshotTable is the snapshot table to be used.
	SnapshotTable string `json:"snapshotTable"`
	// TrackingTable is the tracking table to be used in CDC.
	TrackingTable string `json:"trackingTable"`
	// Trigger is the trigger to be used in CDC.
	Trigger string `json:"trigger"`

	// OrderingColumn is a name of a column that the connector will use for ordering rows.
	OrderingColumn string `json:"orderingColumn" validate:"required"`
	// KeyColumns is the configuration of key column names, separated by commas.
	KeyColumns []string `json:"keyColumns"`
	// Snapshot is the configuration that determines whether the connector
	// will take a snapshot of the entire table before starting cdc mode.
	Snapshot bool `json:"snapshot" default:"true"`
	// Columns list of column names that should be included in each Record's payload.
	Columns []string `json:"columns"`
	// BatchSize is a size of rows batch.
	BatchSize int `json:"batchSize" default:"1000" validate:"gt=0,lt=100001"`
}

// Init initializes the Config with desired values.
func (c Config) Init() Config {
	c.Table = strings.ToUpper(c.Table)
	c.OrderingColumn = strings.ToUpper(c.OrderingColumn)

	if len(c.KeyColumns) != 0 {
		for i := range c.KeyColumns {
			c.KeyColumns[i] = strings.ToUpper(c.KeyColumns[i])
		}
	}

	if len(c.Columns) != 0 {
		for i := range c.Columns {
			c.Columns[i] = strings.ToUpper(c.Columns[i])
		}
	}

	c.setHelperObjects()

	return c
}

// Validate executes manual validations beyond what is defined in struct tags.
func (c Config) Validate() error {
	// Create a list of validatable fields
	fieldsToValidate := []struct {
		name  string
		value string
	}{
		{ConfigSnapshotTable, c.SnapshotTable},
		{ConfigTrackingTable, c.TrackingTable},
		{ConfigTrigger, c.Trigger},
		{ConfigOrderingColumn, c.OrderingColumn},
	}

	// Validate individual string fields
	for _, field := range fieldsToValidate {
		if err := validateStringField(field.name, field.value); err != nil {
			return err
		}
	}

	// Validate key columns
	if err := validateColumnList(ConfigKeyColumns, c.KeyColumns); err != nil {
		return err
	}

	// Validate columns
	if err := validateColumnList(ConfigColumns, c.Columns); err != nil {
		return err
	}

	if len(c.Columns) == 0 {
		return nil
	}

	columnsMap := make(map[string]struct{}, len(c.Columns))
	for _, col := range c.Columns {
		columnsMap[col] = struct{}{}
	}

	if _, ok := columnsMap[c.OrderingColumn]; !ok {
		return fmt.Errorf("columns must include %q", ConfigOrderingColumn)
	}

	for _, keyColumn := range c.KeyColumns {
		if _, ok := columnsMap[keyColumn]; !ok {
			return fmt.Errorf("columns must include all %q", ConfigKeyColumns)
		}
	}

	return nil
}

// validateStringField checks length and Oracle object validity for a single string field.
func validateStringField(fieldName, value string) error {
	if value == "" && fieldName != ConfigOrderingColumn {
		return nil
	}

	if len(value) > common.MaxConfigStringLength {
		return common.NewOutOfRangeError(fieldName)
	}

	if !common.IsOracleObjectValid(value) {
		return common.NewInvalidOracleObjectError(fieldName)
	}

	return nil
}

func validateColumnList(fieldName string, columns []string) error {
	for _, column := range columns {
		if column == "" {
			return fmt.Errorf("invalid %q", fieldName)
		}

		if err := validateStringField(fieldName, column); err != nil {
			return err
		}
	}

	return nil
}

func (c *Config) setHelperObjects() {
	c.setDefaultHelperObjects()
	if c.SnapshotTable != "" {
		c.SnapshotTable = strings.ToUpper(c.SnapshotTable)
	}
	if c.TrackingTable != "" {
		c.TrackingTable = strings.ToUpper(c.TrackingTable)
	}
	if c.Trigger != "" {
		c.Trigger = strings.ToUpper(c.Trigger)
	}
}

func (c *Config) setDefaultHelperObjects() {
	// There's a limit on the length of names in Oracle.
	// Depending on the version, it might be between 30 and 128 bytes.
	// We're going with the safer (lower) limit here.
	id := rand.Int31()
	if id < 0 {
		id = -id
	}

	if c.SnapshotTable == "" {
		c.SnapshotTable = fmt.Sprintf("CONDUIT_SNAPSHOT_%d", id)
	}
	if c.TrackingTable == "" {
		c.TrackingTable = fmt.Sprintf("CONDUIT_TRACKING_%d", id)
	}
	if c.Trigger == "" {
		c.Trigger = fmt.Sprintf("CONDUIT_TRIGGER_%d", id)
	}
}
