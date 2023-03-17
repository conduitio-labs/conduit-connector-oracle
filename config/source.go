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
	"strconv"
	"strings"
)

const (
	TrackingPrefix = "trackingPrefix"
	// OrderingColumn is a config name for an ordering column.
	OrderingColumn = "orderingColumn"
	// KeyColumns is the configuration name of the names of the columns to build the record.Key, separated by commas.
	KeyColumns = "keyColumns"
	// Snapshot is the configuration name for the Snapshot field.
	Snapshot = "snapshot"
	// Columns is a config name for columns.
	Columns = "columns"
	// BatchSize is a config name for a batch size.
	BatchSize = "batchSize"

	DefaultTrackingPrefix = "CONDUIT"
	// DefaultBatchSize is the default value of the BatchSize field.
	DefaultBatchSize = 1000
	// DefaultSnapshot is a default value for the Snapshot field.
	DefaultSnapshot = true
)

// A Source represents a source configuration.
type Source struct {
	Configuration
	// TrackingPrefix is the prefix added to the snapshot table, the tracking table
	// and the trigger. Default value is "CONDUIT_"
	// Oracle names must not be longer than 30-128, depending on version.
	TrackingPrefix string `validate:"lte=128,oracle"`
	// OrderingColumn is a name of a column that the connector will use for ordering rows.
	OrderingColumn string `validate:"required,lte=128,oracle"`
	// KeyColumns is the configuration of key column names, separated by commas.
	KeyColumns []string `validate:"omitempty,dive,lte=128,oracle"`
	// Snapshot is the configuration that determines whether the connector
	// will take a snapshot of the entire table before starting cdc mode.
	Snapshot bool
	// Columns list of column names that should be included in each Record's payload.
	Columns []string `validate:"dive,lte=128,oracle"`
	// BatchSize is a size of rows batch.
	BatchSize int `validate:"gte=1,lte=100000"`
}

// ParseSource parses source configuration.
func ParseSource(cfgMap map[string]string) (Source, error) {
	config, err := parseConfiguration(cfgMap)
	if err != nil {
		return Source{}, err
	}

	cfg := Source{
		Configuration:  config,
		TrackingPrefix: DefaultTrackingPrefix,
		OrderingColumn: strings.ToUpper(cfgMap[OrderingColumn]),
		Snapshot:       DefaultSnapshot,
		BatchSize:      DefaultBatchSize,
	}

	if cfgMap[KeyColumns] != "" {
		keyColumns := strings.Split(strings.ReplaceAll(cfgMap[KeyColumns], " ", ""), ",")
		for i := range keyColumns {
			if keyColumns[i] == "" {
				return Source{}, fmt.Errorf("invalid %q", KeyColumns)
			}

			cfg.KeyColumns = append(cfg.KeyColumns, strings.ToUpper(keyColumns[i]))
		}
	}

	if cfgMap[Snapshot] != "" {
		snapshot, err := strconv.ParseBool(cfgMap[Snapshot])
		if err != nil {
			return Source{}, fmt.Errorf("parse %q: %w", Snapshot, err)
		}

		cfg.Snapshot = snapshot
	}

	if cfgMap[Columns] != "" {
		columnsSl := strings.Split(strings.ReplaceAll(cfgMap[Columns], " ", ""), ",")
		for i := range columnsSl {
			if columnsSl[i] == "" {
				return Source{}, fmt.Errorf("invalid %q", Columns)
			}

			cfg.Columns = append(cfg.Columns, strings.ToUpper(columnsSl[i]))
		}
	}

	if cfgMap[BatchSize] != "" {
		cfg.BatchSize, err = strconv.Atoi(cfgMap[BatchSize])
		if err != nil {
			return Source{}, fmt.Errorf("parse BatchSize: %w", err)
		}
	}

	if cfgMap[TrackingPrefix] != "" {
		cfg.TrackingPrefix = strings.ToUpper(cfgMap[TrackingPrefix])
	}

	err = validate(cfg)
	if err != nil {
		return Source{}, err
	}

	if len(cfg.Columns) == 0 {
		return cfg, nil
	}

	columnsMap := make(map[string]struct{}, len(cfg.Columns))
	for i := 0; i < len(cfg.Columns); i++ {
		columnsMap[cfg.Columns[i]] = struct{}{}
	}

	if _, ok := columnsMap[cfg.OrderingColumn]; !ok {
		return Source{}, fmt.Errorf("columns must include %q", OrderingColumn)
	}

	for i := range cfg.KeyColumns {
		if _, ok := columnsMap[cfg.KeyColumns[i]]; !ok {
			return Source{}, fmt.Errorf("columns must include all %q", KeyColumns)
		}
	}

	return cfg, nil
}
