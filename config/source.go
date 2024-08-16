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

//go:generate paramgen -output=paramgen.go Source

package config

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
)

const (
	SnapshotTable = "snapshotTable"
	TrackingTable = "trackingTable"
	Trigger       = "trigger"
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

	// defaultBatchSize is the default value of the BatchSize field.
	defaultBatchSize = 1000
	// defaultSnapshot is a default value for the Snapshot field.
	defaultSnapshot = true
)

// A Source represents a source configuration.
type Source struct {
	Configuration
	// SnapshotTable is the snapshot table to be used.
	SnapshotTable string `validate:"lte=128,oracle"`
	// TrackingTable is the tracking table to be used in CDC.
	TrackingTable string `validate:"lte=128,oracle"`
	// Trigger is the trigger to be used in CDC.
	// required false, default ""
	Trigger string `validate:"lte=128,oracle"`

	// OrderingColumn is Column name that the connector will use for ordering rows. Must contain unique values and suitable for sorting, otherwise the snapshot won't work correctly.
	OrderingColumn string `validate:"required,lte=128,oracle"`

	// KeyColumns is a comma-separated list of column names to build the opencdc.Record.Key.
	KeyColumns []string `validate:"omitempty,dive,lte=128,oracle"`

	// Snapshot is the configuration that determines whether the connector will take a snapshot of the entire table before starting cdc mode
	Snapshot bool

	// Columns list of column names that should be included in each Record's payload, by default includes all columns.
	Columns []string `validate:"dive,lte=128,oracle"`

	// BatchSize is a size of rows batch. Min is 1 and max is 100000. default = 1000, required = false
	BatchSize int `validate:"gte=1,lte=100000"`
}

// ParseSource parses a map with source configuration values.
// A new config.Source should always be constructed using this function.
func ParseSource(cfgMap map[string]string) (Source, error) {
	config, err := parseConfiguration(cfgMap)
	if err != nil {
		return Source{}, err
	}

	cfg := Source{
		Configuration:  config,
		OrderingColumn: strings.ToUpper(cfgMap[OrderingColumn]),
		Snapshot:       defaultSnapshot,
		BatchSize:      defaultBatchSize,
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

	cfg.setHelperObjects(cfgMap)

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

func (s *Source) setHelperObjects(cfgMap map[string]string) {
	s.setDefaultHelperObjects()
	if cfgMap[SnapshotTable] != "" {
		s.SnapshotTable = strings.ToUpper(cfgMap[SnapshotTable])
	}
	if cfgMap[TrackingTable] != "" {
		s.TrackingTable = strings.ToUpper(cfgMap[TrackingTable])
	}
	if cfgMap[Trigger] != "" {
		s.Trigger = strings.ToUpper(cfgMap[Trigger])
	}
}

func (s *Source) setDefaultHelperObjects() {
	// There's a limit on the length of names in Oracle.
	// Depending on the version, it might be between 30 and 128 bytes.
	// We're going with the safer (lower) limit here.
	id := rand.Int31() //nolint:gosec // no need for a strong random generator here
	if id < 0 {
		id = -id
	}

	s.SnapshotTable = fmt.Sprintf("CONDUIT_SNAPSHOT_%d", id)
	s.TrackingTable = fmt.Sprintf("CONDUIT_TRACKING_%d", id)
	s.Trigger = fmt.Sprintf("CONDUIT_TRIGGER_%d", id)
}
