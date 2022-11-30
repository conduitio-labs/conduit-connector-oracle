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

	// OrderingColumn is a name of a column that the connector will use for ordering rows.
	OrderingColumn string `validate:"required,lte=128,oracle"`
	// KeyColumns is the configuration of key column names, separated by commas.
	KeyColumns []string `validate:"omitempty,dive,oracle"`
	// Snapshot is the configuration that determines whether the connector
	// will take a snapshot of the entire table before starting cdc mode.
	Snapshot bool
	// Columns list of column names that should be included in each Record's payload.
	Columns []string `validate:"dive,lte=128,oracle"`
	// BatchSize is a size of rows batch.
	BatchSize int `validate:"gte=1,lte=100000"`
}

// ParseSource parses source configuration.
func ParseSource(cfg map[string]string) (Source, error) {
	config, err := parseConfiguration(cfg)
	if err != nil {
		return Source{}, err
	}

	sourceConfig := Source{
		Configuration:  config,
		OrderingColumn: strings.ToUpper(cfg[OrderingColumn]),
		Snapshot:       defaultSnapshot,
		BatchSize:      defaultBatchSize,
	}

	if cfg[KeyColumns] != "" {
		keyColumns := strings.Split(strings.ReplaceAll(cfg[KeyColumns], " ", ""), ",")
		for i := range keyColumns {
			if keyColumns[i] == "" {
				return Source{}, fmt.Errorf("invalid %q", KeyColumns)
			}

			sourceConfig.KeyColumns = append(sourceConfig.KeyColumns, strings.ToUpper(keyColumns[i]))
		}
	}

	if cfg[Snapshot] != "" {
		snapshot, err := strconv.ParseBool(cfg[Snapshot])
		if err != nil {
			return Source{}, fmt.Errorf("parse %q: %w", Snapshot, err)
		}

		sourceConfig.Snapshot = snapshot
	}

	if cfg[Columns] != "" {
		columnsSl := strings.Split(strings.ReplaceAll(cfg[Columns], " ", ""), ",")
		for i := range columnsSl {
			if columnsSl[i] == "" {
				return Source{}, fmt.Errorf("invalid %q", Columns)
			}

			sourceConfig.Columns = append(sourceConfig.Columns, strings.ToUpper(columnsSl[i]))
		}
	}

	if cfg[BatchSize] != "" {
		sourceConfig.BatchSize, err = strconv.Atoi(cfg[BatchSize])
		if err != nil {
			return Source{}, fmt.Errorf("parse BatchSize: %w", err)
		}
	}

	err = validate(sourceConfig)
	if err != nil {
		return Source{}, err
	}

	if len(sourceConfig.Columns) == 0 {
		return sourceConfig, nil
	}

	columnsMap := make(map[string]struct{}, len(sourceConfig.Columns))
	for i := 0; i < len(sourceConfig.Columns); i++ {
		columnsMap[sourceConfig.Columns[i]] = struct{}{}
	}

	if _, ok := columnsMap[sourceConfig.OrderingColumn]; !ok {
		return Source{}, fmt.Errorf("columns must include %q", OrderingColumn)
	}

	for i := range sourceConfig.KeyColumns {
		if _, ok := columnsMap[sourceConfig.KeyColumns[i]]; !ok {
			return Source{}, fmt.Errorf("columns must include all %q", KeyColumns)
		}
	}

	return sourceConfig, nil
}
