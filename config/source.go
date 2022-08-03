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
	"fmt"
	"strconv"
	"strings"

	"github.com/conduitio-labs/conduit-connector-oracle/config/validator"
	"github.com/conduitio-labs/conduit-connector-oracle/models"
)

const (
	defaultBatchSize = 1000
)

// A Source represents a source configuration.
type Source struct {
	General

	// Key - Column name that records should use for their `Key` fields.
	KeyColumn string `validate:"required,lte=128,oracle"`
	// OrderingColumn is a name of a column that the connector will use for ordering rows.
	OrderingColumn string `validate:"required"`
	// Columns  list of column names that should be included in each Record's payload.
	Columns []string
	// BatchSize is a size of rows batch.
	BatchSize int `validate:"gte=1,lte=100000"`
}

// ParseSource parses source configuration.
func ParseSource(cfg map[string]string) (Source, error) {
	config, err := parseGeneral(cfg)
	if err != nil {
		return Source{}, err
	}

	sourceConfig := Source{
		General:        config,
		KeyColumn:      cfg[models.ConfigKeyColumn],
		OrderingColumn: cfg[models.ConfigOrderingColumn],
		BatchSize:      defaultBatchSize,
	}

	if columns := cfg[models.ConfigColumns]; columns != "" {
		if er := validator.ValidateColumns(sourceConfig.OrderingColumn, sourceConfig.KeyColumn, columns); er != nil {
			return Source{}, er
		}
		sourceConfig.Columns = strings.Split(columns, ",")
	}

	if batchSize := cfg[models.ConfigBatchSize]; batchSize != "" {
		sourceConfig.BatchSize, err = strconv.Atoi(batchSize)
		if err != nil {
			return Source{}, fmt.Errorf("parse batchSize: %w", err)
		}
	}

	err = validator.Validate(sourceConfig)
	if err != nil {
		return Source{}, err
	}

	return sourceConfig, nil
}
