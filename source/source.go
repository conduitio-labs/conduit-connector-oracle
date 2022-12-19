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

package source

import (
	"context"
	"fmt"

	"github.com/conduitio-labs/conduit-connector-oracle/source/iterator"
	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/conduitio-labs/conduit-connector-oracle/config"
)

// Iterator interface.
type Iterator interface {
	HasNext(context.Context) (bool, error)
	Next(context.Context) (sdk.Record, error)
	PushValueToDelete(sdk.Position) error
	Close() error
}

// Source connector.
type Source struct {
	sdk.UnimplementedSource

	config   config.Source
	iterator Iterator
}

// NewSource initialises a new source.
func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{}, sdk.DefaultSourceMiddleware()...)
}

// Parameters returns a map of named Parameters that describe how to configure the Source.
func (s *Source) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		config.URL: {
			Default:     "",
			Required:    true,
			Description: "String line for connection to Oracle",
		},
		config.Table: {
			Default:     "",
			Required:    true,
			Description: "The name of a table in the database that the connector should write to.",
		},
		config.OrderingColumn: {
			Default:  "",
			Required: true,
			Description: "Column name that the connector will use for ordering rows. Column must contain unique " +
				"values and suitable for sorting, otherwise the snapshot won't work correctly.",
		},
		config.KeyColumns: {
			Default:  "",
			Required: false,
			Description: "Comma-separated list of column names to build the sdk.Record.Key. " +
				"Column names are the keys of the sdk.Record.Key map, and the values are taken from the row.",
		},
		config.Snapshot: {
			Default:     "true",
			Required:    false,
			Description: "Whether the connector will take a snapshot of the entire table before starting cdc mode.",
		},
		config.Columns: {
			Default:  "",
			Required: false,
			Description: "List of column names that should be included in each Record's payload, " +
				"by default includes all columns.",
		},
		config.BatchSize: {
			Default:     "1000",
			Required:    false,
			Description: "Size of rows batch. Min is 1 and max is 100000.",
		},
	}
}

// Configure parses and stores configurations, returns an error in case of invalid configuration.
func (s *Source) Configure(_ context.Context, cfgRaw map[string]string) error {
	cfg, err := config.ParseSource(cfgRaw)
	if err != nil {
		return err
	}

	s.config = cfg

	return nil
}

// Open prepare the plugin to start sending records from the given position.
func (s *Source) Open(ctx context.Context, position sdk.Position) error {
	pos, err := iterator.ParseSDKPosition(position)
	if err != nil {
		return fmt.Errorf("parse position: %w", err)
	}

	s.iterator, err = iterator.New(ctx, iterator.Params{
		Position:       pos,
		URL:            s.config.URL,
		Table:          s.config.Table,
		OrderingColumn: s.config.OrderingColumn,
		KeyColumns:     s.config.KeyColumns,
		Snapshot:       s.config.Snapshot,
		Columns:        s.config.Columns,
		BatchSize:      s.config.BatchSize,
	})
	if err != nil {
		return fmt.Errorf("new iterator: %w", err)
	}

	return nil
}

// Read returns the next record.
func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	hasNext, err := s.iterator.HasNext(ctx)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("has next: %w", err)
	}

	if !hasNext {
		return sdk.Record{}, sdk.ErrBackoffRetry
	}

	r, err := s.iterator.Next(ctx)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("next: %w", err)
	}

	return r, nil
}

// Ack appends the last processed value to the slice to clear the tracking table in the future.
func (s *Source) Ack(ctx context.Context, position sdk.Position) error {
	sdk.Logger(ctx).Debug().Str("position", string(position)).Msg("got ack")

	return s.iterator.PushValueToDelete(position)
}

// Teardown gracefully shutdown connector.
func (s *Source) Teardown(_ context.Context) error {
	if s.iterator != nil {
		err := s.iterator.Close()
		if err != nil {
			return fmt.Errorf("stops iterators and closes database connection: %w", err)
		}
	}

	return nil
}
