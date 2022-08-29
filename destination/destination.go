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

package destination

import (
	"context"
	"fmt"

	"github.com/conduitio-labs/conduit-connector-oracle/config"
	"github.com/conduitio-labs/conduit-connector-oracle/destination/writer"
	"github.com/conduitio-labs/conduit-connector-oracle/models"
	"github.com/conduitio-labs/conduit-connector-oracle/repository"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Writer defines a writer interface needed for the Destination.
type Writer interface {
	Write(context.Context, sdk.Record) error
}

// A Destination represents the destination connector.
type Destination struct {
	sdk.UnimplementedDestination

	repo   *repository.Oracle
	writer Writer
	cfg    config.Destination
}

// NewDestination initialises a new Destination.
func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
}

// Parameters returns a map of named Parameters that describe how to configure the Source.
func (d *Destination) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
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
		},
	}
}

// Configure parses and stores configurations, returns an error in case of invalid configuration.
func (d *Destination) Configure(_ context.Context, cfg map[string]string) error {
	configuration, err := config.ParseDestination(cfg)
	if err != nil {
		return err
	}

	d.cfg = configuration

	return nil
}

// Open initializes a publisher client.
func (d *Destination) Open(_ context.Context) (err error) {
	d.repo, err = repository.New(d.cfg.URL)
	if err != nil {
		return fmt.Errorf("new repository: %w", err)
	}

	d.writer = writer.New(writer.Params{
		Repo:      d.repo,
		Table:     d.cfg.Table,
		KeyColumn: d.cfg.KeyColumn,
	})

	return nil
}

// Write writes a record into a Destination.
func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	for i, r := range records {
		err := d.writer.Write(ctx, r)
		if err != nil {
			return i, err
		}
	}

	return len(records), nil
}

// Teardown gracefully closes connections.
func (d *Destination) Teardown(ctx context.Context) error {
	return d.repo.Close()
}
