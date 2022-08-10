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
	"github.com/conduitio-labs/conduit-connector-oracle/repository"
	sdk "github.com/conduitio/conduit-connector-sdk"

	// Go driver for Oracle.
	_ "github.com/godror/godror"
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

// New initialises a new Destination.
func New() sdk.Destination {
	return &Destination{}
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
func (d *Destination) Write(ctx context.Context, record sdk.Record) error {
	return d.writer.Write(ctx, record)
}

// Teardown gracefully closes connections.
func (d *Destination) Teardown(ctx context.Context) error {
	return d.repo.Close()
}
