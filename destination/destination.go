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

package destination

import (
	"context"
	"fmt"

	"github.com/conduitio-labs/conduit-connector-oracle/destination/config"
	"github.com/conduitio-labs/conduit-connector-oracle/destination/writer"
	"github.com/conduitio-labs/conduit-connector-oracle/repository"
	commonsConfig "github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

//go:generate mockgen -package mock -source destination.go -destination ./mock/destination.go

// Writer defines a writer interface needed for the Destination.
type Writer interface {
	Write(context.Context, opencdc.Record) error
}

// A Destination represents the destination connector.
type Destination struct {
	sdk.UnimplementedDestination

	repo   *repository.Oracle
	writer Writer
	cfg    config.Config
}

// NewDestination initialises a new Destination.
func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
}

// Parameters returns a map of named Parameters that describe how to configure the Source.
func (d *Destination) Parameters() commonsConfig.Parameters {
	return d.cfg.Parameters()
}

// Configure parses and stores configurations, returns an error in case of invalid configuration.
func (d *Destination) Configure(ctx context.Context, cfg commonsConfig.Config) error {
	err := sdk.Util.ParseConfig(ctx, cfg, &d.cfg, NewDestination().Parameters())
	if err != nil {
		return err
	}

	d.cfg = d.cfg.Init()

	err = d.cfg.Validate()
	if err != nil {
		return fmt.Errorf("error validating configuration: %w", err)
	}

	return nil
}

// Open initializes a publisher client.
func (d *Destination) Open(ctx context.Context) (err error) {
	d.repo, err = repository.New(d.cfg.URL)
	if err != nil {
		return fmt.Errorf("new repository: %w", err)
	}

	d.writer, err = writer.New(ctx, writer.Params{
		Repo:      d.repo,
		Table:     d.cfg.Table,
		KeyColumn: d.cfg.KeyColumn,
	})
	if err != nil {
		return fmt.Errorf("new writer: %w", err)
	}

	return nil
}

// Write writes records into a Destination.
func (d *Destination) Write(ctx context.Context, records []opencdc.Record) (int, error) {
	for i, r := range records {
		err := d.writer.Write(ctx, r)
		if err != nil {
			return i, err
		}
	}

	return len(records), nil
}

// Teardown gracefully closes connections.
func (d *Destination) Teardown(_ context.Context) error {
	return d.repo.Close()
}
