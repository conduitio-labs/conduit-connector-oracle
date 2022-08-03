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
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-oracle/models"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"

	// Go driver for Oracle.
	_ "github.com/godror/godror"
)

const (
	testTableNameFormat = "conduit_destination_test_%d"

	// queries.
	queryCreateTable = "CREATE TABLE %s (id NUMBER NOT NULL, name VARCHAR2(20), is_active NUMBER(1,0), " +
		"attributes VARCHAR2(100), achievements VARCHAR2(100))"
	queryDropTable      = "DROP TABLE %s"
	querySelectNameByID = "SELECT name FROM %s WHERE id = %d"
)

func TestDestination_WriteIntegration(t *testing.T) {
	t.Parallel()

	var (
		ctx = context.Background()
		is  = is.New(t)
	)

	cfg, err := prepareConfig()
	if err != nil {
		t.Skip(err)
	}

	db, err := prepareData(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		err = clearData(ctx, db, cfg)
		is.NoErr(err)

		db.Close()
	})

	dest := new(Destination)

	err = dest.Configure(ctx, cfg)
	if err != nil {
		t.Error(err)
	}

	t.Run("insert", func(t *testing.T) {
		err = dest.Open(ctx)
		is.NoErr(err)

		err = dest.Write(ctx, sdk.Record{
			Payload: sdk.StructuredData{
				"id":        42,
				"name":      "John",
				"is_active": true,
				"attributes": map[string]any{
					"attr_0": "string",
					"attr_1": 1,
					"attr_2": false,
				},
				"achievements": []string{"achievement_0", "achievement_1"},
			},
		})
		is.NoErr(err)

		err = dest.Teardown(ctx)
		is.NoErr(err)
	})

	t.Run("update", func(t *testing.T) {
		err = dest.Open(ctx)
		is.NoErr(err)

		err = dest.Write(ctx, sdk.Record{
			Metadata: map[string]string{
				"action": "update",
			},
			Key: sdk.StructuredData{
				"id": 42,
			},
			Payload: sdk.StructuredData{
				"name": "Jane",
			},
		})
		is.NoErr(err)

		row := db.QueryRowContext(context.Background(),
			fmt.Sprintf(querySelectNameByID, cfg[models.ConfigTable], 42),
		)

		var name string
		err = row.Scan(&name)
		is.NoErr(err)

		is.Equal(name, "Jane")

		err = dest.Teardown(ctx)
		is.NoErr(err)
	})

	t.Run("upsert", func(t *testing.T) {
		err = dest.Open(ctx)
		is.NoErr(err)

		err = dest.Write(ctx, sdk.Record{
			Metadata: map[string]string{
				"action": "update",
			},
			Payload: sdk.StructuredData{
				"id":   42,
				"name": "Sam",
			},
		})
		is.NoErr(err)

		row := db.QueryRowContext(context.Background(),
			fmt.Sprintf(querySelectNameByID, cfg[models.ConfigTable], 42),
		)

		var name string
		err = row.Scan(&name)
		is.NoErr(err)

		is.Equal(name, "Sam")

		err = dest.Teardown(ctx)
		is.NoErr(err)
	})

	t.Run("delete", func(t *testing.T) {
		err = dest.Open(ctx)
		is.NoErr(err)

		err = dest.Write(ctx, sdk.Record{
			Metadata: map[string]string{
				"action": "delete",
			},
			Key: sdk.StructuredData{
				"id": 42,
			},
		})
		is.NoErr(err)

		row := db.QueryRowContext(context.Background(),
			fmt.Sprintf(querySelectNameByID, cfg[models.ConfigTable], 42),
		)

		err = row.Scan()
		is.Equal(err, sql.ErrNoRows)

		err = dest.Teardown(ctx)
		is.NoErr(err)
	})

	t.Run("insert wrong column", func(t *testing.T) {
		err = dest.Open(ctx)
		is.NoErr(err)

		err = dest.Write(ctx, sdk.Record{
			Payload: sdk.StructuredData{
				"age": 42,
			},
		})
		is.Equal(err != nil, true)

		err = dest.Teardown(ctx)
		is.NoErr(err)
	})
}

func prepareConfig() (map[string]string, error) {
	url := os.Getenv("ORACLE_URL")
	if url == "" {
		return nil, errors.New("ORACLE_URL env var must be set")
	}

	return map[string]string{
		models.ConfigURL:       url,
		models.ConfigKeyColumn: "id",
		models.ConfigTable:     generateTableName(),
	}, nil
}

func prepareData(ctx context.Context, cfg map[string]string) (*sql.DB, error) {
	db, err := sql.Open(driverName, cfg[models.ConfigURL])
	if err != nil {
		return nil, fmt.Errorf("open connection: %w", err)
	}

	if err = db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf(queryCreateTable, cfg[models.ConfigTable]))
	if err != nil {
		return nil, fmt.Errorf("execute create table query: %w", err)
	}

	return db, nil
}

func clearData(ctx context.Context, db *sql.DB, cfg map[string]string) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf(queryDropTable, cfg[models.ConfigTable]))
	if err != nil {
		return fmt.Errorf("execute drop table query: %w", err)
	}

	return nil
}

func generateTableName() string {
	return fmt.Sprintf(testTableNameFormat, time.Now().UnixNano())
}
