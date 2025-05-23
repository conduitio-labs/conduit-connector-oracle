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
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/conduitio-labs/conduit-connector-oracle/repository"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

func TestDestination_upsert(t *testing.T) {
	var (
		cfg = prepareConfig(t)
		is  = is.New(t)
	)

	repo, err := repository.New(cfg[ConfigUrl])
	is.NoErr(err)
	defer repo.Close()

	err = createTable(repo, cfg[ConfigTable])
	is.NoErr(err)

	defer func() {
		err = dropTable(repo, cfg[ConfigTable])
		is.NoErr(err)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dest := NewDestination()

	err = dest.Configure(ctx, cfg)
	is.NoErr(err)

	err = dest.Open(ctx)
	is.NoErr(err)

	n, err := dest.Write(ctx, []opencdc.Record{
		{
			Operation: opencdc.OperationUpdate,
			Payload: opencdc.Change{After: opencdc.StructuredData{
				"id":   42,
				"name": "Jane",
			}},
		},
	})
	is.NoErr(err)
	is.Equal(n, 1)

	name, err := getNameByID(repo, cfg[ConfigTable], 42)
	is.NoErr(err)
	is.Equal(name, "Jane")

	cancel()

	err = dest.Teardown(context.Background())
	is.NoErr(err)
}

func TestDestination_delete(t *testing.T) {
	var (
		cfg = prepareConfig(t)
		is  = is.New(t)
	)

	repo, err := repository.New(cfg[ConfigUrl])
	is.NoErr(err)
	defer repo.Close()

	err = createTable(repo, cfg[ConfigTable])
	is.NoErr(err)

	defer func() {
		err = dropTable(repo, cfg[ConfigTable])
		is.NoErr(err)
	}()

	err = insertData(repo, cfg[ConfigTable])
	is.NoErr(err)

	// check if row exists
	_, err = getNameByID(repo, cfg[ConfigTable], 42)
	is.NoErr(err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dest := NewDestination()

	err = dest.Configure(ctx, cfg)
	is.NoErr(err)

	err = dest.Open(ctx)
	is.NoErr(err)

	n, err := dest.Write(ctx, []opencdc.Record{
		{
			Operation: opencdc.OperationDelete,
			Key:       opencdc.RawData(`{"id":42}`),
		},
	})
	is.NoErr(err)
	is.Equal(n, 1)

	_, err = getNameByID(repo, cfg[ConfigTable], 42)
	is.Equal(err.Error(), "scan row: sql: no rows in result set")

	cancel()

	err = dest.Teardown(context.Background())
	is.NoErr(err)
}

func TestDestination_wrongColumn(t *testing.T) {
	var (
		cfg = prepareConfig(t)
		is  = is.New(t)
	)

	repo, err := repository.New(cfg[ConfigUrl])
	is.NoErr(err)
	defer repo.Close()

	err = createTable(repo, cfg[ConfigTable])
	is.NoErr(err)

	defer func() {
		err = dropTable(repo, cfg[ConfigTable])
		is.NoErr(err)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dest := NewDestination()

	err = dest.Configure(ctx, cfg)
	is.NoErr(err)

	err = dest.Open(ctx)
	is.NoErr(err)

	_, err = dest.Write(ctx, []opencdc.Record{
		{
			Operation: opencdc.OperationSnapshot,
			Payload: opencdc.Change{After: opencdc.StructuredData{
				"id":           43,
				"wrong_column": "test",
			}},
		},
	})
	is.True(strings.Contains(err.Error(), "invalid identifier"))

	cancel()

	err = dest.Teardown(context.Background())
	is.NoErr(err)
}

func prepareConfig(t *testing.T) map[string]string {
	url := os.Getenv("ORACLE_URL")
	if url == "" {
		t.Skip("ORACLE_URL env var must be set")

		return nil
	}

	return map[string]string{
		ConfigUrl:       url,
		ConfigTable:     fmt.Sprintf("CONDUIT_DEST_TEST_%s", randString(6)),
		ConfigKeyColumn: "id",
	}
}

func createTable(repo *repository.Oracle, table string) error {
	_, err := repo.DB.Exec(fmt.Sprintf(`
	CREATE TABLE %s (
		id NUMBER NOT NULL, 
		name VARCHAR2(20), 
		is_active NUMBER(1,0), 
		attributes VARCHAR2(100), 
		achievements VARCHAR2(100)
	)`, table))
	if err != nil {
		return fmt.Errorf("execute create table query: %w", err)
	}

	return nil
}

func dropTable(repo *repository.Oracle, table string) error {
	_, err := repo.DB.Exec(fmt.Sprintf("DROP TABLE %s", table))
	if err != nil {
		return fmt.Errorf("execute drop table query: %w", err)
	}

	return nil
}

func insertData(repo *repository.Oracle, table string) error {
	_, err := repo.DB.Exec(fmt.Sprintf("INSERT INTO %s (id, name) VALUES (42, 'Sam')", table))
	if err != nil {
		return fmt.Errorf("execute insert query: %w", err)
	}

	return nil
}

func getNameByID(repo *repository.Oracle, table string, id int) (string, error) {
	row := repo.DB.QueryRow(fmt.Sprintf("SELECT name FROM %s WHERE id = %d", table, id))

	name := ""

	err := row.Scan(&name)
	if err != nil {
		return "", fmt.Errorf("scan row: %w", err)
	}

	return name, nil
}

// generates a random string of length n.
func randString(n int) string {
	b := make([]byte, n)
	rand.Read(b) //nolint:errcheck // does not actually fail

	return strings.ToUpper(hex.EncodeToString(b))
}
