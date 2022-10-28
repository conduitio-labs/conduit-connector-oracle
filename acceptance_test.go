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

package oracle

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/conduitio-labs/conduit-connector-oracle/config"
	"github.com/conduitio-labs/conduit-connector-oracle/repository"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/matryer/is"
)

type driver struct {
	sdk.ConfigurableAcceptanceTestDriver

	counter int32
}

// GenerateRecord generates a random sdk.Record.
func (d *driver) GenerateRecord(t *testing.T, operation sdk.Operation) sdk.Record {
	atomic.AddInt32(&d.counter, 1)

	return sdk.Record{
		Position:  nil,
		Operation: operation,
		Metadata: map[string]string{
			"oracle.table": strings.ToUpper(d.Config.SourceConfig[config.Table]),
		},
		Key: sdk.RawData(
			fmt.Sprintf(`{"ID":%d}`, d.counter),
		),
		Payload: sdk.Change{After: sdk.RawData(
			fmt.Sprintf(`{"ID":%d,"NAME":"%s"}`, d.counter, uuid.NewString()),
		)},
	}
}

func TestAcceptance(t *testing.T) {
	cfg := prepareConfig(t)

	is := is.New(t)

	sdk.AcceptanceTest(t, &driver{
		ConfigurableAcceptanceTestDriver: sdk.ConfigurableAcceptanceTestDriver{
			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
				Connector:         Connector,
				SourceConfig:      cfg,
				DestinationConfig: cfg,
				BeforeTest: func(t *testing.T) {
					err := createTable(cfg[config.URL], cfg[config.Table])
					is.NoErr(err)
				},
				AfterTest: func(t *testing.T) {
					err := dropTables(cfg[config.URL], cfg[config.Table])
					is.NoErr(err)
				},
			},
		},
	})
}

// receives the connection URL from the environment variable
// and prepares configuration map.
func prepareConfig(t *testing.T) map[string]string {
	url := os.Getenv("ORACLE_URL")
	if url == "" {
		t.Skip("ORACLE_URL env var must be set")

		return nil
	}

	return map[string]string{
		config.URL:            url,
		config.Table:          fmt.Sprintf("CONDUIT_TEST_%s", randString(6)),
		config.KeyColumn:      "ID",
		config.OrderingColumn: "ID",
	}
}

// creates test table.
func createTable(url, table string) error {
	repo, err := repository.New(url)
	if err != nil {
		return fmt.Errorf("init repo: %w", err)
	}
	defer repo.Close()

	_, err = repo.DB.Exec(fmt.Sprintf(`
	CREATE TABLE %s (
		id NUMBER(38,0), 
		name VARCHAR2(100)
	)`, table))
	if err != nil {
		return fmt.Errorf("execute create table query: %w", err)
	}

	return nil
}

// drops test table and tracking test table if exists.
func dropTables(url, table string) error {
	repo, err := repository.New(url)
	if err != nil {
		return fmt.Errorf("init repo: %w", err)
	}
	defer repo.Close()

	_, err = repo.DB.Exec(fmt.Sprintf("DROP TABLE %s", table))
	if err != nil {
		return fmt.Errorf("execute drop table query: %w", err)
	}

	h := fnv.New32a()
	h.Write([]byte(table))
	hash := h.Sum32()

	_, err = repo.DB.Exec(fmt.Sprintf(`
	DECLARE
		tbl_count number;
		sql_stmt long;
	
	BEGIN
		SELECT COUNT(*) INTO tbl_count 
		FROM dba_tables
		WHERE table_name = 'CONDUIT_TRACKING_%d';
	
		IF(tbl_count <> 0)
			THEN
			sql_stmt:='DROP TABLE CONDUIT_TRACKING_%d';
			EXECUTE IMMEDIATE sql_stmt;
		END IF;
	END;`, hash, hash))
	if err != nil {
		return fmt.Errorf("execute drop tracking table query: %w", err)
	}

	_, err = repo.DB.Exec(fmt.Sprintf(`
	DECLARE
		tbl_count number;
		sql_stmt long;
	
	BEGIN
		SELECT COUNT(*) INTO tbl_count 
		FROM dba_tables
		WHERE table_name = 'CONDUIT_SNAPSHOT_%d';
	
		IF(tbl_count <> 0)
			THEN
			sql_stmt:='DROP SNAPSHOT CONDUIT_SNAPSHOT_%d';
			EXECUTE IMMEDIATE sql_stmt;
		END IF;
	END;`, hash, hash))
	if err != nil {
		return fmt.Errorf("execute drop snapshot query: %w", err)
	}

	return nil
}

// generates a random string of length n.
func randString(n int) string {
	b := make([]byte, n)
	rand.Read(b) //nolint:errcheck // does not actually fail

	return strings.ToUpper(hex.EncodeToString(b))
}
