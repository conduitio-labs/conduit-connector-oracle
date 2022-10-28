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
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"os"
	"strings"
	"testing"

	"github.com/conduitio-labs/conduit-connector-oracle/config"
	"github.com/conduitio-labs/conduit-connector-oracle/repository"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func TestSource_Read_NoTable(t *testing.T) {
	var (
		ctx = context.Background()
		cfg = prepareConfig(t)
		is  = is.New(t)
	)

	repo, err := repository.New(cfg[config.URL])
	is.NoErr(err)
	defer repo.Close()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	src := NewSource()

	err = src.Configure(ctx, cfg)
	is.NoErr(err)

	err = src.Open(ctx, nil)
	is.True(err != nil)

	cancel()
}

func TestSource_Read_EmptyTable(t *testing.T) {
	var (
		ctx = context.Background()
		cfg = prepareConfig(t)
		is  = is.New(t)
	)

	repo, err := repository.New(cfg[config.URL])
	is.NoErr(err)
	defer repo.Close()

	err = createTable(repo, cfg[config.Table])
	is.NoErr(err)

	defer func() {
		err = dropTables(repo, cfg[config.Table])
		is.NoErr(err)
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	src := NewSource()

	err = src.Configure(ctx, cfg)
	is.NoErr(err)

	err = src.Open(ctx, nil)
	is.NoErr(err)

	_, err = src.Read(ctx)
	is.Equal(err, sdk.ErrBackoffRetry)

	cancel()

	err = src.Teardown(context.Background())
	is.NoErr(err)
}

func TestSource_Snapshot_Read(t *testing.T) {
	var (
		ctx = context.Background()
		cfg = prepareConfig(t)
		is  = is.New(t)
	)

	repo, err := repository.New(cfg[config.URL])
	is.NoErr(err)
	defer repo.Close()

	err = createTable(repo, cfg[config.Table])
	is.NoErr(err)

	defer func() {
		err = dropTables(repo, cfg[config.Table])
		is.NoErr(err)
	}()

	// insert snapshot data
	err = insertSnapshotData(repo, cfg[config.Table])
	is.NoErr(err)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	src := NewSource()

	err = src.Configure(ctx, cfg)
	is.NoErr(err)

	err = src.Open(ctx, nil)
	is.NoErr(err)

	// read records
	record, err := src.Read(ctx)
	is.NoErr(err)
	is.Equal(record.Position, sdk.Position(`{"mode":"snapshot","last_processed_val":1}`))
	is.Equal(record.Operation, sdk.OperationSnapshot)
	is.Equal(record.Key, sdk.StructuredData(map[string]interface{}{"ID": 1}))
	is.Equal(record.Payload.After, sdk.RawData(`{"AGE":42,"ID":1,"IS_ACTIVE":true,"NAME":"John"}`))

	record, err = src.Read(ctx)
	is.NoErr(err)
	is.Equal(record.Position, sdk.Position(`{"mode":"snapshot","last_processed_val":2}`))
	is.Equal(record.Operation, sdk.OperationSnapshot)
	is.Equal(record.Key, sdk.StructuredData(map[string]interface{}{"ID": 2}))
	is.Equal(record.Payload.After, sdk.RawData(`{"AGE":12,"ID":2,"IS_ACTIVE":false,"NAME":"Jane"}`))

	cancel()

	err = src.Teardown(context.Background())
	is.NoErr(err)

	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	src = NewSource()

	err = src.Configure(ctx, cfg)
	is.NoErr(err)

	// open source with no nil position
	err = src.Open(ctx, record.Position)
	is.NoErr(err)

	record, err = src.Read(ctx)
	is.NoErr(err)
	is.Equal(record.Position, sdk.Position(`{"mode":"snapshot","last_processed_val":3}`))
	is.Equal(record.Operation, sdk.OperationSnapshot)
	is.Equal(record.Key, sdk.StructuredData(map[string]interface{}{"ID": 3}))
	is.Equal(record.Payload.After, sdk.RawData(`{"AGE":98,"ID":3,"IS_ACTIVE":true,"NAME":"Sam"}`))

	_, err = src.Read(ctx)
	is.Equal(err, sdk.ErrBackoffRetry)

	cancel()

	err = src.Teardown(context.Background())
	is.NoErr(err)
}

func TestSource_CDC_Read(t *testing.T) {
	var (
		ctx = context.Background()
		cfg = prepareConfig(t)
		is  = is.New(t)
	)

	repo, err := repository.New(cfg[config.URL])
	is.NoErr(err)
	defer repo.Close()

	err = createTable(repo, cfg[config.Table])
	is.NoErr(err)

	defer func() {
		err = dropTables(repo, cfg[config.Table])
		is.NoErr(err)
	}()

	// insert snapshot data (3 records)
	err = insertSnapshotData(repo, cfg[config.Table])
	is.NoErr(err)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	src := NewSource()

	err = src.Configure(ctx, cfg)
	is.NoErr(err)

	err = src.Open(ctx, nil)
	is.NoErr(err)

	// read the first snapshot record
	_, err = src.Read(ctx)
	is.NoErr(err)

	// read the second snapshot record
	_, err = src.Read(ctx)
	is.NoErr(err)

	// update the second record to check that the CDC iterator will start immediately after the snapshot
	err = updateCDCData(repo, cfg[config.Table])
	is.NoErr(err)

	// read the third snapshot record
	_, err = src.Read(ctx)
	is.NoErr(err)

	// read the first cdc record without sdk.ErrBackoffRetry error between iterators
	record, err := src.Read(ctx)
	is.NoErr(err)
	is.Equal(record.Position, sdk.Position(`{"mode":"cdc","last_processed_val":1}`))
	is.Equal(record.Operation, sdk.OperationUpdate)
	is.Equal(record.Key, sdk.StructuredData(map[string]interface{}{"ID": 2}))
	is.Equal(record.Payload.After, sdk.RawData(`{"AGE":33,"ID":2,"IS_ACTIVE":false,"NAME":"Jane"}`))

	cancel()

	// insert two records more
	err = insertCDCData(repo, cfg[config.Table])
	is.NoErr(err)

	err = src.Teardown(context.Background())
	is.NoErr(err)

	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	src = NewSource()

	err = src.Configure(ctx, cfg)
	is.NoErr(err)

	// open source with no nil position
	err = src.Open(ctx, record.Position)
	is.NoErr(err)

	record, err = src.Read(ctx)
	is.NoErr(err)
	is.Equal(record.Position, sdk.Position(`{"mode":"cdc","last_processed_val":2}`))
	is.Equal(record.Operation, sdk.OperationCreate)
	is.Equal(record.Key, sdk.StructuredData(map[string]interface{}{"ID": 4}))
	is.Equal(record.Payload.After, sdk.RawData(`{"AGE":81,"ID":4,"IS_ACTIVE":false,"NAME":"Smith"}`))

	record, err = src.Read(ctx)
	is.NoErr(err)
	is.Equal(record.Position, sdk.Position(`{"mode":"cdc","last_processed_val":3}`))
	is.Equal(record.Operation, sdk.OperationCreate)
	is.Equal(record.Key, sdk.StructuredData(map[string]interface{}{"ID": 5}))
	is.Equal(record.Payload.After, sdk.RawData(`{"AGE":26,"ID":5,"IS_ACTIVE":true,"NAME":"Elizabeth"}`))

	_, err = src.Read(ctx)
	is.Equal(err, sdk.ErrBackoffRetry)

	// delete the row with ID=3
	err = deleteCDCData(repo, cfg[config.Table])
	is.NoErr(err)

	record, err = src.Read(ctx)
	is.NoErr(err)
	is.Equal(record.Position, sdk.Position(`{"mode":"cdc","last_processed_val":4}`))
	is.Equal(record.Operation, sdk.OperationDelete)
	is.Equal(record.Key, sdk.StructuredData(map[string]interface{}{"ID": 3}))
	is.Equal(record.Payload, sdk.Change{})

	_, err = src.Read(ctx)
	is.Equal(err, sdk.ErrBackoffRetry)

	cancel()

	err = src.Teardown(context.Background())
	is.NoErr(err)
}

func prepareConfig(t *testing.T) map[string]string {
	url := os.Getenv("ORACLE_URL")
	if url == "" {
		t.Skip("ORACLE_URL env var must be set")

		return nil
	}

	return map[string]string{
		config.URL:            url,
		config.Table:          fmt.Sprintf("CONDUIT_SRC_TEST_%s", randString(6)),
		config.KeyColumn:      "id",
		config.OrderingColumn: "id",
	}
}

func createTable(repo *repository.Oracle, table string) error {
	_, err := repo.DB.Exec(fmt.Sprintf(`
	CREATE TABLE %s (
		 id NUMBER GENERATED by default on null as IDENTITY,
		 name VARCHAR2(30) NOT NULL,
		 age NUMBER,
		 is_active NUMBER(1,0),
		 PRIMARY KEY (id)
	)`, table))
	if err != nil {
		return fmt.Errorf("execute create table query: %w", err)
	}

	return nil
}

func dropTables(repo *repository.Oracle, table string) error {
	_, err := repo.DB.Exec(fmt.Sprintf("DROP TABLE %s", table))
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

	return nil
}

func insertSnapshotData(repo *repository.Oracle, table string) error {
	_, err := repo.DB.Exec(fmt.Sprintf(`
	INSERT INTO %s (name, age, is_active)
		WITH p AS (
			SELECT 'John', 42, 1 FROM dual UNION ALL
			SELECT 'Jane', 12, 0 FROM dual UNION ALL
			SELECT 'Sam', 98, 1 FROM dual
		)
	SELECT * FROM p`, table))
	if err != nil {
		return fmt.Errorf("execute insert for snapshot query: %w", err)
	}

	return nil
}

func insertCDCData(repo *repository.Oracle, table string) error {
	_, err := repo.DB.Exec(fmt.Sprintf(`
	INSERT INTO %s (name, age, is_active)
		WITH p AS (
			SELECT 'Smith', 81, 0 FROM dual UNION ALL
			SELECT 'Elizabeth', 26, 1 FROM dual
		)
	SELECT * FROM p`, table))
	if err != nil {
		return fmt.Errorf("execute insert for cdc query: %w", err)
	}

	return nil
}

func updateCDCData(repo *repository.Oracle, table string) error {
	_, err := repo.DB.Exec(fmt.Sprintf("UPDATE %s SET age = 33 WHERE ID=2", table))
	if err != nil {
		return fmt.Errorf("execute update query: %w", err)
	}

	return nil
}

func deleteCDCData(repo *repository.Oracle, table string) error {
	_, err := repo.DB.Exec(fmt.Sprintf("DELETE FROM %s WHERE ID=3", table))
	if err != nil {
		return fmt.Errorf("execute delete query: %w", err)
	}

	return nil
}

// randString generates a random string of length n.
// (source: https://stackoverflow.com/a/47676287)
func randString(n int) string {
	b := make([]byte, n)
	rand.Read(b) //nolint:errcheck // does not actually fail

	return strings.ToUpper(hex.EncodeToString(b))
}
