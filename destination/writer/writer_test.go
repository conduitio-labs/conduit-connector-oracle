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

package writer

import (
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-oracle/columntypes"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func TestWriter_buildUpsertQuery(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	w := &Writer{
		columnTypes: map[string]columntypes.ColumnDescription{
			"TS": {
				Type: "TIMESTAMP",
			},
			"CREATED": {
				Type: "DATE",
			},
			"FILE": {
				Type: "BLOB",
			},
		},
	}

	payload := sdk.StructuredData{
		"id":      42,
		"name":    "John",
		"ts":      time.Unix(1257894000, 0).UTC(),
		"created": time.Unix(1257894000, 0).UTC(),
		"file":    []byte("test123"),
	}

	wantQuery := "MERGE INTO tbl1 USING DUAL ON (id = :1) " +
		"WHEN MATCHED THEN UPDATE SET created=TO_DATE(:2, 'YYYY-MM-DD HH24:MI:SS'),file=utl_raw.cast_to_raw(:3)," +
		"name=:4,ts=TO_DATE(:5, 'YYYY-MM-DD HH24:MI:SS') " +
		"WHEN NOT MATCHED THEN INSERT (created,file,id,name,ts) VALUES (TO_DATE(:6, 'YYYY-MM-DD HH24:MI:SS')," +
		"utl_raw.cast_to_raw(:7),:8,:9,TO_DATE(:10, 'YYYY-MM-DD HH24:MI:SS'))"

	wantArgs := []any{
		42,
		"2009-11-10 23:00:00",
		[]byte("test123"),
		"John",
		"2009-11-10 23:00:00",
		"2009-11-10 23:00:00",
		[]byte("test123"),
		42,
		"John",
		"2009-11-10 23:00:00",
	}

	query, args, err := w.buildUpsertQuery("tbl1", "id", payload)
	is.NoErr(err)
	is.Equal(query, wantQuery)
	is.Equal(args, wantArgs)
	is.Equal(len(args), len(payload)*2)
}
