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

package iterator

const (
	// metadata related.
	metadataTable = "oracle.table"

	// actions.
	actionInsert = "insert"
	actionUpdate = "update"
	actionDelete = "delete"

	// tracking table columns.
	columnTrackingID    = "CONDUIT_TRACKING_ID"
	columnOperationType = "CONDUIT_OPERATION_TYPE"
	columnTimeCreatedAt = "CONDUIT_TRACKING_CREATED_AT"

	referencingNew = ":NEW."
	referencingOld = ":OLD."

	timeoutBeforeCloseDBSec        = 20
	timeoutToClearTrackingTableSec = 5

	// queries patterns.
	queryIfTableExists     = "SELECT table_name FROM user_tables WHERE table_name='%s'"
	querySnapshotTable     = "CREATE SNAPSHOT %s AS SELECT * FROM %s"
	queryTableCopy         = "CREATE TABLE %s AS SELECT * FROM %s WHERE 1=2 UNION ALL SELECT * FROM %s WHERE 1=2"
	queryTriggerInsertPart = "INSERT INTO %s (%s, %s) VALUES (%s, transaction_type);"
	querySelectRowsFmt     = "SELECT %s FROM %s%s ORDER BY %s ASC FETCH NEXT %d ROWS ONLY"
	queryDeleteByIDs       = "DELETE FROM %s WHERE %s IN (%s)"

	queryTrackingTableExtendWithConduitColumns = `ALTER TABLE %s 
ADD (
	%s NUMBER GENERATED BY DEFAULT ON NULL AS IDENTITY,
    %s VARCHAR2(6) NOT NULL CHECK(%s IN ('insert','update','delete')),
    %s DATE DEFAULT SYSDATE,
	
	CONSTRAINT %s UNIQUE (%s)
)`

	queryTriggerCreate = `
CREATE OR REPLACE TRIGGER %s
    AFTER
    INSERT OR UPDATE OR DELETE
    ON %s
    FOR EACH ROW
DECLARE
   transaction_type VARCHAR2(6);
BEGIN
   transaction_type := CASE
         WHEN INSERTING THEN 'insert'
         WHEN UPDATING THEN 'update'
         WHEN DELETING THEN 'delete'
   END;

   IF INSERTING OR UPDATING THEN
   %s
   ELSE
   %s
   END IF;
END;`
)
