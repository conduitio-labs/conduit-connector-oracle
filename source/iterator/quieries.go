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

import (
	"fmt"
	"strings"

	"github.com/conduitio-labs/conduit-connector-oracle/coltypes"
)

const (
	queryTableIsExists = `
SELECT table_name FROM user_tables WHERE table_name='%s'`

	queryTableCopy = `
CREATE TABLE %s AS SELECT * FROM %s WHERE 1=2 UNION ALL SELECT * FROM %s WHERE 1=2`

	queryTrackingTableExtendWithConduitColumns = `
ALTER TABLE %s 
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

	queryTriggerInsertPart = `
INSERT INTO %s (%s, %s) 
VALUES (%s, transaction_type);
`
)

func buildExpandTrackingTableQuery(trackingTable string) string {
	return fmt.Sprintf(queryTrackingTableExtendWithConduitColumns, trackingTable, columnTrackingID, columnOperationType,
		columnOperationType, columnTimeCreatedAt, trackingTable, columnTrackingID)
}

type buildCreateTriggerParams struct {
	name          string
	table         string
	trackingTable string
	columnTypes   map[string]coltypes.ColumnData
	columns       []string
}

func buildCreateTriggerQuery(params buildCreateTriggerParams) string {
	var columnNames []string

	if params.columns != nil {
		columnNames = append(columnNames, params.columns...)
	} else {
		for key := range params.columnTypes {
			columnNames = append(columnNames, key)
		}
	}

	newValues := make([]string, len(columnNames))
	oldValues := make([]string, len(columnNames))
	for i := range columnNames {
		newValues[i] = fmt.Sprintf("%s%s", referencingNew, columnNames[i])
		oldValues[i] = fmt.Sprintf("%s%s", referencingOld, columnNames[i])
	}

	insertOnInsertingOrUpdating := fmt.Sprintf(queryTriggerInsertPart, params.trackingTable,
		strings.Join(columnNames, ","), columnOperationType, strings.Join(newValues, ","))
	insertOnDeleting := fmt.Sprintf(queryTriggerInsertPart, params.trackingTable,
		strings.Join(columnNames, ","), columnOperationType, strings.Join(oldValues, ","))

	return fmt.Sprintf(queryTriggerCreate, params.name, params.table, insertOnInsertingOrUpdating, insertOnDeleting)
}
