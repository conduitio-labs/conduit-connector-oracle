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
	metadataTable  = "table"
	metadataAction = "action"

	// actions.
	actionInsert = "insert"

	// tracking table columns.
	columnTrackingID    = "CONDUIT_TRACKING_ID"
	columnOperationType = "CONDUIT_OPERATION_TYPE"
	columnTimeCreatedAt = "CONDUIT_TRACKING_CREATED_AT"
)
