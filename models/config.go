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

package models

const (
	// ConfigURL is the configuration name of the url.
	ConfigURL = "url"
	// ConfigTable is the configuration name of the table.
	ConfigTable = "table"
	// ConfigKeyColumn is the configuration name of the key column.
	ConfigKeyColumn = "keyColumn"
)

// ConfigKeyName returns a configuration key name by struct field.
func ConfigKeyName(fieldName string) string {
	return map[string]string{
		"URL":       ConfigURL,
		"Table":     ConfigTable,
		"KeyColumn": ConfigKeyColumn,
	}[fieldName]
}
