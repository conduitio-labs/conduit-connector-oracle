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

package common

import "regexp"

const MaxConfigStringLength = 127

// Docs: https://docs.oracle.com/cd/A81042_01/DOC/server.816/a76989/ch29.htm
var IsOracleObjectValid = regexp.MustCompile(`^[a-zA-Z]+[a-zA-Z\d#$_]*$`).MatchString

// A Configuration represents a general configuration needed to connect to Oracle database.
type Configuration struct {
	// URL is the configuration of the connection string to connect to Oracle database.
	URL string `json:"url" validate:"required"`
	// Table is the configuration of the table name.
	Table string `json:"table" validate:"required"`
}
