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

package repository

import (
	"fmt"

	"github.com/jmoiron/sqlx"

	// Go driver for Oracle.
	_ "github.com/godror/godror"
)

// Oracle represents a Oracle repository.
type Oracle struct {
	DB *sqlx.DB
}

// New opens a database and pings it.
func New(url string) (*Oracle, error) {
	db, err := sqlx.Open("godror", url)
	if err != nil {
		return nil, fmt.Errorf("open connection: %w", err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}

	return &Oracle{DB: db}, nil
}

// Close closes database.
func (o *Oracle) Close() error {
	if o != nil {
		return o.DB.Close()
	}

	return nil
}
