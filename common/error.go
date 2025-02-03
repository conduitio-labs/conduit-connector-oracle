// Copyright © 2024 Meroxa, Inc. & Yalantis
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

import (
	"fmt"
)

type InvalidOracleObjectError struct {
	fieldName string
}

func (e InvalidOracleObjectError) Error() string {
	return fmt.Sprintf("%q can contain only alphanumeric characters from your database character set and "+
		"the underscore (_), dollar sign ($), and pound sign (#)", e.fieldName)
}

func NewInvalidOracleObjectError(fieldName string) InvalidOracleObjectError {
	return InvalidOracleObjectError{fieldName: fieldName}
}

type OutOfRangeError struct {
	fieldName string
}

func (e OutOfRangeError) Error() string {
	return fmt.Sprintf("%q is out of range", e.fieldName)
}

func NewOutOfRangeError(fieldName string) OutOfRangeError {
	return OutOfRangeError{fieldName: fieldName}
}
