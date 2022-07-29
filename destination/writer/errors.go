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

import "errors"

var (
	// ErrEmptyPayload occurs when there's no payload to insert.
	ErrEmptyPayload = errors.New("payload is empty")

	// errEmptyKey occurs when there is no value for key.
	errEmptyKey = errors.New("key value must be provided")
	// errCompositeKeysNotSupported occurs when there are more than one key in a Key map.
	errCompositeKeysNotSupported = errors.New("composite keys not yet supported")
	// errColumnsValuesLenMismatch occurs when trying to insert a row with a different column and value lengths.
	errColumnsValuesLenMismatch = errors.New("number of columns must be equal to number of values")
)
