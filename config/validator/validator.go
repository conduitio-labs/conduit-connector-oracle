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

package validator

import (
	"fmt"
	"regexp"
	"sync"

	"github.com/conduitio-labs/conduit-connector-oracle/models"
	v "github.com/go-playground/validator/v10"
	"go.uber.org/multierr"
)

var (
	validatorInstance *v.Validate
	once              sync.Once

	// Docs: https://docs.oracle.com/cd/A81042_01/DOC/server.816/a76989/ch29.htm
	isOracleObjectValid = regexp.MustCompile(`^[a-zA-Z]+[a-zA-Z\d#$_]*$`).MatchString
)

// Get initializes and registers validation tags once, and returns validator instance.
func Get() *v.Validate {
	once.Do(func() {
		validatorInstance = v.New()

		err := validatorInstance.RegisterValidation("oracle", validateOracleObject)
		if err != nil {
			return
		}
	})

	return validatorInstance
}

func Validate(s interface{}) error {
	var err error

	validationErr := Get().Struct(s)
	if validationErr != nil {
		if _, ok := validationErr.(*v.InvalidValidationError); ok {
			return fmt.Errorf("validate general config struct: %w", validationErr)
		}

		for _, e := range validationErr.(v.ValidationErrors) {
			switch e.ActualTag() {
			case "required":
				err = multierr.Append(err, RequiredErr(models.ConfigKeyName(e.Field())))
			case "oracle":
				err = multierr.Append(err, InvalidOracleObjectErr(models.ConfigKeyName(e.Field())))
			case "lte":
				err = multierr.Append(err, OutOfRangeErr(models.ConfigKeyName(e.Field())))
			}
		}
	}

	return err
}

func validateOracleObject(fl v.FieldLevel) bool {
	return isOracleObjectValid(fl.Field().String())
}
