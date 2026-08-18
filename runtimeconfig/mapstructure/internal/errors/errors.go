// SPDX-License-Identifier: Apache-2.0
// Provenance-includes-location: https://github.com/go-viper/mapstructure/blob/main/internal/errors/errors.go
// Provenance-includes-license: MIT
// Provenance-includes-copyright: Mitchell Hashimoto

package errors

import "errors"

func New(text string) error {
	return errors.New(text)
}

func As(err error, target interface{}) bool {
	return errors.As(err, target)
}
