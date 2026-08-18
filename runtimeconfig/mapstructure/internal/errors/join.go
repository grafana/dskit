// SPDX-License-Identifier: Apache-2.0
// Provenance-includes-location: https://github.com/go-viper/mapstructure/blob/main/internal/errors/join.go
// Provenance-includes-license: MIT
// Provenance-includes-copyright: Mitchell Hashimoto

//go:build go1.20

package errors

import "errors"

func Join(errs ...error) error {
	return errors.Join(errs...)
}
