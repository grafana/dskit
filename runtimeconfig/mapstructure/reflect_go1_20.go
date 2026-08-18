// SPDX-License-Identifier: Apache-2.0
// Provenance-includes-location: https://github.com/go-viper/mapstructure/blob/main/reflect_go1_20.go
// Provenance-includes-license: MIT
// Provenance-includes-copyright: Mitchell Hashimoto

//go:build go1.20

package mapstructure

import "reflect"

// TODO: remove once we drop support for Go <1.20
func isComparable(v reflect.Value) bool {
	return v.Comparable()
}
