// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/weaveworks/common/blob/main/middleware/instrument_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Weaveworks Ltd.

package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

func TestRouteInjector(t *testing.T) {
	testCases := map[string]string{
		"/":                       "root",
		"/foo/bar/blah":           "foo_bar_blah",
		"/templated/name-1/thing": "templated_name_thing",
		"/named":                  "my-named-route",
		"/does-not-exist":         "notfound",
	}

	for url, expectedRouteName := range testCases {
		t.Run(url, func(t *testing.T) {
			actualRouteName := ""

			handler := func(_ http.ResponseWriter, r *http.Request) {
				actualRouteName = ExtractRouteName(r.Context())
			}

			router := mux.NewRouter()
			router.HandleFunc("/", handler)
			router.HandleFunc("/foo/bar/blah", handler)
			router.HandleFunc("/templated/{name}/thing", handler)
			router.HandleFunc("/named", handler).Name("my-named-route")
			router.NotFoundHandler = http.HandlerFunc(handler)

			endpoint := RouteInjector{router}.Wrap(router)
			endpoint.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, url, nil))

			require.Equal(t, expectedRouteName, actualRouteName)
		})
	}

}

func TestMakeLabelValue(t *testing.T) {
	for input, want := range map[string]string{
		"/":                      "root", // special case
		"//":                     "root", // unintended consequence of special case
		"a":                      "a",
		"/foo":                   "foo",
		"foo/":                   "foo",
		"/foo/":                  "foo",
		"/foo/bar":               "foo_bar",
		"foo/bar/":               "foo_bar",
		"/foo/bar/":              "foo_bar",
		"/foo/{orgName}/Bar":     "foo_orgname_bar",
		"/foo/{org_name}/Bar":    "foo_org_name_bar",
		"/foo/{org__name}/Bar":   "foo_org_name_bar",
		"/foo/{org___name}/_Bar": "foo_org_name_bar",
		"/foo.bar/baz.qux/":      "foo_bar_baz_qux",
	} {
		if have := MakeLabelValue(input); want != have {
			t.Errorf("%q: want %q, have %q", input, want, have)
		}
	}
}
