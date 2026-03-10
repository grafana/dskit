// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/mimir/blob/main/pkg/util/propagation/middleware.go
// Provenance-includes-license: AGPL-3.0-only
// Provenance-includes-copyright: Grafana Labs

package propagation

import (
	"net/http"

	"github.com/grafana/dskit/middleware"
)

// Middleware returns an HTTP middleware that calls extractor on each request,
// injecting extracted values into the request context before passing it on.
func Middleware(extractor Extractor) middleware.Interface {
	return middleware.Func(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, err := extractor.ExtractFromCarrier(r.Context(), HttpHeaderCarrier(r.Header))
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			r = r.WithContext(ctx)
			next.ServeHTTP(w, r)
		})
	})
}
