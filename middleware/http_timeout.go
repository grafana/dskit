package middleware

import (
	"net/http"
	"time"
)

func TimeoutMiddleware(dt time.Duration, msg string) Func {
	return func(next http.Handler) http.Handler {
		return http.TimeoutHandler(next, dt, msg)
	}
}
