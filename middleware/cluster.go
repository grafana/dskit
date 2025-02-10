package middleware

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/dskit/clusterutil"
)

// ClusterValidationMiddleware validates that requests are for the correct cluster.
func ClusterValidationMiddleware(cluster string, auxPaths []string, invalidClusters *prometheus.CounterVec, logger log.Logger) Interface {
	var reB strings.Builder
	// Allow for a potential path prefix being configured.
	reB.WriteString(".*/(metrics|debug/pprof.*|ready")
	for _, p := range auxPaths {
		reB.WriteString("|" + regexp.QuoteMeta(p))
	}
	reB.WriteString(")")
	reAuxPath := regexp.MustCompile(reB.String())
	// TODO: Remove me.
	// reAuxPath := regexp.MustCompile(".*/(metrics|debug/pprof.*|ready|backlog_replay_complete|admission/no-downscale|admission/prepare-downscale)")

	return Func(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reqCluster := r.Header.Get(clusterutil.ClusterVerificationLabelHeader)
			if !reAuxPath.MatchString(r.URL.Path) && reqCluster != cluster {
				level.Warn(logger).Log("msg", "rejecting request with wrong cluster verification label",
					"cluster_verification_label", cluster, "request_cluster_verification_label", reqCluster,
					"header", clusterutil.ClusterVerificationLabelHeader, "url", r.URL, "path", r.URL.Path)
				if invalidClusters != nil {
					invalidClusters.WithLabelValues("http", r.URL.Path, reqCluster).Inc()
				}
				http.Error(w, fmt.Sprintf("request has cluster verification label %q - it should be %q", reqCluster, cluster),
					http.StatusBadRequest)
				return
			}

			next.ServeHTTP(w, r)
		})
	})
}
