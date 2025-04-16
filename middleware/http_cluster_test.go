package middleware

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/clusterutil"
)

func TestClusterValidationRoundTripper(t *testing.T) {
	const genericErr = "generic error"
	testCases := map[string]struct {
		serverResponse     func(w http.ResponseWriter)
		cluster            string
		expectedMetrics    string
		expectedLogs       string
		shouldPanic        bool
		expectedErr        error
		expectedStatusCode int
		expectedResponse   string
	}{
		"if no cluster label is set ClusterValidationRoundTripper panics": {
			cluster:     "",
			shouldPanic: true,
		},
		"if cluster label is set it should be propagated to the next handler": {
			cluster: "cluster",
			serverResponse: func(w http.ResponseWriter) {
				w.WriteHeader(http.StatusOK)
			},
		},
		"if the server returns a clusterValidationError it is handled by ClusterValidationRoundTripper": {
			cluster: "cluster",
			serverResponse: func(w http.ResponseWriter) {
				err := clusterValidationError{ClusterValidationErrorMessage: "this is a cluster validation error"}
				err.writeAsJSON(w)
			},
			expectedErr:        fmt.Errorf("request rejected by the server: this is a cluster validation error"),
			expectedStatusCode: http.StatusNetworkAuthenticationRequired,
			expectedResponse:   "request rejected by the server: this is a cluster validation error",
			expectedMetrics: `
				# HELP test_request_invalid_cluster_validation_labels_total Number of requests with invalid cluster validation label.
				# TYPE test_request_invalid_cluster_validation_labels_total counter
				test_request_invalid_cluster_validation_labels_total{method="/Test/Me"} 1
			`,
			expectedLogs: `level=warn msg="request rejected by the server: this is a cluster validation error" method=/Test/Me cluster_validation_label=cluster`,
		},
		"if the server returns a generic error with http.StatusNetworkAuthenticationRequired status code the error is propagated": {
			cluster: "cluster",
			serverResponse: func(w http.ResponseWriter) {
				w.WriteHeader(http.StatusNetworkAuthenticationRequired)
				_, _ = w.Write([]byte(genericErr))
			},
			expectedStatusCode: http.StatusNetworkAuthenticationRequired,
			expectedResponse:   genericErr,
		},
		"if the server returns a generic error without http.StatusNetworkAuthenticationRequired status code the error is propagated": {
			cluster: "cluster",
			serverResponse: func(w http.ResponseWriter) {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(genericErr))
			},
			expectedStatusCode: http.StatusInternalServerError,
			expectedResponse:   genericErr,
		},
	}
	verifyClusterPropagation := func(req *http.Request, expectedCluster string) {
		cluster, err := clusterutil.GetClusterFromRequest(req)
		require.NoError(t, err)
		require.Equal(t, expectedCluster, cluster)
	}
	invalidClusterValidationReporter := func(cluster string, logger log.Logger, invalidClusterValidations *prometheus.CounterVec) InvalidClusterValidationReporter {
		return func(msg string, method string) {
			level.Warn(logger).Log("msg", msg, "method", method, "cluster_validation_label", cluster)
			invalidClusterValidations.WithLabelValues(method).Inc()
		}
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			defer func() {
				r := recover()
				require.Equal(t, testCase.shouldPanic, r != nil)
			}()
			reg := prometheus.NewPedanticRegistry()
			buf := bytes.NewBuffer(nil)
			logger := createLogger(t, buf)

			// Create an HTTP client.
			client := http.DefaultClient
			client.Timeout = 60 * time.Second
			client.Transport = ClusterValidationRoundTripper(testCase.cluster, invalidClusterValidationReporter(testCase.cluster, logger, newRequestInvalidClusterValidationLabelsTotalCounter(reg)), http.DefaultTransport)

			// Create an HTTP server.
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				verifyClusterPropagation(r, testCase.cluster)
				testCase.serverResponse(w)
			}))
			defer server.Close()

			req, err := http.NewRequest("GET", fmt.Sprintf("%s/Test/Me", server.URL), nil)
			require.NoError(t, err)

			resp, err := client.Do(req)
			if testCase.expectedErr != nil {
				require.Error(t, err)
				require.ErrorContains(t, err, testCase.expectedErr.Error())
			} else {
				require.NoError(t, err)
				body, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				resp.Body.Close()
				require.Equal(t, testCase.expectedResponse, string(body))
			}

			err = testutil.GatherAndCompare(reg, strings.NewReader(testCase.expectedMetrics), "test_request_invalid_cluster_validation_labels_total")
			require.NoError(t, err)

			if testCase.expectedLogs == "" {
				require.Empty(t, buf.Bytes())
			} else {
				require.True(t, bytes.Contains(buf.Bytes(), []byte(testCase.expectedLogs)))
			}
		})
	}
}

func TestClusterValidationMiddleware(t *testing.T) {
	testCases := map[string]struct {
		header             func(r *http.Request)
		serverCluster      string
		expectedStatusCode int
		expectedErrorMsg   string
		expectedLogs       string
		expectedMetrics    string
		shouldPanic        bool
	}{
		"empty server cluster makes ClusterUnaryServerInterceptor panic": {
			serverCluster: "",
			shouldPanic:   true,
		},
		"equal request and server clusters give no error": {
			header: func(r *http.Request) {
				r.Header[clusterutil.ClusterValidationLabelHeader] = []string{"cluster"}
			},
			serverCluster:      "cluster",
			expectedStatusCode: http.StatusOK,
		},
		"different request and server clusters give rise to an error if soft validation disabled": {
			header: func(r *http.Request) {
				r.Header[clusterutil.ClusterValidationLabelHeader] = []string{"wrong-cluster"}
			},
			serverCluster: "cluster",
			expectedLogs:  `level=warn msg="request with wrong cluster validation label" path=/Test/Me cluster_validation_label=cluster request_cluster_validation_label=wrong-cluster soft_validation=%v`,
			expectedMetrics: `
                                # HELP server_invalid_cluster_validation_label_requests_total Number of requests received by server with invalid cluster validation label.
                                # TYPE server_invalid_cluster_validation_label_requests_total counter
                                server_invalid_cluster_validation_label_requests_total{cluster_validation_label="cluster",method="/Test/Me",protocol="http",request_cluster_validation_label="wrong-cluster"} 1
			`,
			expectedStatusCode: http.StatusNetworkAuthenticationRequired,
			expectedErrorMsg:   `rejected request with wrong cluster validation label "wrong-cluster" - it should be "cluster"`,
		},
		"empty request cluster and non-empty server cluster give an error if soft validation disabled": {
			header: func(r *http.Request) {
				r.Header[clusterutil.ClusterValidationLabelHeader] = []string{""}
			},
			serverCluster: "cluster",
			expectedLogs:  `level=warn msg="request with no cluster validation label" path=/Test/Me cluster_validation_label=cluster soft_validation=%v`,
			expectedMetrics: `
                                # HELP server_invalid_cluster_validation_label_requests_total Number of requests received by server with invalid cluster validation label.
                                # TYPE server_invalid_cluster_validation_label_requests_total counter
                                server_invalid_cluster_validation_label_requests_total{cluster_validation_label="cluster",method="/Test/Me",protocol="http",request_cluster_validation_label=""} 1
			`,
			expectedStatusCode: http.StatusNetworkAuthenticationRequired,
			expectedErrorMsg:   `rejected request with empty cluster validation label - it should be "cluster"`,
		},
		"no request cluster and non-empty server cluster give an error if soft validation disabled": {
			serverCluster: "cluster",
			expectedLogs:  `level=warn msg="request with no cluster validation label" path=/Test/Me cluster_validation_label=cluster soft_validation=%v`,
			expectedMetrics: `
                                # HELP server_invalid_cluster_validation_label_requests_total Number of requests received by server with invalid cluster validation label.
                                # TYPE server_invalid_cluster_validation_label_requests_total counter
                                server_invalid_cluster_validation_label_requests_total{cluster_validation_label="cluster",method="/Test/Me",protocol="http",request_cluster_validation_label=""} 1
				`,
			expectedStatusCode: http.StatusNetworkAuthenticationRequired,
			expectedErrorMsg:   `rejected request with empty cluster validation label - it should be "cluster"`,
		},
		"if the incoming request contains more than one cluster label and soft validation is disabled an error is returned": {
			header: func(r *http.Request) {
				r.Header[clusterutil.ClusterValidationLabelHeader] = []string{"cluster", "another-cluster"}
			},
			serverCluster: "cluster",
			expectedLogs:  `level=warn msg="detected error during cluster validation label extraction" path=/Test/Me cluster_validation_label=cluster soft_validation=%v err="request header should contain exactly 1 value for key \"X-Cluster\", but it contains [cluster another-cluster]"`,
			expectedMetrics: `
                                # HELP server_invalid_cluster_validation_label_requests_total Number of requests received by server with invalid cluster validation label.
                                # TYPE server_invalid_cluster_validation_label_requests_total counter
                                server_invalid_cluster_validation_label_requests_total{cluster_validation_label="cluster",method="/Test/Me",protocol="http",request_cluster_validation_label=""} 1
			`,
			expectedStatusCode: http.StatusNetworkAuthenticationRequired,
			expectedErrorMsg:   `rejected request: request header should contain exactly 1 value for key "X-Cluster", but it contains [cluster another-cluster]`,
		},
	}
	for testName, testCase := range testCases {
		for _, softValidation := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s softValidation=%v", testName, softValidation), func(t *testing.T) {
				defer func() {
					r := recover()
					require.Equal(t, testCase.shouldPanic, r != nil)
				}()
				buf := bytes.NewBuffer(nil)
				logger := createLogger(t, buf)
				reg := prometheus.NewPedanticRegistry()
				invalidClusterValidations := NewInvalidClusterValidations(reg)
				m := ClusterValidationMiddleware(testCase.serverCluster, nil, softValidation, invalidClusterValidations, logger)
				handler := Merge(m).Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusOK)
				}))

				recorder := httptest.NewRecorder()
				req, err := http.NewRequest("GET", "/Test/Me", nil)
				require.NoError(t, err)
				if testCase.header != nil {
					testCase.header(req)
				}
				handler.ServeHTTP(recorder, req)
				if softValidation {
					require.Equal(t, http.StatusOK, recorder.Code)
				} else {
					require.Equal(t, testCase.expectedStatusCode, recorder.Code)
					if recorder.Code != http.StatusOK {
						require.Equal(t, "application/json", recorder.Header().Get("Content-Type"))
						var clusterValidationErr clusterValidationError
						err = json.Unmarshal(recorder.Body.Bytes(), &clusterValidationErr)
						require.NoError(t, err)
						require.Equal(t, testCase.expectedErrorMsg, clusterValidationErr.ClusterValidationErrorMessage)
					}
				}
				if testCase.expectedLogs != "" {
					require.True(t, bytes.Contains(buf.Bytes(), []byte(fmt.Sprintf(testCase.expectedLogs, softValidation))))
				}
				err = testutil.GatherAndCompare(reg, strings.NewReader(testCase.expectedMetrics), "server_invalid_cluster_validation_label_requests_total")
				require.NoError(t, err)
			})
		}
	}
}

func TestClusterValidationMiddlewareWithExcludedPaths(t *testing.T) {
	testCases := map[string]struct {
		softValidation       bool
		requestPath          string
		excludedPaths        []string
		expectedStatusCode   int
		expectedErrorMessage string
		expectedMetrics      string
	}{
		"when soft validation is enabled and request path is excluded no error is returned": {
			softValidation:       true,
			requestPath:          "Test/Me",
			excludedPaths:        []string{"Exclude/Me", "Test/Me", "Do/Not/Test/Me"},
			expectedStatusCode:   http.StatusOK,
			expectedErrorMessage: "",
		},
		"when soft validation is disabled and request path is excluded no error is returned": {
			softValidation:       false,
			requestPath:          "Test/Me",
			excludedPaths:        []string{"Exclude/Me", "Test/Me", "Do/Not/Test/Me"},
			expectedStatusCode:   http.StatusOK,
			expectedErrorMessage: "",
		},
		"when soft validation is disabled and request path is implicitly excluded no error is returned": {
			softValidation:       false,
			requestPath:          "metrics",
			excludedPaths:        []string{"Exclude/Me", "Test/Me", "Do/Not/Test/Me"},
			expectedStatusCode:   http.StatusOK,
			expectedErrorMessage: "",
		},
		"when soft validation is enabled and request path is not excluded no error is returned": {
			softValidation:       true,
			requestPath:          "Test/Me",
			excludedPaths:        []string{"Exclude/Me", "Do/Not/Test/Me"},
			expectedStatusCode:   http.StatusOK,
			expectedErrorMessage: "",
			expectedMetrics: `
                                # HELP server_invalid_cluster_validation_label_requests_total Number of requests received by server with invalid cluster validation label.
                                # TYPE server_invalid_cluster_validation_label_requests_total counter
                                server_invalid_cluster_validation_label_requests_total{cluster_validation_label="server-cluster",method="/Test/Me",protocol="http",request_cluster_validation_label="client-cluster"} 1
			`,
		},
		"when soft validation is disabled and request path is not excluded an error is returned": {
			softValidation:       false,
			requestPath:          "Test/Me",
			excludedPaths:        []string{"Exclude/Me", "Do/Not/Test/Me"},
			expectedStatusCode:   http.StatusNetworkAuthenticationRequired,
			expectedErrorMessage: `rejected request with wrong cluster validation label "client-cluster" - it should be "server-cluster"`,
			expectedMetrics: `
                                # HELP server_invalid_cluster_validation_label_requests_total Number of requests received by server with invalid cluster validation label.
                                # TYPE server_invalid_cluster_validation_label_requests_total counter
                                server_invalid_cluster_validation_label_requests_total{cluster_validation_label="server-cluster",method="/Test/Me",protocol="http",request_cluster_validation_label="client-cluster"} 1
			`,
		},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			invalidClusterValidations := NewInvalidClusterValidations(reg)
			m := ClusterValidationMiddleware("server-cluster", testCase.excludedPaths, testCase.softValidation, invalidClusterValidations, log.NewNopLogger())
			handler := Merge(m).Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))

			recorder := httptest.NewRecorder()
			req, err := http.NewRequest("GET", fmt.Sprintf("http://localhost:8080/%s", testCase.requestPath), nil)
			require.NoError(t, err)
			req.Header[clusterutil.ClusterValidationLabelHeader] = []string{"client-cluster"}

			handler.ServeHTTP(recorder, req)
			require.Equal(t, testCase.expectedStatusCode, recorder.Code)
			if recorder.Code != http.StatusOK {
				var clusterValidationErr clusterValidationError
				err = json.Unmarshal(recorder.Body.Bytes(), &clusterValidationErr)
				require.NoError(t, err)
				require.Equal(t, testCase.expectedErrorMessage, clusterValidationErr.ClusterValidationErrorMessage)
			}
			err = testutil.GatherAndCompare(reg, strings.NewReader(testCase.expectedMetrics), "server_invalid_cluster_validation_label_requests_total")
			require.NoError(t, err)
		})
	}
}
