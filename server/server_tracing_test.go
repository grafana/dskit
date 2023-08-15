package server

import (
	"bytes"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/gorilla/mux"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-client-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/grafana/dskit/httpgrpc"
	httpgrpcServer "github.com/grafana/dskit/httpgrpc/server"
	"github.com/grafana/dskit/log"
	"github.com/grafana/dskit/middleware"
)

// testObserver implements jaeger.ContribSpanObserver to collect an emitted span's attributes
type testSpanObserver struct {
	OpName     string
	Tags       map[string]interface{}
	References []opentracing.SpanReference
}

func (tso testSpanObserver) OnSetOperationName(operationName string) {
	tso.OpName = operationName //nolint:staticcheck // SA4005
}
func (tso testSpanObserver) OnSetTag(key string, value interface{}) {
	tso.Tags[key] = value
}
func (tso testSpanObserver) OnFinish(_ opentracing.FinishOptions) {}

// testObserver implements jaeger.ContribObserver to collect SpanObservers as they are emitted
type testObserver struct {
	SpanObservers []testSpanObserver
}

func (to *testObserver) OnStartSpan(
	_ opentracing.Span,
	operationName string,
	options opentracing.StartSpanOptions,
) (jaeger.ContribSpanObserver, bool) {
	spanObserver := testSpanObserver{
		OpName:     operationName,
		Tags:       options.Tags,
		References: options.References,
	}
	to.SpanObservers = append(to.SpanObservers, spanObserver)
	return spanObserver, true
}

// assertTracingSpans tests if expected spans and tags were recorded by tracing SpanObservers
func assertTracingSpans(
	t *testing.T,
	spanObservers []testSpanObserver,
	expectedTagsByOpName map[string]map[string]string,
) {
	var observedSpanOpNames []string
	for _, spanObserver := range spanObservers {
		// assert expected tag values for observed span, if any
		if expectedTags, ok := expectedTagsByOpName[spanObserver.OpName]; ok {
			for tagKey, tagValue := range spanObserver.Tags {
				if expectedTagValue, ok := expectedTags[tagKey]; ok {
					require.Equal(t, expectedTagValue, tagValue)
				}
			}
		}
		// collect observed span operation names
		observedSpanOpNames = append(observedSpanOpNames, spanObserver.OpName)
	}
	for opName := range expectedTagsByOpName {
		// assert all expected operations were observed
		require.Contains(t, observedSpanOpNames, opName)
	}
}

func TestHTTPGRPCTracing(t *testing.T) {

	httpPort := 9099
	httpAddress := "127.0.0.1"

	httpMethod := http.MethodGet

	helloRouteName := "hello"
	helloRouteTmpl := "/hello"
	helloRouteURLRaw := fmt.Sprintf("http://%s:%d/hello", httpAddress, httpPort)
	helloRouteURL, err := url.Parse(helloRouteURLRaw)
	require.NoError(t, err)

	helloPathParamRouteTmpl := "/hello/{pathParam}"
	helloPathParamRouteURLRaw := fmt.Sprintf("http://%s:%d/hello/world", httpAddress, httpPort)
	helloPathParamRouteURL, err := url.Parse(helloPathParamRouteURLRaw)
	require.NoError(t, err)

	// extracted route names or path templates are converted to a Prometheus-compatible label value
	expectedHelloRouteLabel := middleware.MakeLabelValue(helloRouteName)
	expectedHelloPathParamRouteLabel := middleware.MakeLabelValue(helloPathParamRouteTmpl)

	// define expected span tags for the HTTP handler spans, so we can assert that they are the same
	// regardless of whether the request is routed through the gRPC Handle method first
	expectedOpNameHelloHTTPSpan := "HTTP " + httpMethod + " - " + expectedHelloRouteLabel
	expectedTagsHelloHTTPSpan := map[string]string{
		string(ext.Component):  "net/http",
		string(ext.HTTPUrl):    helloRouteURL.Path,
		string(ext.HTTPMethod): httpMethod,
		"http.route":           helloRouteName,
	}
	expectedOpNameHelloPathParamHTTPSpan := "HTTP " + httpMethod + " - " + expectedHelloPathParamRouteLabel
	expectedTagsHelloPathParamHTTPSpan := map[string]string{
		string(ext.Component):  "net/http",
		string(ext.HTTPUrl):    helloPathParamRouteURL.Path,
		string(ext.HTTPMethod): httpMethod,
		"http.route":           expectedHelloPathParamRouteLabel,
	}

	tests := map[string]struct {
		useHTTPOverGRPC      bool
		routeName            string // leave blank for unnamed route tests
		routeTmpl            string
		routeLabel           string
		reqURL               string
		expectedTagsByOpName map[string]map[string]string
	}{
		"http over grpc: named route with no params in path template": {
			useHTTPOverGRPC: true,
			routeName:       helloRouteName,
			routeTmpl:       helloRouteTmpl,
			routeLabel:      expectedHelloRouteLabel,
			reqURL:          helloRouteURLRaw,
			expectedTagsByOpName: map[string]map[string]string{
				"/httpgrpc.HTTP/Handle": {
					string(ext.Component):  "gRPC",
					string(ext.HTTPUrl):    helloRouteURL.Path,
					string(ext.HTTPMethod): httpMethod,
					"http.route":           helloRouteName,
				},
				expectedOpNameHelloHTTPSpan: expectedTagsHelloHTTPSpan,
			},
		},
		"http direct request: named route with no params in path template": {
			useHTTPOverGRPC: false,
			routeName:       helloRouteName,
			routeTmpl:       helloRouteTmpl,
			routeLabel:      expectedHelloRouteLabel,
			reqURL:          helloRouteURLRaw,
			expectedTagsByOpName: map[string]map[string]string{
				expectedOpNameHelloHTTPSpan: expectedTagsHelloHTTPSpan,
			},
		},
		"http over grpc: unnamed route with params in path template": {
			useHTTPOverGRPC: true,
			routeName:       "",
			routeTmpl:       helloPathParamRouteTmpl,
			routeLabel:      expectedHelloPathParamRouteLabel,
			reqURL:          helloPathParamRouteURLRaw,
			expectedTagsByOpName: map[string]map[string]string{
				"/httpgrpc.HTTP/Handle": {
					string(ext.Component):  "gRPC",
					string(ext.HTTPUrl):    helloPathParamRouteURL.Path,
					string(ext.HTTPMethod): httpMethod,
					"http.route":           expectedHelloPathParamRouteLabel,
				},
				expectedOpNameHelloPathParamHTTPSpan: expectedTagsHelloPathParamHTTPSpan,
			},
		},
		"http direct request: unnamed route with params in path template": {
			useHTTPOverGRPC: false,
			routeName:       "",
			routeTmpl:       helloPathParamRouteTmpl,
			routeLabel:      expectedHelloPathParamRouteLabel,
			reqURL:          helloPathParamRouteURLRaw,
			expectedTagsByOpName: map[string]map[string]string{
				expectedOpNameHelloPathParamHTTPSpan: expectedTagsHelloPathParamHTTPSpan,
			},
		},
	}

	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {

			observer := testObserver{}
			tracer, closer := jaeger.NewTracer(
				"test",
				jaeger.NewConstSampler(true),
				jaeger.NewInMemoryReporter(),
				jaeger.TracerOptions.ContribObserver(&observer),
			)
			opentracing.SetGlobalTracer(tracer)

			var cfg Config
			cfg.HTTPListenAddress = httpAddress
			cfg.HTTPListenPort = httpPort
			cfg.GRPCListenAddress = httpAddress
			cfg.GRPCListenPort = 1234
			cfg.GPRCServerMaxRecvMsgSize = 4 * 1024 * 1024
			cfg.GRPCServerMaxSendMsgSize = 4 * 1024 * 1024
			cfg.Router = middleware.InitHTTPGRPCMiddleware(mux.NewRouter())
			cfg.MetricsNamespace = "testing_httpgrpc_tracing_" + middleware.MakeLabelValue(testName)
			var lvl log.Level
			require.NoError(t, lvl.Set("info"))
			cfg.LogLevel = lvl

			server, err := New(cfg)
			require.NoError(t, err)

			handlerFunc := func(w http.ResponseWriter, r *http.Request) {}
			if test.routeName != "" {
				// explicitly-named routes will be labeled using the provided route name
				server.HTTP.NewRoute().Name(test.routeName).Path(test.routeTmpl).HandlerFunc(handlerFunc)
			} else {
				// unnamed routes will be labeled with their registered path template
				server.HTTP.HandleFunc(test.routeTmpl, handlerFunc)
			}

			go func() {
				require.NoError(t, server.Run())
			}()

			target := server.GRPCListenAddr()
			conn, err := grpc.Dial(
				target.String(),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(4*1024*1024)),
			)
			require.NoError(t, err)
			client := httpgrpc.NewHTTPClient(conn)

			// emulateHTTPGRPCPRoxy mimics the usage of the Server type as a load balancing proxy,
			// wrapping http requests into gRPC requests to utilize gRPC load balancing features
			emulateHTTPGRPCPRoxy := func(
				client httpgrpc.HTTPClient, req *http.Request,
			) (*httpgrpc.HTTPResponse, error) {
				req.RequestURI = req.URL.String()
				grpcReq, err := httpgrpcServer.HTTPRequest(req)
				require.NoError(t, err)
				return client.Handle(req.Context(), grpcReq)
			}

			req, err := http.NewRequest(httpMethod, test.reqURL, bytes.NewReader([]byte{}))
			require.NoError(t, err)

			if test.useHTTPOverGRPC {
				// http-over-grpc will be routed through HTTPGRPCTracer.Wrap middleware
				_, err = emulateHTTPGRPCPRoxy(client, req)
				require.NoError(t, err)
			} else {
				// direct http requests will be routed through the default Tracer.Wrap HTTP middleware
				_, err = http.DefaultClient.Do(req)
				require.NoError(t, err)
			}

			assertTracingSpans(t, observer.SpanObservers, test.expectedTagsByOpName)

			conn.Close()
			server.Shutdown()
			closer.Close()
		})
	}
}
