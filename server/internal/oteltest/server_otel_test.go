package oteltest

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/log"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/server"
)

var (
	spanExporter = tracetest.NewInMemoryExporter()
)

func init() {
	otel.SetTracerProvider(tracesdk.NewTracerProvider(tracesdk.WithSpanProcessor(tracesdk.NewSimpleSpanProcessor(spanExporter))))
}

func TestOTelTracing(t *testing.T) {
	httpPort := 9099
	httpAddress := "127.0.0.1"

	httpMethod := http.MethodGet

	helloRouteName := "hello"
	helloRouteTmpl := "/hello"
	hostport := net.JoinHostPort(httpAddress, strconv.Itoa(httpPort))
	helloRouteURLRaw := fmt.Sprintf("http://%s/hello", hostport)
	helloRouteURL, err := url.Parse(helloRouteURLRaw)
	require.NoError(t, err)

	helloPathParamRouteTmpl := "/hello/{pathParam}"
	helloPathParamRouteURLRaw := fmt.Sprintf("http://%s/hello/world", hostport)
	helloPathParamRouteURL, err := url.Parse(helloPathParamRouteURLRaw)
	require.NoError(t, err)

	// extracted route names or path templates are converted to a Prometheus-compatible label value
	expectedHelloRouteLabel := middleware.MakeLabelValue(helloRouteName)
	expectedHelloPathParamRouteLabel := middleware.MakeLabelValue(helloPathParamRouteTmpl)

	// define expected span attributes for the HTTP handler spans, so we can assert that they are the same
	// regardless of whether the request is routed through the gRPC Handle method first
	expectedOpNameHelloHTTPSpan := "HTTP " + httpMethod + " - " + expectedHelloRouteLabel
	expectedAttrsHelloHTTPSpan := []attribute.KeyValue{
		attribute.String("http.target", helloRouteURL.Path),
		attribute.String("http.method", httpMethod),
		attribute.String("http.route", helloRouteName),
	}
	expectedOpNameHelloPathParamHTTPSpan := "HTTP " + httpMethod + " - " + expectedHelloPathParamRouteLabel
	expectedAttrsHelloPathParamHTTPSpan := []attribute.KeyValue{
		attribute.String("http.target", helloPathParamRouteURL.Path),
		attribute.String("http.method", httpMethod),
		attribute.String("http.route", expectedHelloPathParamRouteLabel),
	}

	tests := map[string]struct {
		useHTTPOverGRPC              bool
		useOtherGRPC                 bool
		routeName                    string // leave blank for unnamed route tests
		routeTmpl                    string
		reqURL                       string
		reqHeaders                   map[string]string
		expectedAttributesByOpName   map[string][]attribute.KeyValue
		unexpectedAttributesByOpName map[string][]attribute.Key

		traceHeaders         bool
		excludedTraceHeaders []string
	}{
		"HTTP over gRPC: named route with no params in path template": {
			useHTTPOverGRPC: true,
			routeName:       helloRouteName,
			routeTmpl:       helloRouteTmpl,
			reqURL:          helloRouteURLRaw,
			expectedAttributesByOpName: map[string][]attribute.KeyValue{
				"httpgrpc.HTTP/Handle": {
					attribute.String("http.target", helloRouteURL.Path),
					attribute.String("http.method", httpMethod),
					attribute.String("http.route", helloRouteName),
				},
				expectedOpNameHelloHTTPSpan: expectedAttrsHelloHTTPSpan,
			},
		},
		"HTTP direct request: named route with no params in path template": {
			useHTTPOverGRPC: false,
			routeName:       helloRouteName,
			routeTmpl:       helloRouteTmpl,
			reqURL:          helloRouteURLRaw,
			expectedAttributesByOpName: map[string][]attribute.KeyValue{
				expectedOpNameHelloHTTPSpan: expectedAttrsHelloHTTPSpan,
			},
		},
		"HTTP over gRPC: unnamed route with params in path template": {
			useHTTPOverGRPC: true,
			routeName:       "",
			routeTmpl:       helloPathParamRouteTmpl,
			reqURL:          helloPathParamRouteURLRaw,
			expectedAttributesByOpName: map[string][]attribute.KeyValue{
				"httpgrpc.HTTP/Handle": {
					attribute.String("http.target", helloPathParamRouteURL.Path),
					attribute.String("http.method", httpMethod),
					attribute.String("http.route", expectedHelloPathParamRouteLabel),
				},
				expectedOpNameHelloPathParamHTTPSpan: expectedAttrsHelloPathParamHTTPSpan,
			},
		},
		"HTTP direct request: unnamed route with params in path template": {
			useHTTPOverGRPC: false,
			routeName:       "",
			routeTmpl:       helloPathParamRouteTmpl,
			reqURL:          helloPathParamRouteURLRaw,
			reqHeaders: map[string]string{
				"X-Unexpected-Header": "42",
			},
			expectedAttributesByOpName: map[string][]attribute.KeyValue{
				expectedOpNameHelloPathParamHTTPSpan: expectedAttrsHelloPathParamHTTPSpan,
			},
			unexpectedAttributesByOpName: map[string][]attribute.Key{
				expectedOpNameHelloPathParamHTTPSpan: {
					attribute.Key("http.header.X-Unexpected-Header"),
				},
			},
		},
		"HTTP direct request with headers tracing enabled": {
			useHTTPOverGRPC: false,
			routeName:       "",
			routeTmpl:       helloPathParamRouteTmpl,
			reqURL:          helloPathParamRouteURLRaw,
			reqHeaders: map[string]string{
				"X-Unexpected-Header": "42",
				"X-Excluded-Header":   "private",
				"Authorization":       "should always be excluded",
				"X-Access-Token":      "should also always be excluded",
				"X-Grafana-Id":        "also excluded",
			},
			expectedAttributesByOpName: map[string][]attribute.KeyValue{
				expectedOpNameHelloPathParamHTTPSpan: append(expectedAttrsHelloPathParamHTTPSpan,
					attribute.StringSlice("http.header.X-Unexpected-Header", []string{"42"}),
					attribute.String("http.header.X-Excluded-Header.present", "true"),
					attribute.String("http.header.Authorization.present", "true"),
					attribute.String("http.header.X-Access-Token.present", "true"),
					attribute.String("http.header.X-Grafana-Id.present", "true"),
				),
			},

			unexpectedAttributesByOpName: map[string][]attribute.Key{
				expectedOpNameHelloPathParamHTTPSpan: {
					attribute.Key("http.header.X-Excluded-Header"),
				},
			},
			traceHeaders:         true,
			excludedTraceHeaders: []string{"X-Excluded-Header"},
		},
		"gRPC direct request": {
			useOtherGRPC: true,
			expectedAttributesByOpName: map[string][]attribute.KeyValue{
				strings.TrimPrefix(grpc_health_v1.Health_Check_FullMethodName, "/"): {
					attribute.String("rpc.system", "grpc"),
				},
			},
		},
	}

	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			spanExporter.Reset()

			var lvl log.Level
			require.NoError(t, lvl.Set("info"))
			cfg := server.Config{
				HTTPListenAddress:        httpAddress,
				HTTPListenPort:           httpPort,
				GRPCListenAddress:        httpAddress,
				GRPCListenPort:           1234,
				GRPCServerMaxRecvMsgSize: 4 * 1024 * 1024,
				GRPCServerMaxSendMsgSize: 4 * 1024 * 1024,
				MetricsNamespace:         "testing_httpgrpc_tracing_" + middleware.MakeLabelValue(testName),
				LogLevel:                 lvl,
				Registerer:               prometheus.NewPedanticRegistry(),

				TraceRequestHeaders:            test.traceHeaders,
				TraceRequestExcludeHeadersList: strings.Join(test.excludedTraceHeaders, ","),
			}
			srv, err := server.New(cfg)
			require.NoError(t, err)

			grpc_health_v1.RegisterHealthServer(srv.GRPC, health.NewServer())

			handlerFunc := func(http.ResponseWriter, *http.Request) {}
			if test.routeName != "" {
				// explicitly-named routes will be labeled using the provided route name
				srv.HTTP.NewRoute().Name(test.routeName).Path(test.routeTmpl).HandlerFunc(handlerFunc)
			} else {
				// unnamed routes will be labeled with their registered path template
				srv.HTTP.HandleFunc(test.routeTmpl, handlerFunc)
			}

			go func() {
				require.NoError(t, srv.Run())
			}()
			t.Cleanup(srv.Shutdown)

			target := srv.GRPCListenAddr()
			conn, err := grpc.NewClient(
				target.String(),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(4*1024*1024)),
			)
			require.NoError(t, err)
			t.Cleanup(func() { _ = conn.Close() })

			// emulateHTTPGRPCPRoxy mimics the usage of the Server type as a load balancing proxy,
			// wrapping http requests into gRPC requests to utilize gRPC load balancing features
			emulateHTTPGRPCPRoxy := func(
				client httpgrpc.HTTPClient, req *http.Request,
			) (*httpgrpc.HTTPResponse, error) {
				req.RequestURI = req.URL.String()
				grpcReq, err := httpgrpc.FromHTTPRequest(req)
				require.NoError(t, err)
				return client.Handle(req.Context(), grpcReq)
			}

			req, err := http.NewRequest(httpMethod, test.reqURL, bytes.NewReader([]byte{}))
			require.NoError(t, err)
			for header, value := range test.reqHeaders {
				req.Header.Set(header, value)
			}

			if test.useHTTPOverGRPC {
				client := httpgrpc.NewHTTPClient(conn)
				// http-over-grpc will be routed through HTTPGRPCTracer.Wrap middleware
				_, err := emulateHTTPGRPCPRoxy(client, req)
				require.NoError(t, err)
			} else if test.useOtherGRPC {
				client := grpc_health_v1.NewHealthClient(conn)
				_, err := client.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
				require.NoError(t, err)
			} else {
				// direct http requests will be routed through the default Tracer.Wrap HTTP middleware
				_, err := http.DefaultClient.Do(req)
				require.NoError(t, err)
			}

			assertTracingSpans(t, test.expectedAttributesByOpName, test.unexpectedAttributesByOpName)
		})
	}
}

func assertTracingSpans(t *testing.T, expectedAttributesByOpName map[string][]attribute.KeyValue, unexpectedAttributesByOpName map[string][]attribute.Key) {
	allSpans := spanExporter.GetSpans()
	require.NotEmpty(t, allSpans, "expected spans to be recorded")

	for op, expectedAttributes := range expectedAttributesByOpName {
		sp, ok := findSpanByName(allSpans, op)
		require.True(t, ok, "expected span with operation name %q to be present, spans present were: %v", op, spanNames(allSpans))

		for _, attr := range expectedAttributes {
			val, ok := findAttributeByKey(sp.Attributes, attr.Key)
			require.True(t, ok, "expected attribute with key %q with value %q to be present on span %q, got attributes: %v", attr.Key, attr.Value.AsString(), op, sp.Attributes)
			require.Equal(t, attr.Value.AsString(), val.AsString(), "expected attribute value for key %q to match on span %q", attr.Key, op)
		}
	}

	for op, unexpectedAttributes := range unexpectedAttributesByOpName {
		sp, ok := findSpanByName(allSpans, op)
		require.True(t, ok, "expected span with operation name %q to be present, spans present were: %v", op, spanNames(allSpans))

		for _, attrKey := range unexpectedAttributes {
			_, ok := findAttributeByKey(sp.Attributes, attrKey)
			require.False(t, ok, "expected attribute with key %q to NOT be present on span %q, got attributes: %v", attrKey, op, sp.Attributes)
		}
	}
}

func spanNames(spans tracetest.SpanStubs) []string {
	names := make([]string, len(spans))
	for i, span := range spans {
		names[i] = span.Name
	}
	return names
}

func findAttributeByKey(attributes []attribute.KeyValue, key attribute.Key) (attribute.Value, bool) {
	for _, attr := range attributes {
		if attr.Key == key {
			return attr.Value, true
		}
	}
	return attribute.Value{}, false
}

func findSpanByName(spans tracetest.SpanStubs, op string) (tracetest.SpanStub, bool) {
	for _, span := range spans {
		if span.Name == op {
			return span, true
		}
	}
	return tracetest.SpanStub{}, false
}
