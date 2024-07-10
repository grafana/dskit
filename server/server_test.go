// Provenance-includes-location: https://github.com/weaveworks/common/blob/main/server/server_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Weaveworks Ltd.

package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	gokit_log "github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	protobuf "github.com/golang/protobuf/ptypes/empty"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/log"
	"github.com/grafana/dskit/middleware"
)

type FakeServer struct{}

func (f FakeServer) FailWithError(_ context.Context, _ *protobuf.Empty) (*protobuf.Empty, error) {
	return nil, errors.New("test error")
}

func (f FakeServer) FailWithHTTPError(_ context.Context, req *FailWithHTTPErrorRequest) (*protobuf.Empty, error) {
	return nil, httpgrpc.Errorf(int(req.Code), strconv.Itoa(int(req.Code)))
}

func (f FakeServer) Succeed(_ context.Context, _ *protobuf.Empty) (*protobuf.Empty, error) {
	return &protobuf.Empty{}, nil
}

func (f FakeServer) Sleep(ctx context.Context, _ *protobuf.Empty) (*protobuf.Empty, error) {
	err := cancelableSleep(ctx, 10*time.Second)
	return &protobuf.Empty{}, err
}

func (f FakeServer) StreamSleep(_ *protobuf.Empty, stream FakeServer_StreamSleepServer) error {
	for x := 0; x < 100; x++ {
		time.Sleep(time.Second / 100.0)
		if err := stream.Send(&protobuf.Empty{}); err != nil {
			return err
		}
	}
	return nil
}

func cancelableSleep(ctx context.Context, sleep time.Duration) error {
	select {
	case <-time.After(sleep):
	case <-ctx.Done():
	}
	return ctx.Err()
}

func (f FakeServer) ReturnProxyProtoCallerIP(ctx context.Context, _ *protobuf.Empty) (*ProxyProtoIPResponse, error) {
	p, _ := peer.FromContext(ctx)
	ip, _, err := net.SplitHostPort(p.Addr.String())
	if err != nil {
		return nil, err
	}
	return &ProxyProtoIPResponse{
		IP: ip,
	}, nil
}

func TestTCPv4Network(t *testing.T) {
	var cfg Config
	setAutoAssignedPorts(NetworkTCPV4, &cfg)

	t.Run("http", func(t *testing.T) {
		var level log.Level
		require.NoError(t, level.Set("info"))
		cfg.LogLevel = level
		cfg.MetricsNamespace = "testing_http_tcp4"
		srv, err := New(cfg)
		require.NoError(t, err)

		errChan := make(chan error, 1)
		go func() {
			errChan <- srv.Run()
		}()

		require.NoError(t, srv.httpListener.Close())
		require.NotNil(t, <-errChan)

		// So that address is freed for further tests.
		srv.GRPC.Stop()
	})

	t.Run("grpc", func(t *testing.T) {
		cfg.MetricsNamespace = "testing_grpc_tcp4"
		srv, err := New(cfg)
		require.NoError(t, err)

		errChan := make(chan error, 1)
		go func() {
			errChan <- srv.Run()
		}()

		require.NoError(t, srv.grpcListener.Close())
		require.NotNil(t, <-errChan)
	})
}

// Ensure that http and grpc servers work with no overrides to config
// (except http port because an ordinary user can't bind to default port 80)
func TestDefaultAddresses(t *testing.T) {
	var cfg Config
	cfg.RegisterFlags(flag.NewFlagSet("", flag.ExitOnError))
	cfg.HTTPListenPort = 9090
	cfg.MetricsNamespace = "testing_addresses"

	server, err := New(cfg)
	require.NoError(t, err)

	fakeServer := FakeServer{}
	RegisterFakeServerServer(server.GRPC, fakeServer)

	server.HTTP.HandleFunc("/test", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(204)
	})

	go func() {
		require.NoError(t, server.Run())
	}()
	defer server.Shutdown()

	conn, err := grpc.Dial("localhost:9095", grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	empty := protobuf.Empty{}
	client := NewFakeServerClient(conn)
	_, err = client.Succeed(context.Background(), &empty)
	require.NoError(t, err)

	req, err := http.NewRequest("GET", "http://127.0.0.1:9090/test", nil)
	require.NoError(t, err)
	_, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
}

func TestErrorInstrumentationMiddleware(t *testing.T) {
	newRegistry := prometheus.NewRegistry()
	prometheus.DefaultRegisterer = newRegistry
	prometheus.DefaultGatherer = newRegistry

	var cfg Config
	cfg.RegisterFlags(flag.NewFlagSet("", flag.ExitOnError))
	setAutoAssignedPorts(DefaultNetwork, &cfg)

	server, err := New(cfg)
	require.NoError(t, err)

	fakeServer := FakeServer{}
	RegisterFakeServerServer(server.GRPC, fakeServer)

	server.HTTP.HandleFunc("/succeed", func(http.ResponseWriter, *http.Request) {
	})
	server.HTTP.HandleFunc("/error500", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(500)
	})
	server.HTTP.HandleFunc("/sleep10", func(_ http.ResponseWriter, r *http.Request) {
		_ = cancelableSleep(r.Context(), time.Second*10)
	})
	server.HTTP.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})

	go func() {
		require.NoError(t, server.Run())
	}()

	conn, err := grpc.Dial(server.GRPCListenAddr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	empty := protobuf.Empty{}
	client := NewFakeServerClient(conn)
	res, err := client.Succeed(context.Background(), &empty)
	require.NoError(t, err)
	require.EqualValues(t, &empty, res)

	res, err = client.FailWithError(context.Background(), &empty)
	require.Nil(t, res)
	require.Error(t, err)

	s, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, "test error", s.Message())

	res, err = client.FailWithHTTPError(context.Background(), &FailWithHTTPErrorRequest{Code: http.StatusPaymentRequired})
	require.Nil(t, res)
	errResp, ok := httpgrpc.HTTPResponseFromError(err)
	require.True(t, ok)
	require.Equal(t, int32(http.StatusPaymentRequired), errResp.Code)
	require.Equal(t, "402", string(errResp.Body))

	callThenCancel := func(f func(ctx context.Context) error) error {
		ctx, cancel := context.WithCancel(context.Background())
		errChan := make(chan error, 1)
		go func() {
			errChan <- f(ctx)
		}()
		time.Sleep(50 * time.Millisecond) // allow the call to reach the handler
		cancel()
		return <-errChan
	}

	err = callThenCancel(func(ctx context.Context) error {
		_, err = client.Sleep(ctx, &empty)
		return err
	})
	require.Error(t, err, context.Canceled)

	err = callThenCancel(func(ctx context.Context) error {
		_, err = client.StreamSleep(ctx, &empty)
		return err
	})
	require.NoError(t, err) // canceling a streaming fn doesn't generate an error

	// Now test the HTTP versions of the functions
	{
		req, err := http.NewRequest("GET", httpTarget(server, "/succeed"), nil)
		require.NoError(t, err)
		_, err = http.DefaultClient.Do(req)
		require.NoError(t, err)
	}
	{
		req, err := http.NewRequest("GET", httpTarget(server, "/error500"), nil)
		require.NoError(t, err)
		_, err = http.DefaultClient.Do(req)
		require.NoError(t, err)
	}
	{
		req, err := http.NewRequest("GET", httpTarget(server, "/notfound"), nil)
		require.NoError(t, err)
		_, err = http.DefaultClient.Do(req)
		require.NoError(t, err)
	}
	{
		req, err := http.NewRequest("GET", httpTarget(server, "/sleep10"), nil)
		require.NoError(t, err)
		err = callThenCancel(func(ctx context.Context) error {
			_, err = http.DefaultClient.Do(req.WithContext(ctx))
			return err
		})
		require.Error(t, err, context.Canceled)
	}

	require.NoError(t, conn.Close())
	server.Shutdown()

	metrics, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	statuses := map[string]string{}
	for _, family := range metrics {
		if *family.Name == "request_duration_seconds" {
			for _, metric := range family.Metric {
				var route, statusCode string
				for _, label := range metric.GetLabel() {
					switch label.GetName() {
					case "status_code":
						statusCode = label.GetValue()
					case "route":
						route = label.GetValue()
					}
				}
				statuses[route] = statusCode
			}
		}
	}
	require.Equal(t, map[string]string{
		"/server.FakeServer/FailWithError":     "error",
		"/server.FakeServer/FailWithHTTPError": "402",
		"/server.FakeServer/Sleep":             "cancel",
		"/server.FakeServer/StreamSleep":       "cancel",
		"/server.FakeServer/Succeed":           "success",
		"error500":                             "500",
		"sleep10":                              "200",
		"succeed":                              "200",
		"notfound":                             "404",
	}, statuses)
}

func TestHTTPInstrumentationMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	prometheus.DefaultRegisterer = reg
	prometheus.DefaultGatherer = reg

	var cfg Config
	cfg.RegisterFlags(flag.NewFlagSet("", flag.ExitOnError))
	setAutoAssignedPorts(DefaultNetwork, &cfg)

	server, err := New(cfg)
	require.NoError(t, err)

	server.HTTP.HandleFunc("/succeed", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("OK"))
	})
	server.HTTP.HandleFunc("/error500", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(500)
	})
	server.HTTP.HandleFunc("/sleep10", func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body) // Consume body, otherwise it's not counted.
		_ = cancelableSleep(r.Context(), time.Second*10)
	})

	go func() {
		require.NoError(t, server.Run())
	}()

	callThenCancel := func(f func(ctx context.Context) error) error {
		ctx, cancel := context.WithCancel(context.Background())
		errChan := make(chan error, 1)
		go func() {
			errChan <- f(ctx)
		}()
		time.Sleep(50 * time.Millisecond) // allow the call to reach the handler
		cancel()
		return <-errChan
	}

	// Now test the HTTP versions of the functions
	{
		req, err := http.NewRequest("GET", httpTarget(server, "/succeed"), nil)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, "OK", string(body))
	}
	{
		req, err := http.NewRequest("GET", httpTarget(server, "/error500"), nil)
		require.NoError(t, err)
		_, err = http.DefaultClient.Do(req)
		require.NoError(t, err)
	}
	{
		req, err := http.NewRequest("POST", httpTarget(server, "/sleep10"), bytes.NewReader([]byte("Body")))
		require.NoError(t, err)
		err = callThenCancel(func(ctx context.Context) error {
			_, err = http.DefaultClient.Do(req.WithContext(ctx))
			return err
		})
		require.Error(t, err, context.Canceled)
	}

	server.Shutdown()

	require.NoError(t, testutil.GatherAndCompare(prometheus.DefaultGatherer, bytes.NewBufferString(`
		# HELP inflight_requests Current number of inflight requests.
		# TYPE inflight_requests gauge
		inflight_requests{method="POST",route="sleep10"} 0
		inflight_requests{method="GET",route="succeed"} 0
       	inflight_requests{method="GET",route="error500"} 0

		# HELP request_message_bytes Size (in bytes) of messages received in the request.
		# TYPE request_message_bytes histogram
		request_message_bytes_bucket{method="GET",route="error500",le="1.048576e+06"} 1
		request_message_bytes_bucket{method="GET",route="error500",le="2.62144e+06"} 1
		request_message_bytes_bucket{method="GET",route="error500",le="5.24288e+06"} 1
		request_message_bytes_bucket{method="GET",route="error500",le="1.048576e+07"} 1
		request_message_bytes_bucket{method="GET",route="error500",le="2.62144e+07"} 1
		request_message_bytes_bucket{method="GET",route="error500",le="5.24288e+07"} 1
		request_message_bytes_bucket{method="GET",route="error500",le="1.048576e+08"} 1
		request_message_bytes_bucket{method="GET",route="error500",le="2.62144e+08"} 1
		request_message_bytes_bucket{method="GET",route="error500",le="+Inf"} 1
		request_message_bytes_sum{method="GET",route="error500"} 0
		request_message_bytes_count{method="GET",route="error500"} 1

		request_message_bytes_bucket{method="POST",route="sleep10",le="1.048576e+06"} 1
		request_message_bytes_bucket{method="POST",route="sleep10",le="2.62144e+06"} 1
		request_message_bytes_bucket{method="POST",route="sleep10",le="5.24288e+06"} 1
		request_message_bytes_bucket{method="POST",route="sleep10",le="1.048576e+07"} 1
		request_message_bytes_bucket{method="POST",route="sleep10",le="2.62144e+07"} 1
		request_message_bytes_bucket{method="POST",route="sleep10",le="5.24288e+07"} 1
		request_message_bytes_bucket{method="POST",route="sleep10",le="1.048576e+08"} 1
		request_message_bytes_bucket{method="POST",route="sleep10",le="2.62144e+08"} 1
		request_message_bytes_bucket{method="POST",route="sleep10",le="+Inf"} 1
		request_message_bytes_sum{method="POST",route="sleep10"} 4
		request_message_bytes_count{method="POST",route="sleep10"} 1

		request_message_bytes_bucket{method="GET",route="succeed",le="1.048576e+06"} 1
		request_message_bytes_bucket{method="GET",route="succeed",le="2.62144e+06"} 1
		request_message_bytes_bucket{method="GET",route="succeed",le="5.24288e+06"} 1
		request_message_bytes_bucket{method="GET",route="succeed",le="1.048576e+07"} 1
		request_message_bytes_bucket{method="GET",route="succeed",le="2.62144e+07"} 1
		request_message_bytes_bucket{method="GET",route="succeed",le="5.24288e+07"} 1
		request_message_bytes_bucket{method="GET",route="succeed",le="1.048576e+08"} 1
		request_message_bytes_bucket{method="GET",route="succeed",le="2.62144e+08"} 1
		request_message_bytes_bucket{method="GET",route="succeed",le="+Inf"} 1
		request_message_bytes_sum{method="GET",route="succeed"} 0
		request_message_bytes_count{method="GET",route="succeed"} 1

		# HELP response_message_bytes Size (in bytes) of messages sent in response.
		# TYPE response_message_bytes histogram
		response_message_bytes_bucket{method="GET",route="error500",le="1.048576e+06"} 1
		response_message_bytes_bucket{method="GET",route="error500",le="2.62144e+06"} 1
		response_message_bytes_bucket{method="GET",route="error500",le="5.24288e+06"} 1
		response_message_bytes_bucket{method="GET",route="error500",le="1.048576e+07"} 1
		response_message_bytes_bucket{method="GET",route="error500",le="2.62144e+07"} 1
		response_message_bytes_bucket{method="GET",route="error500",le="5.24288e+07"} 1
		response_message_bytes_bucket{method="GET",route="error500",le="1.048576e+08"} 1
		response_message_bytes_bucket{method="GET",route="error500",le="2.62144e+08"} 1
		response_message_bytes_bucket{method="GET",route="error500",le="+Inf"} 1
		response_message_bytes_sum{method="GET",route="error500"} 0
		response_message_bytes_count{method="GET",route="error500"} 1

		response_message_bytes_bucket{method="POST",route="sleep10",le="1.048576e+06"} 1
		response_message_bytes_bucket{method="POST",route="sleep10",le="2.62144e+06"} 1
		response_message_bytes_bucket{method="POST",route="sleep10",le="5.24288e+06"} 1
		response_message_bytes_bucket{method="POST",route="sleep10",le="1.048576e+07"} 1
		response_message_bytes_bucket{method="POST",route="sleep10",le="2.62144e+07"} 1
		response_message_bytes_bucket{method="POST",route="sleep10",le="5.24288e+07"} 1
		response_message_bytes_bucket{method="POST",route="sleep10",le="1.048576e+08"} 1
		response_message_bytes_bucket{method="POST",route="sleep10",le="2.62144e+08"} 1
		response_message_bytes_bucket{method="POST",route="sleep10",le="+Inf"} 1
		response_message_bytes_sum{method="POST",route="sleep10"} 0
		response_message_bytes_count{method="POST",route="sleep10"} 1

		response_message_bytes_bucket{method="GET",route="succeed",le="1.048576e+06"} 1
		response_message_bytes_bucket{method="GET",route="succeed",le="2.62144e+06"} 1
		response_message_bytes_bucket{method="GET",route="succeed",le="5.24288e+06"} 1
		response_message_bytes_bucket{method="GET",route="succeed",le="1.048576e+07"} 1
		response_message_bytes_bucket{method="GET",route="succeed",le="2.62144e+07"} 1
		response_message_bytes_bucket{method="GET",route="succeed",le="5.24288e+07"} 1
		response_message_bytes_bucket{method="GET",route="succeed",le="1.048576e+08"} 1
		response_message_bytes_bucket{method="GET",route="succeed",le="2.62144e+08"} 1
		response_message_bytes_bucket{method="GET",route="succeed",le="+Inf"} 1
		response_message_bytes_sum{method="GET",route="succeed"} 2
		response_message_bytes_count{method="GET",route="succeed"} 1

		# HELP tcp_connections Current number of accepted TCP connections.
		# TYPE tcp_connections gauge
		tcp_connections{protocol="http"} 0
		tcp_connections{protocol="grpc"} 0
	`), "request_message_bytes", "response_message_bytes", "inflight_requests", "tcp_connections"))
}

func TestRunReturnsError(t *testing.T) {
	var cfg Config
	setAutoAssignedPorts(DefaultNetwork, &cfg)

	t.Run("http", func(t *testing.T) {
		cfg.MetricsNamespace = "testing_http"
		var level log.Level
		require.NoError(t, level.Set("info"))
		cfg.LogLevel = level
		srv, err := New(cfg)
		require.NoError(t, err)

		errChan := make(chan error, 1)
		go func() {
			errChan <- srv.Run()
		}()

		require.NoError(t, srv.httpListener.Close())
		require.NotNil(t, <-errChan)

		// So that address is freed for further tests.
		srv.GRPC.Stop()
	})

	t.Run("grpc", func(t *testing.T) {
		cfg.MetricsNamespace = "testing_grpc"
		srv, err := New(cfg)
		require.NoError(t, err)

		errChan := make(chan error, 1)
		go func() {
			errChan <- srv.Run()
		}()

		require.NoError(t, srv.grpcListener.Close())
		require.NotNil(t, <-errChan)
	})
}

// Test to see what the logging of a 500 error looks like
func TestMiddlewareLogging(t *testing.T) {
	var level log.Level
	require.NoError(t, level.Set("info"))
	cfg := Config{
		HTTPMiddleware:                []middleware.Interface{middleware.Log{Log: log.Global()}},
		MetricsNamespace:              "testing_logging",
		LogLevel:                      level,
		DoNotAddDefaultHTTPMiddleware: true,
		Router:                        &mux.Router{},
	}
	setAutoAssignedPorts(DefaultNetwork, &cfg)

	server, err := New(cfg)
	require.NoError(t, err)

	server.HTTP.HandleFunc("/error500", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(500)
	})

	go func() {
		require.NoError(t, server.Run())
	}()
	defer server.Shutdown()

	req, err := http.NewRequest("GET", httpTarget(server, "/error500"), nil)
	require.NoError(t, err)
	_, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
}

func TestTLSServer(t *testing.T) {
	var level log.Level
	require.NoError(t, level.Set("info"))

	certsDir := t.TempDir()
	cmd := exec.Command("bash", filepath.Join("certs", "genCerts.sh"), certsDir, "1")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))

	cfg := Config{
		HTTPTLSConfig: TLSConfig{
			TLSCertPath: filepath.Join(certsDir, "server.crt"),
			TLSKeyPath:  filepath.Join(certsDir, "server.key"),
			ClientAuth:  "RequireAndVerifyClientCert",
			ClientCAs:   filepath.Join(certsDir, "root.crt"),
		},
		GRPCTLSConfig: TLSConfig{
			TLSCertPath: filepath.Join(certsDir, "server.crt"),
			TLSKeyPath:  filepath.Join(certsDir, "server.key"),
			ClientAuth:  "VerifyClientCertIfGiven",
			ClientCAs:   filepath.Join(certsDir, "root.crt"),
		},
		MetricsNamespace: "testing_tls",
		LogLevel:         level,
	}
	setAutoAssignedPorts(DefaultNetwork, &cfg)

	server, err := New(cfg)
	require.NoError(t, err)

	server.HTTP.HandleFunc("/testhttps", func(w http.ResponseWriter, _ *http.Request) {
		_, err := w.Write([]byte("Hello World!"))
		require.NoError(t, err)
	})

	fakeServer := FakeServer{}
	RegisterFakeServerServer(server.GRPC, fakeServer)

	go func() {
		require.NoError(t, server.Run())
	}()
	defer server.Shutdown()

	clientCert, err := tls.LoadX509KeyPair(filepath.Join(certsDir, "client.crt"), filepath.Join(certsDir, "client.key"))
	require.NoError(t, err)

	caCert, err := os.ReadFile(cfg.HTTPTLSConfig.ClientCAs)
	require.NoError(t, err)

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
		Certificates:       []tls.Certificate{clientCert},
		RootCAs:            caCertPool,
	}
	tr := &http.Transport{
		TLSClientConfig: tlsConfig,
	}

	client := &http.Client{Transport: tr}
	res, err := client.Get(httpsTarget(server, "/testhttps"))
	require.NoError(t, err)
	defer res.Body.Close()

	require.Equal(t, res.StatusCode, http.StatusOK)

	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	expected := []byte("Hello World!")
	require.Equal(t, expected, body)

	conn, err := grpc.Dial(server.GRPCListenAddr().String(), grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	require.NoError(t, err)
	defer conn.Close()

	empty := protobuf.Empty{}
	grpcClient := NewFakeServerClient(conn)
	grpcRes, err := grpcClient.Succeed(context.Background(), &empty)
	require.NoError(t, err)
	require.EqualValues(t, &empty, grpcRes)
}

func TestTLSServerWithInlineCerts(t *testing.T) {
	var level log.Level
	require.NoError(t, level.Set("info"))

	certsDir := t.TempDir()
	cmd := exec.Command("bash", filepath.Join("certs", "genCerts.sh"), certsDir, "1")
	err := cmd.Run()
	require.NoError(t, err)

	cert, err := os.ReadFile(filepath.Join(certsDir, "server.crt"))
	require.NoError(t, err)

	key, err := os.ReadFile(filepath.Join(certsDir, "server.key"))
	require.NoError(t, err)

	clientCAs, err := os.ReadFile(filepath.Join(certsDir, "root.crt"))
	require.NoError(t, err)

	cfg := Config{
		HTTPTLSConfig: TLSConfig{
			TLSCert:       string(cert),
			TLSKey:        config.Secret(key),
			ClientAuth:    "RequireAndVerifyClientCert",
			ClientCAsText: string(clientCAs),
		},
		GRPCTLSConfig: TLSConfig{
			TLSCert:       string(cert),
			TLSKey:        config.Secret(key),
			ClientAuth:    "VerifyClientCertIfGiven",
			ClientCAsText: string(clientCAs),
		},
		MetricsNamespace: "testing_tls_certs_inline",
		LogLevel:         level,
	}
	setAutoAssignedPorts(DefaultNetwork, &cfg)

	server, err := New(cfg)
	defer server.Shutdown()

	require.NoError(t, err)

	server.HTTP.HandleFunc("/testhttps", func(w http.ResponseWriter, _ *http.Request) {
		_, err := w.Write([]byte("Hello World!"))
		require.NoError(t, err)
	})

	fakeServer := FakeServer{}
	RegisterFakeServerServer(server.GRPC, fakeServer)

	go func() {
		require.NoError(t, server.Run())
	}()

	clientCert, err := tls.LoadX509KeyPair(filepath.Join(certsDir, "client.crt"), filepath.Join(certsDir, "client.key"))
	require.NoError(t, err)

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(clientCAs)

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
		Certificates:       []tls.Certificate{clientCert},
		RootCAs:            caCertPool,
	}
	tr := &http.Transport{
		TLSClientConfig: tlsConfig,
	}

	client := &http.Client{Transport: tr}
	res, err := client.Get(httpsTarget(server, "/testhttps"))
	require.NoError(t, err)
	defer res.Body.Close()

	require.Equal(t, res.StatusCode, http.StatusOK)

	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	expected := []byte("Hello World!")
	require.Equal(t, expected, body)

	conn, err := grpc.Dial(server.GRPCListenAddr().String(), grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	require.NoError(t, err)
	defer conn.Close()

	empty := protobuf.Empty{}
	grpcClient := NewFakeServerClient(conn)
	grpcRes, err := grpcClient.Succeed(context.Background(), &empty)
	require.NoError(t, err)
	require.EqualValues(t, &empty, grpcRes)
}

type FakeLogger struct {
	logger gokit_log.Logger
	buf    *bytes.Buffer
}

func newFakeLogger() *FakeLogger {
	buf := bytes.NewBuffer(nil)
	log := log.NewGoKitWithWriter(log.LogfmtFormat, buf)
	return &FakeLogger{
		logger: log,
		buf:    buf,
	}
}

func (f *FakeLogger) Log(keyvals ...interface{}) error {
	return f.logger.Log(keyvals...)
}

func (f *FakeLogger) assertContains(t *testing.T, content string) {
	require.True(t, bytes.Contains(f.buf.Bytes(), []byte(content)))
}

func (f *FakeLogger) assertNotContains(t *testing.T, content string) {
	require.False(t, bytes.Contains(f.buf.Bytes(), []byte(content)))
}

func TestLogSourceIPs(t *testing.T) {
	var level log.Level
	require.NoError(t, level.Set("info"))
	cfg := Config{
		HTTPMiddleware:   []middleware.Interface{middleware.Log{Log: log.Global()}},
		MetricsNamespace: "testing_mux",
		LogLevel:         level,
		LogSourceIPs:     true,
	}
	setAutoAssignedPorts(DefaultNetwork, &cfg)

	startServer := func(cfg Config) *Server {
		prometheus.DefaultRegisterer = prometheus.NewRegistry()
		server, err := New(cfg)
		require.NoError(t, err)

		server.HTTP.HandleFunc("/error500", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(500)
		})

		go func() {
			require.NoError(t, server.Run())
		}()

		return server
	}

	t.Run("without PROXY protocol", func(t *testing.T) {
		logger := newFakeLogger()
		cfg.Log = logger

		server := startServer(cfg)
		defer server.Shutdown()

		logger.assertNotContains(t, "sourceIPs")

		req, err := http.NewRequest("GET", httpTarget(server, "/error500"), nil)
		require.NoError(t, err)
		_, err = http.DefaultClient.Do(req)
		require.NoError(t, err)

		logger.assertContains(t, "sourceIPs=127.0.0.1")
	})

	t.Run("with PROXY protocol", func(t *testing.T) {
		logger := newFakeLogger()
		cfg.Log = logger
		cfg.ProxyProtocolEnabled = true

		server := startServer(cfg)
		defer server.Shutdown()

		logger.assertNotContains(t, "sourceIPs")

		fakeSourceIP := "1.2.3.4"
		proxyHeader := fmt.Sprintf("PROXY TCP4 %s 192.168.0.1 51234 80\r\n", fakeSourceIP)
		client := &http.Client{
			Transport: &http.Transport{
				DialContext: proxyDialer(proxyHeader),
			},
		}

		req, err := http.NewRequest("GET", httpTarget(server, "/error500"), nil)
		require.NoError(t, err)
		_, err = client.Do(req)
		require.NoError(t, err)

		logger.assertContains(t, fmt.Sprintf("sourceIPs=%s", fakeSourceIP))
	})
}

func TestStopWithDisabledSignalHandling(t *testing.T) {
	var level log.Level
	require.NoError(t, level.Set("info"))
	cfg := Config{
		LogLevel: level,
	}
	setAutoAssignedPorts(DefaultNetwork, &cfg)

	var test = func(t *testing.T, metricsNamespace string, handler SignalHandler) {
		cfg.SignalHandler = handler
		cfg.MetricsNamespace = metricsNamespace
		srv, err := New(cfg)
		require.NoError(t, err)

		errChan := make(chan error, 1)
		go func() {
			errChan <- srv.Run()
		}()

		srv.Stop()
		require.Nil(t, <-errChan)

		// So that addresses is freed for further tests.
		srv.Shutdown()
	}

	t.Run("signals_enabled", func(t *testing.T) {
		test(t, "signals_enabled", nil)
	})

	t.Run("signals_disabled", func(t *testing.T) {
		test(t, "signals_disabled", dummyHandler{quit: make(chan struct{})})
	})
}

type proxyProtocolConn struct {
	net.Conn
	proxyHeaderWritten bool
	proxyHeader        []byte
}

func (pc *proxyProtocolConn) Write(b []byte) (int, error) {
	if !pc.proxyHeaderWritten {
		_, err := pc.Conn.Write(pc.proxyHeader)
		if err != nil {
			return 0, err
		}
		pc.proxyHeaderWritten = true
	}
	return pc.Conn.Write(b)
}

func proxyDialer(proxyHeader string) func(context.Context, string, string) (net.Conn, error) {
	return func(_ context.Context, network string, addr string) (net.Conn, error) {
		conn, err := net.Dial(network, addr)
		if err != nil {
			return nil, err
		}
		return &proxyProtocolConn{
			Conn:               conn,
			proxyHeader:        []byte(proxyHeader),
			proxyHeaderWritten: false,
		}, nil
	}
}

func TestHttpOverProxyProtocol(t *testing.T) {
	prometheus.DefaultRegisterer = prometheus.NewRegistry()

	var cfg Config
	cfg.RegisterFlags(flag.NewFlagSet("", flag.ExitOnError))
	cfg.ProxyProtocolEnabled = true
	setAutoAssignedPorts(DefaultNetwork, &cfg)

	server, err := New(cfg)
	require.NoError(t, err)
	defer server.Shutdown()

	server.HTTP.HandleFunc("/test-proxy-proto", func(w http.ResponseWriter, r *http.Request) {
		ip, _, err := net.SplitHostPort(r.RemoteAddr)
		require.NoError(t, err)
		_, err = w.Write([]byte(ip))
		require.NoError(t, err)
	})

	go func() {
		require.NoError(t, server.Run())
	}()

	t.Run("good PROXY header", func(t *testing.T) {
		fakeSourceIP := "1.2.3.4"
		proxyHeader := fmt.Sprintf("PROXY TCP4 %s 192.168.0.1 51234 80\r\n", fakeSourceIP)
		client := &http.Client{
			Transport: &http.Transport{
				DialContext: proxyDialer(proxyHeader),
			},
		}

		res, err := client.Get(httpTarget(server, "/test-proxy-proto"))
		require.NoError(t, err)
		defer res.Body.Close()
		require.Equal(t, res.StatusCode, http.StatusOK)

		body, err := io.ReadAll(res.Body)
		require.NoError(t, err)
		require.Equal(t, []byte(fakeSourceIP), body)
	})

	t.Run("malformed PROXY header", func(t *testing.T) {
		proxyHeader := "badPROXY TCP4 1.2.3.4 192.168.0.1 51234 80\r\n"
		client := &http.Client{
			Transport: &http.Transport{
				DialContext: proxyDialer(proxyHeader),
			},
		}

		res, err := client.Get(httpTarget(server, "/test-proxy-proto"))
		require.NoError(t, err)
		defer res.Body.Close()
		require.Equal(t, http.StatusBadRequest, res.StatusCode)
	})

	t.Run("no PROXY header", func(t *testing.T) {
		client := &http.Client{}
		res, err := client.Get(httpTarget(server, "/test-proxy-proto"))
		require.NoError(t, err)
		defer res.Body.Close()
		require.Equal(t, res.StatusCode, http.StatusOK)

		body, err := io.ReadAll(res.Body)
		require.NoError(t, err)
		require.NotEmpty(t, body)
	})
}

func TestGrpcOverProxyProtocol(t *testing.T) {
	prometheus.DefaultRegisterer = prometheus.NewRegistry()

	var cfg Config
	cfg.RegisterFlags(flag.NewFlagSet("", flag.ExitOnError))
	cfg.ProxyProtocolEnabled = true
	// Set this to 0 to have it choose a random port
	cfg.HTTPListenPort = 0

	fakeSourceIP := "1.2.3.4"

	// Custom dialer that sends a PROXY header
	customDialer := func(_ context.Context, address string) (net.Conn, error) {
		conn, err := net.Dial("tcp", address)
		if err != nil {
			return nil, err
		}

		proxyHeader := fmt.Sprintf("PROXY TCP4 %s 192.168.0.1 51234 80\r\n", fakeSourceIP)
		_, err = conn.Write([]byte(proxyHeader))
		if err != nil {
			conn.Close()
			return nil, err
		}

		return conn, nil
	}

	server, err := New(cfg)
	require.NoError(t, err)

	fakeServer := FakeServer{}
	RegisterFakeServerServer(server.GRPC, fakeServer)

	go func() {
		require.NoError(t, server.Run())
	}()
	defer server.Shutdown()

	conn, err := grpc.Dial("localhost:9095", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(customDialer))
	require.NoError(t, err)
	defer conn.Close()

	client := NewFakeServerClient(conn)
	res, err := client.ReturnProxyProtoCallerIP(context.Background(), &protobuf.Empty{})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, fakeSourceIP, res.IP)
}

type dummyHandler struct {
	quit chan struct{}
}

func (dh dummyHandler) Loop() {
	<-dh.quit
}

func (dh dummyHandler) Stop() {
	close(dh.quit)
}

func setAutoAssignedPorts(network string, cfg *Config) {
	cfg.HTTPListenNetwork = network
	cfg.HTTPListenAddress = "localhost"
	cfg.HTTPListenPort = 0
	cfg.GRPCListenNetwork = network
	cfg.GRPCListenAddress = "localhost"
	cfg.GRPCListenPort = 0
}

func httpTarget(srv *Server, path string) string {
	return fmt.Sprintf("http://%s%s", srv.HTTPListenAddr().String(), path)
}

func httpsTarget(srv *Server, path string) string {
	return fmt.Sprintf("https://%s%s", srv.HTTPListenAddr().String(), path)
}

func TestGrpcServerRecvBuffersConfigError(t *testing.T) {
	var cfg Config
	cfg.RegisterFlags(flag.NewFlagSet("", flag.ExitOnError))
	setAutoAssignedPorts(DefaultNetwork, &cfg)
	cfg.GRPCServerStatsTrackingEnabled = true
	cfg.GRPCServerRecvBufferPoolsEnabled = true
	cfg.MetricsNamespace = "testing_grpc_config_conflicts"

	_, err := New(cfg)
	require.Error(t, err)
}
