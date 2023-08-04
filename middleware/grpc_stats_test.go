// Provenance-includes-location: https://github.com/weaveworks/common/blob/main/middleware/grpc_stats_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Weaveworks Ltd.

package middleware

import (
	"bytes"
	"context"
	"crypto/rand"
	"net"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/grafana/dskit/middleware/middleware_test"
)

func TestGrpcStats(t *testing.T) {
	reg := prometheus.NewRegistry()

	received := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "received_payload_bytes",
		Help:    "Size of received gRPC messages",
		Buckets: BodySizeBuckets,
	}, []string{"method", "route"})

	sent := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "sent_payload_bytes",
		Help:    "Size of sent gRPC",
		Buckets: BodySizeBuckets,
	}, []string{"method", "route"})

	inflightRequests := promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "inflight_requests",
		Help: "Current number of inflight requests.",
	}, []string{"method", "route"})

	stats := NewStatsHandler(received, sent, inflightRequests)

	serv := grpc.NewServer(grpc.StatsHandler(stats), grpc.MaxRecvMsgSize(10e6))
	defer serv.GracefulStop()

	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	grpc_health_v1.RegisterHealthServer(serv, health.NewServer())

	go func() {
		require.NoError(t, serv.Serve(listener))
	}()

	closed := false
	conn, err := grpc.Dial(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() {
		if !closed {
			require.NoError(t, conn.Close())
		}
	}()

	hc := grpc_health_v1.NewHealthClient(conn)

	// First request (empty).
	resp, err := hc.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.Status)

	// Second request, with large service name. This returns error, which doesn't count as "payload".
	_, err = hc.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{
		Service: generateString(8 * 1024 * 1024),
	})
	require.EqualError(t, err, "rpc error: code = NotFound desc = unknown service")

	err = testutil.GatherAndCompare(reg, bytes.NewBufferString(`
			# HELP received_payload_bytes Size of received gRPC messages
			# TYPE received_payload_bytes histogram
			received_payload_bytes_bucket{method="gRPC", route="/grpc.health.v1.Health/Check",le="1.048576e+06"} 1
			received_payload_bytes_bucket{method="gRPC", route="/grpc.health.v1.Health/Check",le="2.62144e+06"} 1
			received_payload_bytes_bucket{method="gRPC", route="/grpc.health.v1.Health/Check",le="5.24288e+06"} 1
			received_payload_bytes_bucket{method="gRPC", route="/grpc.health.v1.Health/Check",le="1.048576e+07"} 2
			received_payload_bytes_bucket{method="gRPC", route="/grpc.health.v1.Health/Check",le="2.62144e+07"} 2
			received_payload_bytes_bucket{method="gRPC", route="/grpc.health.v1.Health/Check",le="5.24288e+07"} 2
			received_payload_bytes_bucket{method="gRPC", route="/grpc.health.v1.Health/Check",le="1.048576e+08"} 2
			received_payload_bytes_bucket{method="gRPC", route="/grpc.health.v1.Health/Check",le="2.62144e+08"} 2
			received_payload_bytes_bucket{method="gRPC", route="/grpc.health.v1.Health/Check",le="+Inf"} 2
			received_payload_bytes_sum{method="gRPC", route="/grpc.health.v1.Health/Check"} 8.388623e+06
			received_payload_bytes_count{method="gRPC", route="/grpc.health.v1.Health/Check"} 2

			# HELP sent_payload_bytes Size of sent gRPC
			# TYPE sent_payload_bytes histogram
			sent_payload_bytes_bucket{method="gRPC", route="/grpc.health.v1.Health/Check",le="1.048576e+06"} 1
			sent_payload_bytes_bucket{method="gRPC", route="/grpc.health.v1.Health/Check",le="2.62144e+06"} 1
			sent_payload_bytes_bucket{method="gRPC", route="/grpc.health.v1.Health/Check",le="5.24288e+06"} 1
			sent_payload_bytes_bucket{method="gRPC", route="/grpc.health.v1.Health/Check",le="1.048576e+07"} 1
			sent_payload_bytes_bucket{method="gRPC", route="/grpc.health.v1.Health/Check",le="2.62144e+07"} 1
			sent_payload_bytes_bucket{method="gRPC", route="/grpc.health.v1.Health/Check",le="5.24288e+07"} 1
			sent_payload_bytes_bucket{method="gRPC", route="/grpc.health.v1.Health/Check",le="1.048576e+08"} 1
			sent_payload_bytes_bucket{method="gRPC", route="/grpc.health.v1.Health/Check",le="2.62144e+08"} 1
			sent_payload_bytes_bucket{method="gRPC", route="/grpc.health.v1.Health/Check",le="+Inf"} 1
			sent_payload_bytes_sum{method="gRPC", route="/grpc.health.v1.Health/Check"} 7
			sent_payload_bytes_count{method="gRPC", route="/grpc.health.v1.Health/Check"} 1
	`), "received_payload_bytes", "sent_payload_bytes")
	require.NoError(t, err)

	closed = true
	require.NoError(t, conn.Close())
}

func TestGrpcStatsStreaming(t *testing.T) {
	reg := prometheus.NewRegistry()

	received := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "received_payload_bytes",
		Help:    "Size of received gRPC messages",
		Buckets: BodySizeBuckets,
	}, []string{"method", "route"})

	sent := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "sent_payload_bytes",
		Help:    "Size of sent gRPC",
		Buckets: BodySizeBuckets,
	}, []string{"method", "route"})

	inflightRequests := promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "inflight_requests",
		Help: "Current number of inflight requests.",
	}, []string{"method", "route"})

	stats := NewStatsHandler(received, sent, inflightRequests)

	serv := grpc.NewServer(grpc.StatsHandler(stats), grpc.MaxSendMsgSize(10e6), grpc.MaxRecvMsgSize(10e6))
	defer serv.GracefulStop()

	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	middleware_test.RegisterEchoServerServer(serv, &halfEcho{log: t.Log})

	go func() {
		require.NoError(t, serv.Serve(listener))
	}()

	conn, err := grpc.Dial(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(10e6), grpc.MaxCallSendMsgSize(10e6)))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	fc := middleware_test.NewEchoServerClient(conn)

	s, err := fc.Process(context.Background())
	require.NoError(t, err)

	for ix := 0; ix < 5; ix++ {
		msg := &middleware_test.Msg{
			Body: []byte(generateString((ix + 1) * 1024 * 1024)),
		}

		t.Log("Client Sending", msg.Size())
		err = s.Send(msg)
		require.NoError(t, err)

		_, err := s.Recv()
		require.NoError(t, err)

		err = testutil.GatherAndCompare(reg, bytes.NewBufferString(`
			# HELP inflight_requests Current number of inflight requests.
			# TYPE inflight_requests gauge
			inflight_requests{method="gRPC", route="/middleware.EchoServer/Process"} 1
		`), "inflight_requests")
		require.NoError(t, err)
	}
	require.NoError(t, s.CloseSend())

	// Wait for inflight_requests to go to 0.
	timeout := 1 * time.Second
	sleep := timeout / 10

	for endTime := time.Now().Add(timeout); time.Now().Before(endTime); {
		err = testutil.GatherAndCompare(reg, bytes.NewBufferString(`
			# HELP inflight_requests Current number of inflight requests.
			# TYPE inflight_requests gauge
			inflight_requests{method="gRPC", route="/middleware.EchoServer/Process"} 0
		`), "inflight_requests")
		if err == nil {
			break
		}
		time.Sleep(sleep)
	}
	require.NoError(t, err)

	err = testutil.GatherAndCompare(reg, bytes.NewBufferString(`
			# HELP received_payload_bytes Size of received gRPC messages
			# TYPE received_payload_bytes histogram
			received_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="1.048576e+06"} 0
			received_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="2.62144e+06"} 2
			received_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="5.24288e+06"} 4
			received_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="1.048576e+07"} 5
			received_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="2.62144e+07"} 5
			received_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="5.24288e+07"} 5
			received_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="1.048576e+08"} 5
			received_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="2.62144e+08"} 5
			received_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="+Inf"} 5
			received_payload_bytes_sum{method="gRPC",route="/middleware.EchoServer/Process"} 1.5728689e+07
			received_payload_bytes_count{method="gRPC",route="/middleware.EchoServer/Process"} 5

			# HELP sent_payload_bytes Size of sent gRPC
			# TYPE sent_payload_bytes histogram
			sent_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="1.048576e+06"} 1
			sent_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="2.62144e+06"} 4
			sent_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="5.24288e+06"} 5
			sent_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="1.048576e+07"} 5
			sent_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="2.62144e+07"} 5
			sent_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="5.24288e+07"} 5
			sent_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="1.048576e+08"} 5
			sent_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="2.62144e+08"} 5
			sent_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="+Inf"} 5
			sent_payload_bytes_sum{method="gRPC",route="/middleware.EchoServer/Process"} 7.864367e+06
			sent_payload_bytes_count{method="gRPC",route="/middleware.EchoServer/Process"} 5
	`), "received_payload_bytes", "sent_payload_bytes")

	require.NoError(t, err)
}

type halfEcho struct {
	log func(args ...interface{})
}

func (f halfEcho) Process(server middleware_test.EchoServer_ProcessServer) error {
	for {
		msg, err := server.Recv()
		if err != nil {
			return err
		}

		// Half the body
		msg.Body = msg.Body[:len(msg.Body)/2]

		f.log("Server Sending", msg.Size())
		err = server.Send(msg)
		if err != nil {
			return err
		}
	}
}

func generateString(size int) string {
	// Use random bytes, to avoid compression.
	buf := make([]byte, size)
	_, err := rand.Read(buf)
	if err != nil {
		// Should not happen.
		panic(err)
	}

	// To avoid invalid UTF-8 sequences (which protobuf complains about), we cleanup the data a bit.
	for ix, b := range buf {
		if b < ' ' {
			b += ' '
		}
		b = b & 0x7f
		buf[ix] = b
	}
	return string(buf)
}
