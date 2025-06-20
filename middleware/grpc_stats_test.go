// Provenance-includes-location: https://github.com/weaveworks/common/blob/main/middleware/grpc_stats_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Weaveworks Ltd.

package middleware

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	mathRand "math/rand"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
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

	stats := NewStatsHandler(reg, received, sent, inflightRequests, true)

	serv := grpc.NewServer(grpc.StatsHandler(stats), grpc.MaxRecvMsgSize(10e6))
	defer serv.GracefulStop()

	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	grpc_health_v1.RegisterHealthServer(serv, health.NewServer())

	go func() {
		require.NoError(t, serv.Serve(listener))
	}()

	closed := false
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
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
			received_payload_bytes_bucket{method="gRPC",route="/grpc.health.v1.Health/Check",le="4"} 0
			received_payload_bytes_bucket{method="gRPC",route="/grpc.health.v1.Health/Check",le="16"} 1
			received_payload_bytes_bucket{method="gRPC",route="/grpc.health.v1.Health/Check",le="64"} 1
			received_payload_bytes_bucket{method="gRPC",route="/grpc.health.v1.Health/Check",le="256"} 1
			received_payload_bytes_bucket{method="gRPC",route="/grpc.health.v1.Health/Check",le="1024"} 1
			received_payload_bytes_bucket{method="gRPC",route="/grpc.health.v1.Health/Check",le="4096"} 1
			received_payload_bytes_bucket{method="gRPC",route="/grpc.health.v1.Health/Check",le="16384"} 1
			received_payload_bytes_bucket{method="gRPC",route="/grpc.health.v1.Health/Check",le="65536"} 1
			received_payload_bytes_bucket{method="gRPC",route="/grpc.health.v1.Health/Check",le="262144"} 1
			received_payload_bytes_bucket{method="gRPC",route="/grpc.health.v1.Health/Check",le="1.048576e+06"} 1
			received_payload_bytes_bucket{method="gRPC",route="/grpc.health.v1.Health/Check",le="4.194304e+06"} 1
			received_payload_bytes_bucket{method="gRPC",route="/grpc.health.v1.Health/Check",le="1.6777216e+07"} 2
			received_payload_bytes_bucket{method="gRPC",route="/grpc.health.v1.Health/Check",le="6.7108864e+07"} 2
			received_payload_bytes_bucket{method="gRPC",route="/grpc.health.v1.Health/Check",le="2.68435456e+08"} 2
			received_payload_bytes_bucket{method="gRPC",route="/grpc.health.v1.Health/Check",le="1.073741824e+09"} 2
			received_payload_bytes_bucket{method="gRPC", route="/grpc.health.v1.Health/Check",le="+Inf"} 2
			received_payload_bytes_sum{method="gRPC", route="/grpc.health.v1.Health/Check"} 8.388623e+06
			received_payload_bytes_count{method="gRPC", route="/grpc.health.v1.Health/Check"} 2

			# HELP sent_payload_bytes Size of sent gRPC
			# TYPE sent_payload_bytes histogram
			sent_payload_bytes_bucket{method="gRPC",route="/grpc.health.v1.Health/Check",le="4"} 0
			sent_payload_bytes_bucket{method="gRPC",route="/grpc.health.v1.Health/Check",le="16"} 1
			sent_payload_bytes_bucket{method="gRPC",route="/grpc.health.v1.Health/Check",le="64"} 1
			sent_payload_bytes_bucket{method="gRPC",route="/grpc.health.v1.Health/Check",le="256"} 1
			sent_payload_bytes_bucket{method="gRPC",route="/grpc.health.v1.Health/Check",le="1024"} 1
			sent_payload_bytes_bucket{method="gRPC",route="/grpc.health.v1.Health/Check",le="4096"} 1
			sent_payload_bytes_bucket{method="gRPC",route="/grpc.health.v1.Health/Check",le="16384"} 1
			sent_payload_bytes_bucket{method="gRPC",route="/grpc.health.v1.Health/Check",le="65536"} 1
			sent_payload_bytes_bucket{method="gRPC",route="/grpc.health.v1.Health/Check",le="262144"} 1
			sent_payload_bytes_bucket{method="gRPC",route="/grpc.health.v1.Health/Check",le="1.048576e+06"} 1
			sent_payload_bytes_bucket{method="gRPC",route="/grpc.health.v1.Health/Check",le="4.194304e+06"} 1
			sent_payload_bytes_bucket{method="gRPC",route="/grpc.health.v1.Health/Check",le="1.6777216e+07"} 1
			sent_payload_bytes_bucket{method="gRPC",route="/grpc.health.v1.Health/Check",le="6.7108864e+07"} 1
			sent_payload_bytes_bucket{method="gRPC",route="/grpc.health.v1.Health/Check",le="2.68435456e+08"} 1
			sent_payload_bytes_bucket{method="gRPC",route="/grpc.health.v1.Health/Check",le="1.073741824e+09"} 1
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

	stats := NewStatsHandler(reg, received, sent, inflightRequests, true)

	serv := grpc.NewServer(grpc.StatsHandler(stats), grpc.MaxSendMsgSize(10e6), grpc.MaxRecvMsgSize(10e6))
	defer serv.GracefulStop()

	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	middleware_test.RegisterEchoServerServer(serv, &halfEcho{log: t.Log})

	go func() {
		require.NoError(t, serv.Serve(listener))
	}()

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(10e6), grpc.MaxCallSendMsgSize(10e6)))
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
			received_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="4"} 0
			received_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="16"} 0
			received_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="64"} 0
			received_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="256"} 0
			received_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="1024"} 0
			received_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="4096"} 0
			received_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="16384"} 0
			received_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="65536"} 0
			received_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="262144"} 0
			received_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="1.048576e+06"} 0
			received_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="4.194304e+06"} 3
			received_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="1.6777216e+07"} 5
			received_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="6.7108864e+07"} 5
			received_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="2.68435456e+08"} 5
			received_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="1.073741824e+09"} 5
			received_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="+Inf"} 5
			received_payload_bytes_sum{method="gRPC",route="/middleware.EchoServer/Process"} 1.5728689e+07
			received_payload_bytes_count{method="gRPC",route="/middleware.EchoServer/Process"} 5

			# HELP sent_payload_bytes Size of sent gRPC
			# TYPE sent_payload_bytes histogram
			sent_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="4"} 0
			sent_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="16"} 0
			sent_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="64"} 0
			sent_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="256"} 0
			sent_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="1024"} 0
			sent_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="4096"} 0
			sent_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="16384"} 0
			sent_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="65536"} 0
			sent_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="262144"} 0
			sent_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="1.048576e+06"} 1
			sent_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="4.194304e+06"} 5
			sent_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="1.6777216e+07"} 5
			sent_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="6.7108864e+07"} 5
			sent_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="2.68435456e+08"} 5
			sent_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="1.073741824e+09"} 5
			sent_payload_bytes_bucket{method="gRPC",route="/middleware.EchoServer/Process",le="+Inf"} 5
			sent_payload_bytes_sum{method="gRPC",route="/middleware.EchoServer/Process"} 7.864367e+06
			sent_payload_bytes_count{method="gRPC",route="/middleware.EchoServer/Process"} 5
	`), "received_payload_bytes", "sent_payload_bytes")

	require.NoError(t, err)
}

func TestGrpcStatsMaxStreamsDisabled(t *testing.T) {
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

	stats := NewStatsHandler(reg, received, sent, inflightRequests, false)

	serv := grpc.NewServer(grpc.StatsHandler(stats), grpc.MaxRecvMsgSize(10e6))
	defer serv.GracefulStop()

	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	middleware_test.RegisterEchoServerServer(serv, &halfEcho{log: t.Log})

	go func() {
		require.NoError(t, serv.Serve(listener))
	}()

	ctx, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	conn1 := launchConnWithStreams(t, ctx, listener, 10)
	defer conn1.Close()

	err = testutil.GatherAndCompare(reg, bytes.NewBufferString(``), "grpc_concurrent_streams_by_conn_max")
	assert.NoError(t, err)
}

func TestGrpcStatsMaxStreams(t *testing.T) {
	const (
		waitTime = 1 * time.Second
		sleep    = waitTime / 10
	)
	reg := prometheus.NewRegistry()

	waitAndExpectMaxStreams := func(expected int) {
		var err error
		for endTime := time.Now().Add(waitTime); time.Now().Before(endTime); {
			err = testutil.GatherAndCompare(reg, bytes.NewBufferString(fmt.Sprintf(`
			# HELP grpc_concurrent_streams_by_conn_max The current number of concurrent streams in the connection with the most concurrent streams.
			# TYPE grpc_concurrent_streams_by_conn_max gauge
			grpc_concurrent_streams_by_conn_max{} %d
		`, expected)), "grpc_concurrent_streams_by_conn_max")
			if err == nil {
				break
			}
			time.Sleep(sleep)
		}
		require.NoError(t, err)
	}

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

	stats := NewStatsHandler(reg, received, sent, inflightRequests, true)

	serv := grpc.NewServer(grpc.StatsHandler(stats), grpc.MaxRecvMsgSize(10e6))
	defer serv.GracefulStop()

	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	middleware_test.RegisterEchoServerServer(serv, &halfEcho{log: t.Log})

	go func() {
		require.NoError(t, serv.Serve(listener))
	}()

	// Create a connection with 10 streams
	ctx, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	conn1 := launchConnWithStreams(t, ctx, listener, 10)
	defer conn1.Close()
	waitAndExpectMaxStreams(10)

	// Create a connection with 5 streams. Max is still 10.
	ctx, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	conn2 := launchConnWithStreams(t, ctx, listener, 5)
	defer conn2.Close()
	waitAndExpectMaxStreams(10)

	// Cancel the first connection context to stop the streams. Connection 2 should still have 5 streams.
	cancel1()
	waitAndExpectMaxStreams(5)

	// Launch a new connection with 15 streams
	ctx, cancel3 := context.WithCancel(context.Background())
	defer cancel3()
	conn3 := launchConnWithStreams(t, ctx, listener, 15)
	defer conn3.Close()
	waitAndExpectMaxStreams(15)

	// Cancel the third connection context to stop the streams. Connection 2 should still have 5 streams.
	cancel3()
	waitAndExpectMaxStreams(5)

	// Cancel the second connection context to stop the streams. All streams should be closed.
	cancel2()
	waitAndExpectMaxStreams(0)

}

func launchConnWithStreams(t *testing.T, ctx context.Context, listener net.Listener, streamsCount int) *grpc.ClientConn {
	t.Helper()

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(10e6), grpc.MaxCallSendMsgSize(10e6)))
	require.NoError(t, err)

	fc := middleware_test.NewEchoServerClient(conn)

	streams := make([]middleware_test.EchoServer_ProcessClient, streamsCount)
	for i := 0; i < streamsCount; i++ {
		stream, err := fc.Process(context.Background())
		require.NoError(t, err)
		streams[i] = stream
	}

	// Keep streams alive by continuously sending messages
	for i := 0; i < len(streams); i++ {
		go func(streamIndex int) {
			for {
				select {
				case <-ctx.Done():
					if err := streams[streamIndex].CloseSend(); err != nil {
						t.Log("Error closing stream", err)
					}
					return
				default:
					msg := &middleware_test.Msg{
						Body: []byte(generateString(100)), // Small message to keep stream alive
					}
					if err := streams[streamIndex].Send(msg); err != nil {
						return
					}
					if _, err := streams[streamIndex].Recv(); err != nil {
						return
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
		}(i)
	}

	return conn
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

func TestGrpcStreamTracker(t *testing.T) {
	tracker := NewStreamTracker(prometheus.NewDesc(
		"grpc_streams_by_conn_max",
		"The current number of concurrent streams in the connection with the most concurrent streams.",
		[]string{},
		prometheus.Labels{},
	))

	// Test basic stream operations
	conn0 := "conn0"
	conn1 := "conn1"

	// Open streams for conn0
	tracker.OpenStream(conn0)
	require.Equal(t, 1, tracker.MaxStreams())

	tracker.OpenStream(conn0)
	require.Equal(t, 2, tracker.MaxStreams())

	// Open streams for conn1
	tracker.OpenStream(conn1)
	require.Equal(t, 2, tracker.MaxStreams()) // conn0 still has more streams

	tracker.OpenStream(conn1)
	require.Equal(t, 2, tracker.MaxStreams()) // equal number of streams

	tracker.OpenStream(conn1)
	require.Equal(t, 3, tracker.MaxStreams()) // conn1 now has more streams

	// Close streams
	tracker.CloseStream(conn1)
	require.Equal(t, 2, tracker.MaxStreams()) // back to equal

	tracker.CloseStream(conn0)
	require.Equal(t, 2, tracker.MaxStreams()) // conn1 has more

	tracker.CloseStream(conn0)
	require.Equal(t, 2, tracker.MaxStreams()) // conn1 still has more

	tracker.CloseStream(conn1)
	require.Equal(t, 1, tracker.MaxStreams())

	tracker.CloseStream(conn1)
	require.Equal(t, 0, tracker.MaxStreams()) // all streams closed

	// Test concurrent operations
	// Open streams for conn0, conn1, conn2
	var wg sync.WaitGroup
	conns := []string{"conn0", "conn1", "conn2"}
	streamsPerConn := 300

	for connIndex, conn := range conns {
		wg.Add(1)
		go func(connIndex int, conn string) {
			defer wg.Done()
			for i := 0; i < streamsPerConn+connIndex; i++ {
				tracker.OpenStream(conn)
				time.Sleep(time.Millisecond) // Add some delay to increase chance of race conditions
			}
		}(connIndex, conn)
	}

	wg.Wait()
	require.Equal(t, streamsPerConn+2, tracker.MaxStreams()) // +2 because idx=0,1,2

	// Close half of the streams for each conn
	for _, conn := range conns {
		wg.Add(1)
		go func(conn string) {
			defer wg.Done()
			for i := 0; i < streamsPerConn/2; i++ {
				tracker.CloseStream(conn)
				time.Sleep(time.Millisecond) // Add some delay to increase chance of race conditions
			}
		}(conn)
	}

	wg.Wait()
	require.Equal(t, streamsPerConn/2+2, tracker.MaxStreams()) // +2 because idx=0,1,2

	// Close all streams on conn0
	for i := 0; i < streamsPerConn; i++ {
		tracker.CloseStream("conn0")
	}
	require.Equal(t, (streamsPerConn/2)+2, tracker.MaxStreams()) // +2 because idx=1,2 remain

	// Close all streams on conn2
	for i := 0; i < streamsPerConn; i++ {
		tracker.CloseStream("conn2")
	}
	require.Equal(t, (streamsPerConn/2)+1, tracker.MaxStreams()) // +1 because idx=1 remains

	// Close remaining streams. Only conn1 should remain open.
	for tracker.MaxStreams() > 1 {
		curr := tracker.MaxStreams()
		tracker.CloseStream("conn1")
		require.Equal(t, curr-1, tracker.MaxStreams())
	}
}

func BenchmarkStreamTracker(b *testing.B) {
	tracker := NewStreamTracker(prometheus.NewDesc(
		"grpc_streams_by_conn_max",
		"The current number of concurrent streams in the connection with the most concurrent streams.",
		[]string{},
		prometheus.Labels{},
	))
	numConns := 1000
	existingConnIDs := make([]string, numConns)
	for i := 0; i < numConns; i++ {
		existingConnIDs[i] = fmt.Sprintf("conn%d", i)
	}
	newConnIDs := make([]string, numConns)
	for i := 0; i < numConns; i++ {
		newConnIDs[i] = fmt.Sprintf("conn%d", i+numConns)
	}
	connIDs := append(existingConnIDs, newConnIDs...)

	// Bootstrap a bit of streams to make sure we have a bit of load
	for i := 0; i < numConns*1000; i++ {
		connID := existingConnIDs[mathRand.Intn(numConns)]
		tracker.OpenStream(connID)
	}

	// Benchmark opening streams
	b.Run("OpenStreamOnExistingConn", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			connID := existingConnIDs[mathRand.Intn(numConns)]
			tracker.OpenStream(connID)
		}
	})

	// Benchmark opening streams on new connections
	b.Run("OpenStreamOnNewConn", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			connID := newConnIDs[mathRand.Intn(numConns)]
			tracker.OpenStream(connID)
		}
	})

	// Benchmark getting the max streams with a ton of streams
	b.Run("MaxStreams", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tracker.MaxStreams()
		}
	})

	b.Run("CloseStream", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			connID := connIDs[mathRand.Intn(numConns*2)]
			tracker.CloseStream(connID)
		}
	})
}

func BenchmarkStreamTrackerConnectionsSimulation(b *testing.B) {
	tracker := NewStreamTracker(prometheus.NewDesc(
		"grpc_streams_by_conn_max",
		"The current number of concurrent streams in the connection with the most concurrent streams.",
		[]string{},
		prometheus.Labels{},
	))

	for i := 0; i < b.N; i++ {
		numRoutines := 10000

		wg := sync.WaitGroup{}
		wg.Add(numRoutines)
		for i := 0; i < numRoutines; i++ {
			go func(i int) {
				defer wg.Done()

				// Open 3, close 3
				connID := fmt.Sprintf("conn%d", i%10)
				tracker.OpenStream(connID)
				time.Sleep(time.Millisecond)
				tracker.OpenStream(connID)
				time.Sleep(time.Millisecond)
				tracker.CloseStream(connID)
				time.Sleep(time.Millisecond)
				tracker.OpenStream(connID)
				time.Sleep(time.Millisecond)
				tracker.CloseStream(connID)
				time.Sleep(time.Millisecond)
				tracker.CloseStream(connID)
				time.Sleep(time.Millisecond)
				tracker.OpenStream(connID)
				tracker.OpenStream(connID)
				tracker.CloseStream(connID)
				tracker.CloseStream(connID)
			}(i)
		}

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 1000; i++ {
					tracker.MaxStreams()
				}
			}()
		}

		wg.Wait()

		if tracker.MaxStreams() != 0 {
			b.Fatalf("MaxStreams() = %d, want 0", tracker.MaxStreams())
		}
	}
}
