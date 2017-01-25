package httpgrpc

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"

	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/mwitkow/go-grpc-middleware"
	"github.com/opentracing/opentracing-go"
	"github.com/sercand/kuberesolver"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/weaveworks/common/middleware"
)

// HTTPoGRPCServer is a HTTPServer (ie gRPC) implementation
type HTTPoGRPCServer struct {
	handler http.Handler
}

// NewHTTPoGRPCServer makes a new HTTPoGRPCServer
func NewHTTPoGRPCServer(handler http.Handler) *HTTPoGRPCServer {
	return &HTTPoGRPCServer{
		handler: handler,
	}
}

// Handle implements HTTPServer
func (s HTTPoGRPCServer) Handle(ctx context.Context, r *HTTPRequest) (*HTTPResponse, error) {
	req, err := http.NewRequest(r.Method, r.Url, ioutil.NopCloser(bytes.NewReader(r.Body)))
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	toHeader(r.Headers, req.Header)
	recorder := httptest.NewRecorder()
	s.handler.ServeHTTP(recorder, req)
	resp := &HTTPResponse{
		Code:    int32(recorder.Code),
		Headers: fromHeader(recorder.Header()),
		Body:    recorder.Body.Bytes(),
	}
	return resp, nil
}

// HTTPoGRPCClient is a http.Handler that forward the request over gRPC
type HTTPoGRPCClient struct {
	client HTTPClient
	conn   *grpc.ClientConn
}

// NewHTTPoGRPCClient makes a new NewHTTPoGRPCClient, given a kubernetes
// service address.
func NewHTTPoGRPCClient(service, namespace string, port int) (*HTTPoGRPCClient, error) {
	balancer := kuberesolver.NewWithNamespace(namespace)
	conn, err := grpc.Dial(
		fmt.Sprintf("kubernetes://%s:%d", service, port),
		balancer.DialOption(),
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
			otgrpc.OpenTracingClientInterceptor(opentracing.GlobalTracer()),
			middleware.ClientUserHeaderInterceptor,
		)),
	)
	if err != nil {
		return nil, err
	}
	return &HTTPoGRPCClient{
		client: NewHTTPClient(conn),
		conn:   conn,
	}, nil
}

// ServeHTTP implements http.Handler
func (c *HTTPoGRPCClient) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	req := &HTTPRequest{
		Method:  r.Method,
		Url:     r.URL.String(),
		Body:    body,
		Headers: fromHeader(r.Header),
	}

	resp, err := c.client.Handle(r.Context(), req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	toHeader(resp.Headers, w.Header())
	w.WriteHeader(int(resp.Code))
	if _, err := w.Write(resp.Body); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func toHeader(hs []*Header, header http.Header) {
	for _, h := range hs {
		header[h.Key] = h.Values
	}
}

func fromHeader(hs http.Header) []*Header {
	result := make([]*Header, 0, len(hs))
	for k, vs := range hs {
		result = append(result, &Header{
			Key:    k,
			Values: vs,
		})
	}
	return result
}
