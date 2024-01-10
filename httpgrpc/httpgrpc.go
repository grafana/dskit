// Provenance-includes-location: https://github.com/weaveworks/common/blob/main/httpgrpc/httpgrpc.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Weaveworks Ltd.

package httpgrpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/go-kit/log/level"
	spb "github.com/gogo/googleapis/google/rpc"
	"github.com/gogo/protobuf/types"
	"github.com/gogo/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/log"
)

const (
	MetadataMethod = "httpgrpc-method"
	MetadataURL    = "httpgrpc-url"
)

// AppendRequestMetadataToContext appends metadata of HTTPRequest into gRPC metadata.
func AppendRequestMetadataToContext(ctx context.Context, req *HTTPRequest) context.Context {
	return metadata.AppendToOutgoingContext(ctx,
		MetadataMethod, req.Method,
		MetadataURL, req.Url)
}

type nopCloser struct {
	*bytes.Buffer
}

func (nopCloser) Close() error { return nil }

// BytesBuffer returns the underlaying `bytes.buffer` used to build this io.ReadCloser.
func (n nopCloser) BytesBuffer() *bytes.Buffer { return n.Buffer }

// FromHTTPRequest converts an ordinary http.Request into an httpgrpc.HTTPRequest
func FromHTTPRequest(r *http.Request) (*HTTPRequest, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	return &HTTPRequest{
		Method:  r.Method,
		Url:     r.RequestURI,
		Body:    body,
		Headers: FromHeader(r.Header),
	}, nil
}

// ToHTTPRequest converts httpgrpc.HTTPRequest to http.Request.
func ToHTTPRequest(ctx context.Context, r *HTTPRequest) (*http.Request, error) {
	req, err := http.NewRequest(r.Method, r.Url, nopCloser{Buffer: bytes.NewBuffer(r.Body)})
	if err != nil {
		return nil, err
	}
	ToHeader(r.Headers, req.Header)
	req = req.WithContext(ctx)
	req.RequestURI = r.Url
	req.ContentLength = int64(len(r.Body))
	return req, nil
}

// WriteResponse converts an httpgrpc response to an HTTP one
func WriteResponse(w http.ResponseWriter, resp *HTTPResponse) error {
	ToHeader(resp.Headers, w.Header())
	w.WriteHeader(int(resp.Code))
	_, err := w.Write(resp.Body)
	return err
}

// WriteError converts an httpgrpc error to an HTTP one
func WriteError(w http.ResponseWriter, err error) {
	resp, ok := HTTPResponseFromError(err)
	if ok {
		_ = WriteResponse(w, resp)
	} else {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func ToHeader(hs []*Header, header http.Header) {
	for _, h := range hs {
		header[h.Key] = h.Values
	}
}

func FromHeader(hs http.Header) []*Header {
	result := make([]*Header, 0, len(hs))
	for k, vs := range hs {
		result = append(result, &Header{
			Key:    k,
			Values: vs,
		})
	}
	return result
}

type HTTPError struct {
	resp *HTTPResponse
	err  error
}

func (e HTTPError) Error() string {
	return e.err.Error()
}

func (e HTTPError) Unwrap() error {
	return e.err
}

// GRPCStatus with a *grpcstatus.Status as output is needed
// for a correct execution of grpc/status.FromError().
func (e HTTPError) GRPCStatus() *grpcstatus.Status {
	if stat, ok := e.err.(interface{ GRPCStatus() *grpcstatus.Status }); ok {
		return stat.GRPCStatus()
	}
	return nil
}

func (e HTTPError) GetHTTPResponse() *HTTPResponse {
	return e.resp
}

// HTTPErrorf returns an HTTPError than is correctly forwarded over
// gRPC, and can eventually be converted back to an HTTP response with
// HTTPResponseFromError.
func HTTPErrorf(code int, tmpl string, args ...interface{}) HTTPError {
	return HTTPErrorFromHTTPResponse(&HTTPResponse{
		Code: int32(code),
		Body: []byte(fmt.Sprintf(tmpl, args...)),
	})
}

// Errorf returns an HTTP gRPC error than is correctly forwarded over
// gRPC, and can eventually be converted back to an HTTP response with
// HTTPResponseFromError.
func Errorf(code int, tmpl string, args ...interface{}) error {
	return ErrorFromHTTPResponse(&HTTPResponse{
		Code: int32(code),
		Body: []byte(fmt.Sprintf(tmpl, args...)),
	})
}

// HTTPErrorFromHTTPResponse converts an HTTP response into an HTTPError.
func HTTPErrorFromHTTPResponse(resp *HTTPResponse) HTTPError {
	return HTTPError{
		resp: resp,
		err:  errorFromHTTPResponse(int32(codes.Internal), resp),
	}
}

// ErrorFromHTTPResponse converts an HTTP response into a grpc error.
func ErrorFromHTTPResponse(resp *HTTPResponse) error {
	return errorFromHTTPResponse(resp.Code, resp)
}

func errorFromHTTPResponse(code int32, resp *HTTPResponse) error {
	a, err := types.MarshalAny(resp)
	if err != nil {
		return err
	}

	return status.ErrorProto(&spb.Status{
		Code:    code,
		Message: string(resp.Body),
		Details: []*types.Any{a},
	})
}

// HTTPResponseFromError converts a grpc error into an HTTP response
func HTTPResponseFromError(err error) (*HTTPResponse, bool) {
	var httpError HTTPError
	if errors.As(err, &httpError) {
		return httpError.GetHTTPResponse(), true
	}
	s, ok := grpcutil.ErrorToStatus(err)
	if !ok {
		return nil, false
	}

	status := s.Proto()
	if len(status.Details) != 1 {
		return nil, false
	}

	var resp HTTPResponse
	if err := types.UnmarshalAny(status.Details[0], &resp); err != nil {
		level.Error(log.Global()).Log("msg", "got error containing non-response", "err", err)
		return nil, false
	}

	return &resp, true
}
