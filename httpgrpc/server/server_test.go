// Provenance-includes-location: https://github.com/weaveworks/common/blob/main/httpgrpc/server/server_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Weaveworks Ltd.

package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	jaegercfg "github.com/uber/jaeger-client-go/config"
	"google.golang.org/grpc"

	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/user"
)

func TestReturn4XXErrorsOption(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, err := fmt.Fprint(w, "test")
		require.NoError(t, err)
	})
	serverOptions := make([]Option, 0, 1)
	server := NewServer(handler, serverOptions...)
	require.NotNil(t, server)
	require.False(t, server.return4XXErrors)

	serverOptions = append(serverOptions, WithReturn4XXErrors)
	server = NewServer(handler, serverOptions...)
	require.NotNil(t, server)
	require.True(t, server.return4XXErrors)
}

type testServer struct {
	*Server
	URL        string
	grpcServer *grpc.Server
}

func newTestServer(t *testing.T, handler http.Handler) (*testServer, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}

	server := &testServer{
		Server:     NewServer(handler),
		grpcServer: grpc.NewServer(),
		URL:        "direct://" + lis.Addr().String(),
	}

	httpgrpc.RegisterHTTPServer(server.grpcServer, server.Server)
	go func() {
		require.NoError(t, server.grpcServer.Serve(lis))
	}()

	return server, nil
}

func TestBasic(t *testing.T) {
	server, err := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, err := fmt.Fprint(w, "world")
		require.NoError(t, err)
	}))
	require.NoError(t, err)
	defer server.grpcServer.GracefulStop()

	client, err := NewClient(server.URL)
	require.NoError(t, err)

	req, err := http.NewRequest("GET", "/hello", &bytes.Buffer{})
	require.NoError(t, err)

	req = req.WithContext(user.InjectOrgID(context.Background(), "1"))
	recorder := httptest.NewRecorder()
	client.ServeHTTP(recorder, req)

	assert.Equal(t, "world", recorder.Body.String())
	assert.Equal(t, 200, recorder.Code)
}

func TestError(t *testing.T) {
	for _, doNotLog := range []bool{true, false} {
		var stat string
		if !doNotLog {
			stat = "not "
		}
		t.Run(fmt.Sprintf("test header when DoNotLogErrorHeaderKey is %spresent", stat), func(t *testing.T) {
			server, err := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if doNotLog {
					w.Header().Set(DoNotLogErrorHeaderKey, "true")
				}
				// Does a Fprintln, injecting a newline.
				http.Error(w, "foo", http.StatusInternalServerError)
			}))
			require.NoError(t, err)
			defer server.grpcServer.GracefulStop()

			client, err := NewClient(server.URL)
			require.NoError(t, err)

			req, err := http.NewRequest("GET", "/hello", &bytes.Buffer{})
			require.NoError(t, err)

			req = req.WithContext(user.InjectOrgID(context.Background(), "1"))
			recorder := httptest.NewRecorder()
			client.ServeHTTP(recorder, req)

			assert.Equal(t, "foo\n", recorder.Body.String())
			assert.Equal(t, 500, recorder.Code)
			assert.NotContains(t, recorder.Header(), DoNotLogErrorHeaderKey)
		})
	}
}

func TestServerHandleDoNotLogError(t *testing.T) {
	testCases := map[string]struct {
		errorCode     int
		doNotLogError bool
		expectedError bool
	}{
		"HTTPResponse with code 5xx and with DoNotLogError header should return a non-loggable error": {
			errorCode:     http.StatusInternalServerError,
			doNotLogError: true,
			expectedError: true,
		},
		"HTTPResponse with code 5xx and without DoNotLogError header should return a loggable error": {
			errorCode:     http.StatusInternalServerError,
			expectedError: true,
		},
		"HTTPResponse with code different from 5xx and with DoNotLogError header should not return an error": {
			errorCode:     http.StatusBadRequest,
			doNotLogError: true,
			expectedError: false,
		},
		"HTTPResponse with code different from 5xx and without DoNotLogError header should not return an error": {
			errorCode:     http.StatusBadRequest,
			expectedError: false,
		},
	}
	errMsg := "this is an error"
	for testName, testData := range testCases {
		t.Run(testName, func(t *testing.T) {
			h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if testData.doNotLogError {
					w.Header().Set(DoNotLogErrorHeaderKey, "true")
				}
				http.Error(w, errMsg, testData.errorCode)
			})

			s := NewServer(h)
			req := &httpgrpc.HTTPRequest{
				Method: "GET",
				Url:    "/test",
			}
			resp, err := s.Handle(context.Background(), req)
			if testData.expectedError {
				require.Error(t, err)
				require.Nil(t, resp)
				var optional middleware.OptionalLogging
				if testData.doNotLogError {
					require.ErrorAs(t, err, &optional)

					actual, _ := optional.ShouldLog(context.Background())
					require.False(t, actual)
				} else {
					require.False(t, errors.As(err, &optional))
				}
				checkError(t, err, testData.errorCode, errMsg)
			} else {
				require.NoError(t, err)
				require.NotNil(t, resp)
				checkHTTPResponse(t, resp, testData.errorCode, errMsg)
			}
		})
	}
}

func TestServerHandleReturn4XXErrors(t *testing.T) {
	testCases := map[string]struct {
		errorCode       int
		return4xxErrors bool
		expectedError   bool
	}{
		"HTTPResponse with code 5xx should return an error when server creates with Return4XXErrorsOption": {
			errorCode:       http.StatusInternalServerError,
			return4xxErrors: true,
			expectedError:   true,
		},
		"HTTPResponse with code 5xx should return an error when server creates without Return4XXErrorsOption": {
			errorCode:       http.StatusInternalServerError,
			return4xxErrors: false,
			expectedError:   true,
		},
		"HTTPResponse with code 4xx should return an error when server creates with Return4XXErrorsOption": {
			errorCode:       http.StatusBadRequest,
			return4xxErrors: true,
			expectedError:   true,
		},
		"HTTPResponse with code 4xx should not return an error when server creates without Return4XXErrorsOption": {
			errorCode:       http.StatusBadRequest,
			return4xxErrors: false,
			expectedError:   false,
		},
		"HTTPResponse with code different from 5xx and 4xx should not return an error when server creates with Return4XXErrorsOption": {
			errorCode:       http.StatusNoContent,
			return4xxErrors: true,
			expectedError:   false,
		},
		"HTTPResponse with code different from 5xx and 4xx should not return an error when server creates without Return4XXErrorsOption": {
			errorCode:       http.StatusNoContent,
			return4xxErrors: false,
			expectedError:   false,
		},
	}
	errMsg := "this is an error"
	for testName, testData := range testCases {
		t.Run(testName, func(t *testing.T) {
			h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, errMsg, testData.errorCode)
			})

			var serverOptions []Option
			if testData.return4xxErrors {
				serverOptions = []Option{WithReturn4XXErrors}
			}
			s := NewServer(h, serverOptions...)

			req := &httpgrpc.HTTPRequest{
				Method: "GET",
				Url:    "/test",
			}
			resp, err := s.Handle(context.Background(), req)
			if testData.expectedError {
				require.Error(t, err)
				require.Nil(t, resp)
				checkError(t, err, testData.errorCode, errMsg)
			} else {
				require.NoError(t, err)
				require.NotNil(t, resp)
				checkHTTPResponse(t, resp, testData.errorCode, errMsg)
			}
		})
	}
}

func checkError(t *testing.T, err error, expectedCode int, expectedMessage string) {
	resp, ok := httpgrpc.HTTPResponseFromError(err)
	require.True(t, ok)
	checkHTTPResponse(t, resp, expectedCode, expectedMessage)
}

func checkHTTPResponse(t *testing.T, resp *httpgrpc.HTTPResponse, expectedCode int, expectedBody string) {
	require.Equal(t, int32(expectedCode), resp.GetCode())
	require.Equal(t, fmt.Sprintf("%s\n", expectedBody), string(resp.GetBody()))
	hs := resp.GetHeaders()
	for _, h := range hs {
		require.NotEqual(t, DoNotLogErrorHeaderKey, h.Key)
	}
}

func TestParseURL(t *testing.T) {
	for _, tc := range []struct {
		input    string
		expected string
		err      string
	}{
		{"direct://foo", "foo", ""},
		{"kubernetes://foo:123", "kubernetes:///foo:123", ""},
		{"querier.cortex:995", "kubernetes:///querier.cortex:995", ""},
		{"foo.bar.svc.local:995", "kubernetes:///foo.bar.svc.local:995", ""},
		{"kubernetes:///foo:123", "kubernetes:///foo:123", ""},
		{"dns:///foo.bar.svc.local:995", "dns:///foo.bar.svc.local:995", ""},
		{"monster://foo:995", "", "unrecognised scheme: monster"},
	} {
		got, err := ParseURL(tc.input)
		if tc.err == "" {
			require.NoError(t, err)
		} else {
			require.EqualError(t, err, tc.err)
		}
		assert.Equal(t, tc.expected, got)
	}
}

func TestTracePropagation(t *testing.T) {
	jaeger := jaegercfg.Configuration{}
	closer, err := jaeger.InitGlobalTracer("test")
	require.NoError(t, err)
	defer closer.Close()

	server, err := newTestServer(t, middleware.Tracer{}.Wrap(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			span := opentracing.SpanFromContext(r.Context())
			_, err := fmt.Fprint(w, span.BaggageItem("name"))
			require.NoError(t, err)
		}),
	))

	require.NoError(t, err)
	defer server.grpcServer.GracefulStop()

	client, err := NewClient(server.URL)
	require.NoError(t, err)

	req, err := http.NewRequest("GET", "/hello", &bytes.Buffer{})
	require.NoError(t, err)

	sp, ctx := opentracing.StartSpanFromContext(context.Background(), "Test")
	sp.SetBaggageItem("name", "world")

	req = req.WithContext(user.InjectOrgID(ctx, "1"))
	recorder := httptest.NewRecorder()
	client.ServeHTTP(recorder, req)

	assert.Equal(t, "world", recorder.Body.String())
	assert.Equal(t, 200, recorder.Code)
}

func TestGrpcErrorsHaveCorrectMessage(t *testing.T) {
	testCases := map[string]struct {
		responseBody         string
		errorMessageInHeader string

		expectedErrorMessage string
	}{
		"error response with string body": {
			responseBody:         "hello world",
			expectedErrorMessage: "rpc error: code = Code(500) desc = hello world",
		},
		"error response with binary body": {
			responseBody:         "\x08\x08\x12\xc7\x03the request has been rejected",
			expectedErrorMessage: "rpc error: code = Code(500) desc = \x08\x08\x12\xc7\x03the request has been rejected",
		},
		"error response with binary body and provided message via header": {
			responseBody:         "\x08\x08\x12\xc7\x03the request has been rejected",
			errorMessageInHeader: "hello world",
			expectedErrorMessage: "rpc error: code = Code(500) desc = hello world",
		},
	}
	for testName, testData := range testCases {
		t.Run(testName, func(t *testing.T) {
			h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if testData.errorMessageInHeader != "" {
					w.Header().Set(ErrorMessageHeaderKey, testData.errorMessageInHeader)
				}
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(testData.responseBody))
			})

			s := NewServer(h)
			req := &httpgrpc.HTTPRequest{Method: "GET", Url: "/test"}
			resp, err := s.Handle(context.Background(), req)
			require.Error(t, err)
			require.Nil(t, resp)

			require.Equal(t, testData.expectedErrorMessage, err.Error())

			httpResp, ok := httpgrpc.HTTPResponseFromError(err)
			require.True(t, ok)
			// Verify that header was removed
			require.Empty(t, httpResp.Headers)
		})
	}
}

func TestIsHandledByHttpgrpcServer(t *testing.T) {
	t.Run("false by default", func(t *testing.T) {
		require.False(t, IsHandledByHttpgrpcServer(context.Background()))
	})

	const testHeader = "X-HandledByHttpgrpcServer"

	// Handler will return value returned by IsHandledByHttpgrpcServer in test header.
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(testHeader, strconv.FormatBool(IsHandledByHttpgrpcServer(r.Context())))
		w.WriteHeader(200)
	})

	t.Run("handler runs outside of httpgrpc.Server", func(t *testing.T) {
		rec := httptest.NewRecorder()
		handler(rec, &http.Request{})
		require.Equal(t, "false", rec.Header().Get(testHeader))
	})

	t.Run("handler runs from httpgrpc.Server", func(t *testing.T) {
		s := NewServer(handler)
		resp, err := s.Handle(context.Background(), &httpgrpc.HTTPRequest{Method: "GET", Url: "/test"})
		require.NoError(t, err)
		require.NotNil(t, resp)

		require.Equal(t, []*httpgrpc.Header{{Key: http.CanonicalHeaderKey(testHeader), Values: []string{"true"}}}, resp.Headers)
	})
}
