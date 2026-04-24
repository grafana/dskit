// Provenance-includes-location: https://github.com/weaveworks/common/blob/main/middleware/grpc_auth.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Weaveworks Ltd.

package middleware

import (
	"context"

	"google.golang.org/grpc"

	"github.com/grafana/dskit/user"
)

// ClientUserHeaderInterceptor propagates the user ID from the context to gRPC metadata, which eventually ends up as a HTTP2 header.
func ClientUserHeaderInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	ctx, err := user.InjectIntoGRPCRequest(ctx)
	if err != nil {
		return err
	}

	return invoker(ctx, method, req, reply, cc, opts...)
}

// StreamClientUserHeaderInterceptor propagates the user ID from the context to gRPC metadata, which eventually ends up as a HTTP2 header.
// For streaming gRPC requests.
func StreamClientUserHeaderInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	ctx, err := user.InjectIntoGRPCRequest(ctx)
	if err != nil {
		return nil, err
	}

	return streamer(ctx, desc, cc, method, opts...)
}

// ServerUserHeaderInterceptor propagates the user ID from the gRPC metadata back to our context.
func ServerUserHeaderInterceptor(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	orgID, err := user.ExtractOrgIDFromGRPCRequest(ctx)
	if err != nil {
		return nil, err
	}

	return handler(user.InjectOrgIDInline(ctx, orgID), req)
}

// StreamServerUserHeaderInterceptor propagates the user ID from the gRPC metadata back to our context.
func StreamServerUserHeaderInterceptor(srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	orgID, err := user.ExtractOrgIDFromGRPCRequest(ss.Context())
	if err != nil {
		return err
	}

	s := &orgIDServerStream{ServerStream: ss}
	s.orgIDCtx.Context = ss.Context()
	s.orgIDCtx.SetOrgID(orgID)
	return handler(srv, s)
}

// orgIDServerStream wraps a grpc.ServerStream and injects the org ID into the
// context without the overhead of context.WithValue. The OrgIDCtx is embedded
// directly to avoid a separate heap allocation for the context.
type orgIDServerStream struct {
	grpc.ServerStream
	orgIDCtx user.OrgIDCtx
}

func (ss *orgIDServerStream) Context() context.Context {
	return &ss.orgIDCtx
}
