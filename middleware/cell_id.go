// SPDX-License-Identifier: AGPL-3.0-only

package middleware

import (
	"context"
	"net/http"

	"github.com/gogo/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

type CellIDChecker struct {
	CellID string
}

func (i CellIDChecker) Wrap(handler http.Handler) http.Handler {
	if i.CellID == "" {
		return handler
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-Cell-ID"); got != "" && got != i.CellID {
			http.Error(w, "Invalid cell ID", http.StatusForbidden)
			return
		}

		handler.ServeHTTP(w, r)
	})
}

func (i CellIDChecker) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	if i.CellID == "" {
		return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
			return handler(ctx, req)
		}
	}

	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if md, ok := metadata.FromIncomingContext(ctx); !ok {
			return nil, status.Error(codes.PermissionDenied, "cell ID required")
		} else if len(md["cell-id"]) != 1 || md["cell-id"][0] != i.CellID {
			return nil, status.Error(codes.PermissionDenied, "invalid cell ID")
		}

		return handler(ctx, req)
	}
}

func AppendCellIDToOutgoingContext(ctx context.Context, cellID string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "cell-id", cellID)
}

func AddCellIDToRequest(req *http.Request, cellID string) {
	req.Header.Set("X-Cell-ID", cellID)
}
