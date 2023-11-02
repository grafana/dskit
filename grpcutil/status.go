package grpcutil

import (
	"context"
	"errors"

	"github.com/gogo/status"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

// ErrorToStatus tries to cast the given error into gogo's status.Status.
// If the given error, or any error from its tree are a status.Status,
// that status.Status and the outcome true are returned.
// Otherwise, nil and the outcome false are returned.
// This implementation differs from status.FromError() because the
// latter checks only if the given error can be cast to status.Status,
// and doesn't check other errors in the given error's tree.
func ErrorToStatus(err error) (*status.Status, bool) {
	if err == nil {
		return nil, false
	}
	type grpcStatus interface{ GRPCStatus() *grpcstatus.Status }
	var gs grpcStatus
	if errors.As(err, &gs) {
		st := gs.GRPCStatus()
		if st == nil {
			return nil, false
		}
		return status.FromGRPCStatus(st), true
	}
	return nil, false
}

// ErrorToStatusCode returns the Code of the error if it is a gogo's status.Status
// error or if it wraps a status.Status error. If that is not the case, it returns
// codes.OK if err is nil, or codes.Unknown otherwise.
func ErrorToStatusCode(err error) codes.Code {
	if err == nil {
		return codes.OK
	}
	type grpcStatus interface{ GRPCStatus() *grpcstatus.Status }
	var gs grpcStatus
	if errors.As(err, &gs) {
		st := gs.GRPCStatus()
		if st != nil {
			return st.Code()
		}
	}
	return codes.Unknown
}

// IsCanceled checks whether an error comes from an operation being canceled.
func IsCanceled(err error) bool {
	if errors.Is(err, context.Canceled) {
		return true
	}
	statusCode := ErrorToStatusCode(err)
	return statusCode == codes.Canceled
}
