package user

import (
	"net/http"

	"github.com/weaveworks/common/errors"

	"golang.org/x/net/context"
)

const (
	// orgIDHeaderName is a legacy from scope as a service.
	orgIDHeaderName = "X-Scope-OrgID"

	// LowerOrgIDHeaderName as gRPC / HTTP2.0 headers are lowercased.
	lowerOrgIDHeaderName = "x-scope-orgid"
)

// Errors that we return
const (
	ErrNoOrgID               = errors.Error("no org id")
	ErrDifferentOrgIDPresent = errors.Error("different org ID already present")
)

// ExtractFromHTTPRequest extracts the org ID from the request headers and returns
// the org ID and a context with the org ID embbedded.
func ExtractFromHTTPRequest(r *http.Request) (string, context.Context, error) {
	orgID := r.Header.Get(orgIDHeaderName)
	if orgID == "" {
		return "", r.Context(), ErrNoOrgID
	}
	return orgID, Inject(r.Context(), orgID), nil
}

// InjectIntoHTTPRequest injects the orgID from the context into the request headers.
func InjectIntoHTTPRequest(ctx context.Context, r *http.Request) error {
	orgID, err := Extract(ctx)
	if err != nil {
		return err
	}
	existingID := r.Header.Get(orgIDHeaderName)
	if existingID != "" && existingID != orgID {
		return ErrDifferentOrgIDPresent
	}
	r.Header.Set(orgIDHeaderName, orgID)
	return nil
}
