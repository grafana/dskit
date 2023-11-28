package tenant

import (
	"context"
	"errors"
	"strings"

	"github.com/grafana/dskit/user"
)

var (
	errInvalidTenantID = errors.New("invalid tenant ID")
)

// TenantID returns exactly a single tenant ID from the context. It should be
// used when a certain endpoint should only support exactly a single
// tenant ID. It returns an error user.ErrNoOrgID if there is no tenant ID
// supplied or user.ErrTooManyOrgIDs if there are multiple tenant IDs present.
//
// ignore stutter warning
//
//nolint:revive
func TenantID(ctx context.Context) (string, error) {
	orgIDs, err := TenantIDs(ctx)
	if err != nil {
		return "", err
	}

	if len(orgIDs) > 1 {
		return "", user.ErrTooManyOrgIDs
	}

	return orgIDs[0], nil
}

// TenantIDs returns all tenant IDs from the context. It should return
// normalized list of ordered and distinct tenant IDs (as produced by
// NormalizeTenantIDs).
//
// ignore stutter warning
//
//nolint:revive
func TenantIDs(ctx context.Context) ([]string, error) {
	//lint:ignore faillint wrapper around upstream method
	orgID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}

	orgIDs := strings.Split(orgID, tenantIDsSeparator)
	for _, orgID := range orgIDs {
		if err := ValidTenantID(orgID); err != nil {
			return nil, err
		}
		if containsUnsafePathSegments(orgID) {
			return nil, errInvalidTenantID
		}
	}

	return NormalizeTenantIDs(orgIDs), nil
}

type Resolver interface {
	// TenantID returns exactly a single tenant ID from the context. It should be
	// used when a certain endpoint should only support exactly a single
	// tenant ID. It returns an error user.ErrNoOrgID if there is no tenant ID
	// supplied or user.ErrTooManyOrgIDs if there are multiple tenant IDs present.
	TenantID(context.Context) (string, error)

	// TenantIDs returns all tenant IDs from the context. It should return
	// normalized list of ordered and distinct tenant IDs (as produced by
	// NormalizeTenantIDs).
	TenantIDs(context.Context) ([]string, error)
}

type MultiResolver struct{}

// NewMultiResolver creates a tenant resolver, which allows request to have
// multiple tenant ids submitted separated by a '|' character. This enforces
// further limits on the character set allowed within tenants as detailed here:
// https://grafana.com/docs/mimir/latest/configure/about-tenant-ids/
func NewMultiResolver() *MultiResolver {
	return &MultiResolver{}
}

func (t *MultiResolver) TenantID(ctx context.Context) (string, error) {
	return TenantID(ctx)
}

func (t *MultiResolver) TenantIDs(ctx context.Context) ([]string, error) {
	return TenantIDs(ctx)
}

// containsUnsafePathSegments will return true if the string is a directory
// reference like `.` and `..` or if any path separator character like `/` and
// `\` can be found.
func containsUnsafePathSegments(id string) bool {
	// handle the relative reference to current and parent path.
	if id == "." || id == ".." {
		return true
	}

	return strings.ContainsAny(id, "\\/")
}
