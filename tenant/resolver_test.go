package tenant

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/grafana/dskit/user"
)

func strptr(s string) *string {
	return &s
}

type resolverTestCase struct {
	name         string
	headerValue  *string
	errTenantID  error
	errTenantIDs error
	tenantID     string
	tenantIDs    []string
}

func TestTenantIDs(t *testing.T) {
	for _, tc := range []resolverTestCase{
		{
			name:         "no-header",
			errTenantID:  user.ErrNoOrgID,
			errTenantIDs: user.ErrNoOrgID,
		},
		{
			name:        "empty",
			headerValue: strptr(""),
			tenantIDs:   []string{""},
		},
		{
			name:        "single-tenant",
			headerValue: strptr("tenant-a"),
			tenantID:    "tenant-a",
			tenantIDs:   []string{"tenant-a"},
		},
		{
			name:         "parent-dir",
			headerValue:  strptr(".."),
			errTenantID:  errInvalidTenantID,
			errTenantIDs: errInvalidTenantID,
		},
		{
			name:         "current-dir",
			headerValue:  strptr("."),
			errTenantID:  errInvalidTenantID,
			errTenantIDs: errInvalidTenantID,
		},
		{
			name:        "multi-tenant",
			headerValue: strptr("tenant-a|tenant-b"),
			errTenantID: user.ErrTooManyOrgIDs,
			tenantIDs:   []string{"tenant-a", "tenant-b"},
		},
		{
			name:        "multi-tenant-wrong-order",
			headerValue: strptr("tenant-b|tenant-a"),
			errTenantID: user.ErrTooManyOrgIDs,
			tenantIDs:   []string{"tenant-a", "tenant-b"},
		},
		{
			name:        "multi-tenant-duplicate-order",
			headerValue: strptr("tenant-b|tenant-b|tenant-a"),
			errTenantID: user.ErrTooManyOrgIDs,
			tenantIDs:   []string{"tenant-a", "tenant-b"},
		},
		{
			name:         "multi-tenant-with-relative-path",
			headerValue:  strptr("tenant-a|tenant-b|.."),
			errTenantID:  errInvalidTenantID,
			errTenantIDs: errInvalidTenantID,
		},
		{
			name:         "containing-forward-slash",
			headerValue:  strptr("forward/slash"),
			errTenantID:  &errTenantIDUnsupportedCharacter{pos: 7, tenantID: "forward/slash"},
			errTenantIDs: &errTenantIDUnsupportedCharacter{pos: 7, tenantID: "forward/slash"},
		},
		{
			name:         "containing-backward-slash",
			headerValue:  strptr(`backward\slash`),
			errTenantID:  &errTenantIDUnsupportedCharacter{pos: 8, tenantID: "backward\\slash"},
			errTenantIDs: &errTenantIDUnsupportedCharacter{pos: 8, tenantID: "backward\\slash"},
		},
		{
			name:         "too-long",
			headerValue:  strptr(strings.Repeat("123", MaxTenantIDLength)),
			errTenantID:  errTenantIDTooLong,
			errTenantIDs: errTenantIDTooLong,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			if tc.headerValue != nil {
				ctx = user.InjectOrgID(ctx, *tc.headerValue)
			}

			tenantID, err := TenantID(ctx)
			if tc.errTenantID != nil {
				assert.Equal(t, tc.errTenantID, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.tenantID, tenantID)
			}

			tenantIDs, err := TenantIDs(ctx)
			if tc.errTenantIDs != nil {
				assert.Equal(t, tc.errTenantIDs, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.tenantIDs, tenantIDs)
			}
		})
	}
}
