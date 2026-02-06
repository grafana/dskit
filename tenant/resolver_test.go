package tenant

import (
	"context"
	"strings"
	"testing"

	"github.com/grafana/dskit/user"

	"github.com/stretchr/testify/assert"
)

func strptr(s string) *string {
	return &s
}

type resolverTestCase struct {
	name            string
	headerValue     *string
	errTenantID     error
	errTenantIDs    error
	errSubtenantID  error
	errSubtenantIDs error
	tenantID        string
	tenantIDs       []string
	subtenantID     string
	subtenantIDs    []string
}

func TestTenantIDs(t *testing.T) {
	for _, tc := range []resolverTestCase{
		{
			name:            "no-header",
			errTenantID:     user.ErrNoOrgID,
			errTenantIDs:    user.ErrNoOrgID,
			errSubtenantID:  user.ErrNoOrgID,
			errSubtenantIDs: user.ErrNoOrgID,
		},
		{
			name:        "empty",
			headerValue: strptr(""),
			tenantIDs:   []string{""},
			subtenantID: "",
		},
		{
			name:        "single-tenant",
			headerValue: strptr("tenant-a"),
			tenantID:    "tenant-a",
			tenantIDs:   []string{"tenant-a"},
			subtenantID: "",
		},
		{
			name:            "parent-dir",
			headerValue:     strptr(".."),
			errTenantID:     errUnsafeTenantID,
			errTenantIDs:    errUnsafeTenantID,
			errSubtenantID:  errUnsafeTenantID,
			errSubtenantIDs: errUnsafeTenantID,
		},
		{
			name:            "current-dir",
			headerValue:     strptr("."),
			errTenantID:     errUnsafeTenantID,
			errTenantIDs:    errUnsafeTenantID,
			errSubtenantID:  errUnsafeTenantID,
			errSubtenantIDs: errUnsafeTenantID,
		},
		{
			name:           "multi-tenant",
			headerValue:    strptr("tenant-a|tenant-b"),
			errTenantID:    user.ErrTooManyOrgIDs,
			tenantIDs:      []string{"tenant-a", "tenant-b"},
			errSubtenantID: user.ErrTooManyOrgIDs,
		},
		{
			name:           "multi-tenant-wrong-order",
			headerValue:    strptr("tenant-b|tenant-a"),
			errTenantID:    user.ErrTooManyOrgIDs,
			tenantIDs:      []string{"tenant-a", "tenant-b"},
			errSubtenantID: user.ErrTooManyOrgIDs,
		},
		{
			name:           "multi-tenant-duplicate-order",
			headerValue:    strptr("tenant-b|tenant-b|tenant-a"),
			errTenantID:    user.ErrTooManyOrgIDs,
			tenantIDs:      []string{"tenant-a", "tenant-b"},
			errSubtenantID: user.ErrTooManyOrgIDs,
		},
		{
			// Duplicated single tenant should return that tenant (backward compatible)
			name:         "multi-tenant-same-tenant-duplicated",
			headerValue:  strptr("tenant-a|tenant-a"),
			tenantID:     "tenant-a",
			tenantIDs:    []string{"tenant-a"},
			subtenantID:  "",
			subtenantIDs: nil,
		},
		{
			// Duplicated single tenant with subtenant
			name:         "multi-tenant-same-tenant-with-subtenant-duplicated",
			headerValue:  strptr("tenant-a:k6|tenant-a:k6"),
			tenantID:     "tenant-a",
			tenantIDs:    []string{"tenant-a"},
			subtenantID:  "k6",
			subtenantIDs: []string{"k6"},
		},
		{
			// TenantID/SubtenantID return early when different tenants found, before validating all
			name:            "multi-tenant-with-relative-path",
			headerValue:     strptr("tenant-a|tenant-b|.."),
			errTenantID:     user.ErrTooManyOrgIDs,
			errTenantIDs:    errUnsafeTenantID,
			errSubtenantID:  user.ErrTooManyOrgIDs,
			errSubtenantIDs: errUnsafeTenantID,
		},
		{
			name:            "containing-forward-slash",
			headerValue:     strptr("forward/slash"),
			errTenantID:     &errTenantIDUnsupportedCharacter{pos: 7, tenantID: "forward/slash"},
			errTenantIDs:    &errTenantIDUnsupportedCharacter{pos: 7, tenantID: "forward/slash"},
			errSubtenantID:  &errTenantIDUnsupportedCharacter{pos: 7, tenantID: "forward/slash"},
			errSubtenantIDs: &errTenantIDUnsupportedCharacter{pos: 7, tenantID: "forward/slash"},
		},
		{
			name:            "containing-backward-slash",
			headerValue:     strptr(`backward\slash`),
			errTenantID:     &errTenantIDUnsupportedCharacter{pos: 8, tenantID: "backward\\slash"},
			errTenantIDs:    &errTenantIDUnsupportedCharacter{pos: 8, tenantID: "backward\\slash"},
			errSubtenantID:  &errTenantIDUnsupportedCharacter{pos: 8, tenantID: "backward\\slash"},
			errSubtenantIDs: &errTenantIDUnsupportedCharacter{pos: 8, tenantID: "backward\\slash"},
		},
		{
			name:            "too-long",
			headerValue:     strptr(strings.Repeat("123", MaxTenantIDLength)),
			errTenantID:     errTenantIDTooLong,
			errTenantIDs:    errTenantIDTooLong,
			errSubtenantID:  errTenantIDTooLong,
			errSubtenantIDs: errTenantIDTooLong,
		},
		// Subtenant test cases
		{
			name:         "tenant-with-subtenant",
			headerValue:  strptr("123456:k6"),
			tenantID:     "123456",
			tenantIDs:    []string{"123456"},
			subtenantID:  "k6",
			subtenantIDs: []string{"k6"},
		},
		{
			name:         "tenant-with-subtenant-complex",
			headerValue:  strptr("my-tenant-id:my-subtenant"),
			tenantID:     "my-tenant-id",
			tenantIDs:    []string{"my-tenant-id"},
			subtenantID:  "my-subtenant",
			subtenantIDs: []string{"my-subtenant"},
		},
		{
			name:        "tenant-with-empty-subtenant",
			headerValue: strptr("tenant-a:"),
			tenantID:    "tenant-a",
			tenantIDs:   []string{"tenant-a"},
			subtenantID: "",
		},
		{
			// TenantID/TenantIDs don't validate subtenant, only SubtenantID/SubtenantIDs do
			name:            "invalid-subtenant-with-slash",
			headerValue:     strptr("tenant-a:sub/tenant"),
			tenantID:        "tenant-a",
			tenantIDs:       []string{"tenant-a"},
			errSubtenantID:  &errTenantIDUnsupportedCharacter{pos: 3, tenantID: "sub/tenant"},
			errSubtenantIDs: &errTenantIDUnsupportedCharacter{pos: 3, tenantID: "sub/tenant"},
		},
		{
			// TenantID/TenantIDs don't validate subtenant, only SubtenantID/SubtenantIDs do
			name:            "invalid-subtenant-too-long",
			headerValue:     strptr("tenant-a:" + strings.Repeat("x", MaxTenantIDLength+1)),
			tenantID:        "tenant-a",
			tenantIDs:       []string{"tenant-a"},
			errSubtenantID:  errTenantIDTooLong,
			errSubtenantIDs: errTenantIDTooLong,
		},
		{
			// TenantID/TenantIDs don't validate subtenant, only SubtenantID/SubtenantIDs do
			name:            "invalid-subtenant-parent-dir",
			headerValue:     strptr("tenant-a:.."),
			tenantID:        "tenant-a",
			tenantIDs:       []string{"tenant-a"},
			errSubtenantID:  errUnsafeTenantID,
			errSubtenantIDs: errUnsafeTenantID,
		},
		{
			name:           "multi-tenant-with-subtenant",
			headerValue:    strptr("tenant-a|tenant-b:k6"),
			errTenantID:    user.ErrTooManyOrgIDs,
			tenantIDs:      []string{"tenant-a", "tenant-b"},
			errSubtenantID: user.ErrTooManyOrgIDs,
			subtenantIDs:   []string{"k6"},
		},
		{
			// Multiple tenants with the same subtenant is supported.
			name:           "multi-tenant-with-same-subtenant",
			headerValue:    strptr("123123:k6|1234515:k6"),
			errTenantID:    user.ErrTooManyOrgIDs,
			tenantIDs:      []string{"123123", "1234515"},
			errSubtenantID: user.ErrTooManyOrgIDs,
			subtenantIDs:   []string{"k6"},
		},
		{
			// Multiple tenants with different subtenants.
			name:           "multi-tenant-with-different-subtenants",
			headerValue:    strptr("123123:k6|1234515:k7"),
			errTenantID:    user.ErrTooManyOrgIDs,
			tenantIDs:      []string{"123123", "1234515"},
			errSubtenantID: user.ErrTooManyOrgIDs,
			subtenantIDs:   []string{"k6", "k7"},
		},
		{
			// Mixed: some tenants with subtenant, some without.
			name:           "multi-tenant-mixed-subtenant",
			headerValue:    strptr("tenant-a|tenant-b:k6"),
			errTenantID:    user.ErrTooManyOrgIDs,
			tenantIDs:      []string{"tenant-a", "tenant-b"},
			errSubtenantID: user.ErrTooManyOrgIDs,
			subtenantIDs:   []string{"k6"},
		},
		{
			// Each tenant has its own subtenant declaration but they're the same.
			name:           "multi-tenant-each-with-same-subtenant",
			headerValue:    strptr("tenant-a:k6|tenant-b:k6|tenant-c:k6"),
			errTenantID:    user.ErrTooManyOrgIDs,
			tenantIDs:      []string{"tenant-a", "tenant-b", "tenant-c"},
			errSubtenantID: user.ErrTooManyOrgIDs,
			subtenantIDs:   []string{"k6"},
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

			tenantIDFromSub, subtenantID, err := SubtenantID(ctx)
			if tc.errSubtenantID != nil {
				assert.Equal(t, tc.errSubtenantID, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.tenantID, tenantIDFromSub)
				assert.Equal(t, tc.subtenantID, subtenantID)
			}

			subtenantIDs, err := SubtenantIDs(ctx)
			if tc.errSubtenantIDs != nil {
				assert.Equal(t, tc.errSubtenantIDs, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.subtenantIDs, subtenantIDs)
			}
		})
	}
}
