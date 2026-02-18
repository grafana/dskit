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
	name               string
	headerValue        *string
	errTenantID        error
	errTenantIDs       error
	errMetadata        error
	errMetadataContain string // for checking error message contains this string
	tenantID           string
	tenantIDs          []string
	metadata           *Metadata
}

func TestTenantIDs(t *testing.T) {
	for _, tc := range []resolverTestCase{
		{
			name:         "no-header",
			errTenantID:  user.ErrNoOrgID,
			errTenantIDs: user.ErrNoOrgID,
			errMetadata:  user.ErrNoOrgID,
		},
		{
			name:        "empty",
			headerValue: strptr(""),
			tenantIDs:   []string{""},
			metadata:    nil,
		},
		{
			name:        "single-tenant",
			headerValue: strptr("tenant-a"),
			tenantID:    "tenant-a",
			tenantIDs:   []string{"tenant-a"},
			metadata:    nil,
		},
		{
			name:         "parent-dir",
			headerValue:  strptr(".."),
			errTenantID:  errUnsafeTenantID,
			errTenantIDs: errUnsafeTenantID,
			errMetadata:  errUnsafeTenantID,
		},
		{
			name:         "current-dir",
			headerValue:  strptr("."),
			errTenantID:  errUnsafeTenantID,
			errTenantIDs: errUnsafeTenantID,
			errMetadata:  errUnsafeTenantID,
		},
		{
			name:        "multi-tenant",
			headerValue: strptr("tenant-a|tenant-b"),
			errTenantID: user.ErrTooManyOrgIDs,
			tenantIDs:   []string{"tenant-a", "tenant-b"},
			errMetadata: user.ErrTooManyOrgIDs,
		},
		{
			name:        "multi-tenant-wrong-order",
			headerValue: strptr("tenant-b|tenant-a"),
			errTenantID: user.ErrTooManyOrgIDs,
			tenantIDs:   []string{"tenant-a", "tenant-b"},
			errMetadata: user.ErrTooManyOrgIDs,
		},
		{
			name:        "multi-tenant-duplicate-order",
			headerValue: strptr("tenant-b|tenant-b|tenant-a"),
			errTenantID: user.ErrTooManyOrgIDs,
			tenantIDs:   []string{"tenant-a", "tenant-b"},
			errMetadata: user.ErrTooManyOrgIDs,
		},
		{
			// Duplicated single tenant should return that tenant (backward compatible)
			name:        "multi-tenant-same-tenant-duplicated",
			headerValue: strptr("tenant-a|tenant-a"),
			tenantID:    "tenant-a",
			tenantIDs:   []string{"tenant-a"},
			metadata:    nil,
		},
		{
			// Duplicated single tenant with metadata
			name:        "multi-tenant-same-tenant-with-metadata-duplicated",
			headerValue: strptr("tenant-a:key=value|tenant-a:key=value"),
			tenantID:    "tenant-a",
			tenantIDs:   []string{"tenant-a"},
			metadata:    &Metadata{Key: "key", Value: "value"},
		},
		{
			// TenantID/TenantWithMetadata return early when different tenants found, before validating all
			name:         "multi-tenant-with-relative-path",
			headerValue:  strptr("tenant-a|tenant-b|.."),
			errTenantID:  user.ErrTooManyOrgIDs,
			errTenantIDs: errUnsafeTenantID,
			errMetadata:  user.ErrTooManyOrgIDs,
		},
		{
			name:         "containing-forward-slash",
			headerValue:  strptr("forward/slash"),
			errTenantID:  &errTenantIDUnsupportedCharacter{pos: 7, tenantID: "forward/slash"},
			errTenantIDs: &errTenantIDUnsupportedCharacter{pos: 7, tenantID: "forward/slash"},
			errMetadata:  &errTenantIDUnsupportedCharacter{pos: 7, tenantID: "forward/slash"},
		},
		{
			name:         "containing-backward-slash",
			headerValue:  strptr(`backward\slash`),
			errTenantID:  &errTenantIDUnsupportedCharacter{pos: 8, tenantID: "backward\\slash"},
			errTenantIDs: &errTenantIDUnsupportedCharacter{pos: 8, tenantID: "backward\\slash"},
			errMetadata:  &errTenantIDUnsupportedCharacter{pos: 8, tenantID: "backward\\slash"},
		},
		{
			name:         "too-long",
			headerValue:  strptr(strings.Repeat("123", MaxTenantIDLength)),
			errTenantID:  errTenantIDTooLong,
			errTenantIDs: errTenantIDTooLong,
			errMetadata:  errTenantIDTooLong,
		},
		// Metadata test cases
		{
			name:        "tenant-with-metadata",
			headerValue: strptr("123456:key=value"),
			tenantID:    "123456",
			tenantIDs:   []string{"123456"},
			metadata:    &Metadata{Key: "key", Value: "value"},
		},
		{
			name:        "tenant-with-metadata-complex",
			headerValue: strptr("my-tenant-id:product=k6"),
			tenantID:    "my-tenant-id",
			tenantIDs:   []string{"my-tenant-id"},
			metadata:    &Metadata{Key: "product", Value: "k6"},
		},
		{
			name:        "tenant-with-empty-metadata",
			headerValue: strptr("tenant-a:"),
			tenantID:    "tenant-a",
			tenantIDs:   []string{"tenant-a"},
			metadata:    nil,
		},
		{
			// TenantID/TenantIDs don't validate metadata, only TenantWithMetadata does
			name:        "invalid-metadata-with-slash",
			headerValue: strptr("tenant-a:key/value"),
			tenantID:    "tenant-a",
			tenantIDs:   []string{"tenant-a"},
			errMetadata: &errMetadataUnsupportedCharacter{pos: 3, metadata: "key/value"},
		},
		{
			// TenantID/TenantIDs don't validate metadata, only TenantWithMetadata does
			name:               "invalid-metadata-too-long",
			headerValue:        strptr("tenant-a:" + strings.Repeat("x", MaxMetadataLength+1)),
			tenantID:           "tenant-a",
			tenantIDs:          []string{"tenant-a"},
			errMetadataContain: "metadata too long",
		},
		{
			// Metadata missing = separator
			name:               "invalid-metadata-no-equals",
			headerValue:        strptr("tenant-a:keyvalue"),
			tenantID:           "tenant-a",
			tenantIDs:          []string{"tenant-a"},
			errMetadataContain: "invalid metadata string",
		},
		{
			name:        "multi-tenant-with-metadata",
			headerValue: strptr("tenant-a|tenant-b:key=value"),
			errTenantID: user.ErrTooManyOrgIDs,
			tenantIDs:   []string{"tenant-a", "tenant-b"},
			errMetadata: user.ErrTooManyOrgIDs,
		},
		{
			// Multiple tenants with the same metadata is supported.
			name:        "multi-tenant-with-same-metadata",
			headerValue: strptr("123123:key=k6|1234515:key=k6"),
			errTenantID: user.ErrTooManyOrgIDs,
			tenantIDs:   []string{"123123", "1234515"},
			errMetadata: user.ErrTooManyOrgIDs,
		},
		{
			// Multiple tenants with different metadata.
			name:        "multi-tenant-with-different-metadata",
			headerValue: strptr("123123:key=k6|1234515:key=k7"),
			errTenantID: user.ErrTooManyOrgIDs,
			tenantIDs:   []string{"123123", "1234515"},
			errMetadata: user.ErrTooManyOrgIDs,
		},
		{
			// Mixed: some tenants with metadata, some without.
			name:        "multi-tenant-mixed-metadata",
			headerValue: strptr("tenant-a|tenant-b:key=value"),
			errTenantID: user.ErrTooManyOrgIDs,
			tenantIDs:   []string{"tenant-a", "tenant-b"},
			errMetadata: user.ErrTooManyOrgIDs,
		},
		{
			// Each tenant has its own metadata declaration but they're the same.
			name:        "multi-tenant-each-with-same-metadata",
			headerValue: strptr("tenant-a:key=k6|tenant-b:key=k6|tenant-c:key=k6"),
			errTenantID: user.ErrTooManyOrgIDs,
			tenantIDs:   []string{"tenant-a", "tenant-b", "tenant-c"},
			errMetadata: user.ErrTooManyOrgIDs,
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

			tenantIDFromMeta, metadata, err := TenantWithMetadata(ctx)
			if tc.errMetadata != nil {
				assert.Equal(t, tc.errMetadata, err)
			} else if tc.errMetadataContain != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.errMetadataContain)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.tenantID, tenantIDFromMeta)
				assert.Equal(t, tc.metadata, metadata)
			}
		})
	}
}
