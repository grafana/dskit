package memberlist

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodedNodeMetadata_RoleAndZone(t *testing.T) {
	tests := []struct {
		name string
		role NodeRole
		zone string
	}{
		{
			name: "member with zone",
			role: NodeRoleMember,
			zone: "us-east-1a",
		},
		{
			name: "bridge with zone",
			role: NodeRoleBridge,
			zone: "eu-west-1b",
		},
		{
			name: "member with empty zone",
			role: NodeRoleMember,
			zone: "",
		},
		{
			name: "member with max length zone",
			role: NodeRoleMember,
			zone: "0123456789abcdef", // exactly 16 bytes
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := EncodeNodeMetadata(tt.role, tt.zone)
			require.NoError(t, err)

			// Test zero-allocation accessors.
			meta := EncodedNodeMetadata(encoded)
			assert.Equal(t, tt.role, meta.Role())
			assert.Equal(t, tt.zone, meta.Zone())
		})
	}
}

func TestEncodeNodeMetadata_ValidationErrors(t *testing.T) {
	tests := []struct {
		name    string
		role    NodeRole
		zone    string
		wantErr string
	}{
		{
			name:    "zone too long",
			role:    NodeRoleMember,
			zone:    "0123456789abcdef0", // 17 bytes
			wantErr: "zone name too long",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := EncodeNodeMetadata(tt.role, tt.zone)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestEncodedNodeMetadata_ZeroAllocations(t *testing.T) {
	encoded, err := EncodeNodeMetadata(NodeRoleBridge, "zone-a")
	require.NoError(t, err)

	meta := EncodedNodeMetadata(encoded)

	// Test Role() has zero allocations.
	allocs := testing.AllocsPerRun(100, func() {
		_ = meta.Role()
	})
	assert.Equal(t, float64(0), allocs, "Role() should have zero allocations")

	// Test Zone() has zero allocations.
	allocs = testing.AllocsPerRun(100, func() {
		_ = meta.Zone()
	})
	assert.Equal(t, float64(0), allocs, "Zone() should have zero allocations")
}

func TestEncodedNodeMetadata_InvalidData(t *testing.T) {
	tests := []struct {
		name         string
		data         EncodedNodeMetadata
		expectedRole NodeRole
		expectedZone string
	}{
		{
			name:         "empty data",
			data:         EncodedNodeMetadata{},
			expectedRole: NodeRoleMember,
			expectedZone: "",
		},
		{
			name:         "too short",
			data:         EncodedNodeMetadata{1, 0},
			expectedRole: NodeRoleMember,
			expectedZone: "",
		},
		{
			name:         "unknown version",
			data:         EncodedNodeMetadata{99, 0, 0},
			expectedRole: NodeRoleMember,
			expectedZone: "",
		},
		{
			name:         "truncated zone data",
			data:         EncodedNodeMetadata{1, 2, 5, 'a', 'b'}, // claims 5 bytes but only has 2
			expectedRole: NodeRoleBridge,                         // role is still readable
			expectedZone: "",                                     // but zone is not
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expectedRole, tt.data.Role())
			assert.Equal(t, tt.expectedZone, tt.data.Zone())
		})
	}
}

func BenchmarkEncodedNodeMetadata_Role(b *testing.B) {
	encoded, _ := EncodeNodeMetadata(NodeRoleBridge, "us-east-1a")
	meta := EncodedNodeMetadata(encoded)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = meta.Role()
	}
}

func BenchmarkEncodedNodeMetadata_Zone(b *testing.B) {
	encoded, _ := EncodeNodeMetadata(NodeRoleBridge, "us-east-1a")
	meta := EncodedNodeMetadata(encoded)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = meta.Zone()
	}
}
