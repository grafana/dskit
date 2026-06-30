package client

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/grafana/dskit/ring"
)

func TestNewRingServiceDiscovery(t *testing.T) {
	tests := map[string]struct {
		ringReplicationSet ring.ReplicationSet
		ringErr            error
		expectedAddrs      []string
		expectedErr        error
	}{
		"discovery failure": {
			ringErr:     errors.New("mocked error"),
			expectedErr: errors.New("mocked error"),
		},
		"empty ring": {
			ringErr:       ring.ErrEmptyRing,
			expectedAddrs: nil,
		},
		"empty replication set": {
			ringReplicationSet: ring.ReplicationSet{
				Instances: []ring.InstanceDesc{},
			},
			expectedAddrs: nil,
		},
		"replication containing some endpoints": {
			ringReplicationSet: ring.ReplicationSet{
				Instances: []ring.InstanceDesc{
					{Addr: "1.1.1.1"},
					{Addr: "2.2.2.2"},
				},
			},
			expectedAddrs: []string{"1.1.1.1", "2.2.2.2"},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			r := &mockReadRing{}
			r.mockedReplicationSet = testData.ringReplicationSet
			r.mockedErr = testData.ringErr

			d := NewRingServiceDiscovery(r)
			addrs, err := d()
			assert.Equal(t, testData.expectedErr, err)
			assert.Equal(t, testData.expectedAddrs, addrs)
		})
	}
}

func TestNewRingsServiceDiscovery(t *testing.T) {
	tests := map[string]struct {
		rings         []*mockReadRing
		expectedAddrs []string
		expectedErr   error
	}{
		"no rings": {
			rings:         nil,
			expectedAddrs: nil,
		},
		"single ring with multiple instances": {
			rings: []*mockReadRing{
				{mockedReplicationSet: ring.ReplicationSet{Instances: []ring.InstanceDesc{
					{Addr: "1.1.1.1"},
					{Addr: "2.2.2.2"},
				}}},
			},
			expectedAddrs: []string{"1.1.1.1", "2.2.2.2"},
		},
		"multiple rings with disjoint instances": {
			rings: []*mockReadRing{
				{mockedReplicationSet: ring.ReplicationSet{Instances: []ring.InstanceDesc{
					{Addr: "1.1.1.1"},
					{Addr: "2.2.2.2"},
				}}},
				{mockedReplicationSet: ring.ReplicationSet{Instances: []ring.InstanceDesc{
					{Addr: "3.3.3.3"},
				}}},
			},
			expectedAddrs: []string{"1.1.1.1", "2.2.2.2", "3.3.3.3"},
		},
		"multiple rings with overlapping instances are deduplicated": {
			rings: []*mockReadRing{
				{mockedReplicationSet: ring.ReplicationSet{Instances: []ring.InstanceDesc{
					{Addr: "1.1.1.1"},
					{Addr: "2.2.2.2"},
				}}},
				{mockedReplicationSet: ring.ReplicationSet{Instances: []ring.InstanceDesc{
					{Addr: "2.2.2.2"},
					{Addr: "3.3.3.3"},
				}}},
			},
			expectedAddrs: []string{"1.1.1.1", "2.2.2.2", "3.3.3.3"},
		},
		"duplicate instances within a single ring are deduplicated": {
			rings: []*mockReadRing{
				{mockedReplicationSet: ring.ReplicationSet{Instances: []ring.InstanceDesc{
					{Addr: "1.1.1.1"},
					{Addr: "1.1.1.1"},
				}}},
			},
			expectedAddrs: []string{"1.1.1.1"},
		},
		"all rings are empty": {
			rings: []*mockReadRing{
				{mockedErr: ring.ErrEmptyRing},
				{mockedErr: ring.ErrEmptyRing},
			},
			expectedAddrs: nil,
		},
		"one ring fails discovery": {
			rings: []*mockReadRing{
				{mockedReplicationSet: ring.ReplicationSet{Instances: []ring.InstanceDesc{
					{Addr: "1.1.1.1"},
				}}},
				{mockedErr: errors.New("mocked error")},
			},
			expectedErr: errors.New("mocked error"),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			rings := make([]ring.ReadRing, len(testData.rings))
			for i, r := range testData.rings {
				rings[i] = r
			}

			d := NewRingsServiceDiscovery(rings)
			addrs, err := d()
			assert.Equal(t, testData.expectedErr, err)
			assert.Equal(t, testData.expectedAddrs, addrs)
		})
	}
}

type mockReadRing struct {
	ring.ReadRing

	mockedReplicationSet ring.ReplicationSet
	mockedErr            error
}

func (m *mockReadRing) GetAllHealthy(_ ring.Operation) (ring.ReplicationSet, error) {
	return m.mockedReplicationSet, m.mockedErr
}
