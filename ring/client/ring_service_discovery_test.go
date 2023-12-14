package client

import (
	"errors"
	"testing"
	"time"

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

type mockReadRing struct {
	ring.ReadRing

	mockedReplicationSet ring.ReplicationSet
	mockedErr            error
}

func (m *mockReadRing) GetAllHealthy(_ ring.Operation) (ring.ReplicationSet, error) {
	return m.mockedReplicationSet, m.mockedErr
}

func TestNewPartitionRingServiceDiscovery(t *testing.T) {
	const heartbeatTimeout = time.Minute

	tests := map[string]struct {
		partitions    map[int32]ring.PartitionDesc
		owners        map[string]ring.OwnerDesc
		expectedAddrs []string
		expectedErr   error
	}{
		"no partitions": {
			expectedErr:   nil,
			expectedAddrs: nil,
		},
		"no owners": {
			partitions: map[int32]ring.PartitionDesc{
				1: {State: ring.PartitionActive},
			},
			owners:        map[string]ring.OwnerDesc{},
			expectedErr:   ring.ErrTooManyUnhealthyInstances,
			expectedAddrs: nil,
		},
		"no healthy owners": {
			partitions: map[int32]ring.PartitionDesc{
				1: {State: ring.PartitionActive},
			},
			owners: map[string]ring.OwnerDesc{
				"1": {Addr: "1.1.1.1", Id: "1", State: ring.ACTIVE, OwnedPartition: 1, Heartbeat: time.Now().Add(-2 * heartbeatTimeout).Unix()},
			},
			expectedErr:   ring.ErrTooManyUnhealthyInstances,
			expectedAddrs: nil,
		},
		"only leaving owners": {
			partitions: map[int32]ring.PartitionDesc{
				1: {State: ring.PartitionActive},
			},
			owners: map[string]ring.OwnerDesc{
				"1": {Addr: "1.1.1.1", Id: "1", State: ring.LEAVING, OwnedPartition: 1, Heartbeat: time.Now().Unix()},
			},
			expectedAddrs: []string{"1.1.1.1"},
		},
		"inactive partition": {
			partitions: map[int32]ring.PartitionDesc{
				1: {State: ring.PartitionActive},
				2: {State: ring.PartitionInactive},
			},
			owners: map[string]ring.OwnerDesc{
				"1": {Addr: "1.1.1.1", Id: "1", State: ring.ACTIVE, OwnedPartition: 1, Heartbeat: time.Now().Unix()},
				"2": {Addr: "2.2.2.2", Id: "2", State: ring.ACTIVE, OwnedPartition: 2, Heartbeat: time.Now().Unix()},
			},
			expectedAddrs: []string{"1.1.1.1", "2.2.2.2"},
		},
		"happy path": {
			partitions: map[int32]ring.PartitionDesc{
				1: {State: ring.PartitionActive},
				2: {State: ring.PartitionActive},
			},
			owners: map[string]ring.OwnerDesc{
				"1": {Addr: "1.1.1.1", Id: "1", State: ring.ACTIVE, OwnedPartition: 1, Heartbeat: time.Now().Unix()},
				"2": {Addr: "2.2.2.2", Id: "2", State: ring.ACTIVE, OwnedPartition: 2, Heartbeat: time.Now().Unix()},
			},
			expectedAddrs: []string{"1.1.1.1", "2.2.2.2"},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			desc := ring.PartitionRingDesc{
				Partitions: testData.partitions,
				Owners:     testData.owners,
			}
			ringGetter := &mockPartitionRingGetter{ring.NewPartitionRing(desc, heartbeatTimeout)}

			discover := NewPartitionRingServiceDiscovery(ringGetter)
			addrs, err := discover()
			assert.ErrorIs(t, err, testData.expectedErr)
			assert.ElementsMatch(t, testData.expectedAddrs, addrs)
		})
	}
}

func TestNewPartitionRingServiceDiscovery_DoesntUseStaleRing(t *testing.T) {
	desc := ring.PartitionRingDesc{
		Partitions: map[int32]ring.PartitionDesc{
			1: {State: ring.PartitionActive},
		},
		Owners: map[string]ring.OwnerDesc{
			"1": {Addr: "1.1.1.1", Id: "1", State: ring.LEAVING, OwnedPartition: 1, Heartbeat: time.Now().Unix()},
		},
	}
	ringGetter := &mockPartitionRingGetter{ring.NewPartitionRing(desc, time.Minute)}

	discover := NewPartitionRingServiceDiscovery(ringGetter)
	addrs, err := discover()
	assert.NoError(t, err)
	assert.Equal(t, []string{"1.1.1.1"}, addrs)

	desc = ring.PartitionRingDesc{
		Partitions: map[int32]ring.PartitionDesc{
			1: {State: ring.PartitionActive},
		},
		Owners: map[string]ring.OwnerDesc{
			"1": {Addr: "2.2.2.2", Id: "1", State: ring.LEAVING, OwnedPartition: 1, Heartbeat: time.Now().Unix()},
		},
	}
	ringGetter.PartitionRing = ring.NewPartitionRing(desc, time.Minute)
	addrs, err = discover()
	assert.NoError(t, err)
	assert.Equal(t, []string{"2.2.2.2"}, addrs)
}

type mockPartitionRingGetter struct {
	*ring.PartitionRing
}

func (m *mockPartitionRingGetter) GetRing() *ring.PartitionRing {
	return m.PartitionRing
}
