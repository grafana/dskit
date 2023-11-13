package ring

import (
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/httpgrpc"
)

type scenario struct {
	itemTrackers []*itemTracker
	err          error
}

func TestBatchTracker_Record(t *testing.T) {
	minSuccess := 2
	maxFailure := 1
	replicationFactor := int32(3)

	it1 := &itemTracker{minSuccess: minSuccess, maxFailures: maxFailure}
	it1.remaining.Store(replicationFactor)
	it2 := &itemTracker{minSuccess: minSuccess, maxFailures: maxFailure}
	it2.remaining.Store(replicationFactor)

	itemTrackers := []*itemTracker{it1, it2}
	bt := batchTracker{
		done: make(chan struct{}, 1),
		err:  make(chan error, 1),
	}
	bt.rpcsPending.Store(int32(len(itemTrackers)))
	httpGRPC4xx := httpgrpc.Errorf(http.StatusBadRequest, "this is an error")

	badScenario := []scenario{
		{itemTrackers: itemTrackers[:1], err: nil},
		{itemTrackers: itemTrackers[1:], err: httpGRPC4xx},
		{itemTrackers: itemTrackers[:1], err: nil},
		{itemTrackers: itemTrackers[:1], err: nil},
		{itemTrackers: itemTrackers[1:], err: httpGRPC4xx},
		{itemTrackers: itemTrackers[1:], err: httpGRPC4xx},
	}

	err := executeScenario(bt, badScenario)
	require.Error(t, err)

	goodScenario := []scenario{
		{itemTrackers: itemTrackers[:1], err: nil},
		{itemTrackers: itemTrackers[1:], err: httpGRPC4xx},
		{itemTrackers: itemTrackers[1:], err: httpGRPC4xx},
		{itemTrackers: itemTrackers[:1], err: nil},
		{itemTrackers: itemTrackers[:1], err: nil},
		{itemTrackers: itemTrackers[1:], err: httpGRPC4xx},
	}

	err = executeScenario(bt, goodScenario)
	require.Error(t, err)
}

func executeScenario(bt batchTracker, scenarios []scenario) error {
	var wg sync.WaitGroup
	wg.Add(len(scenarios))

	go func() {
		for _, sc := range scenarios {
			bt.record(sc.itemTrackers, sc.err, isHTTPStatus4xx)
			wg.Done()
		}
	}()

	go func() {
		wg.Wait()
	}()

	select {
	case err := <-bt.err:
		return err
	case <-bt.done:
		return nil
	}
}
