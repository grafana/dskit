package ring

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Verify that updates trigger the receiver, and the latest update is observed.
func TestDelayedUpdates(t *testing.T) {
	one := 1
	two := 2

	var val *int
	storeVal := func(d *int) {
		val = d
	}

	o := newDelayedObserver(1*time.Millisecond, storeVal)

	assert.Nil(t, val, "no desc initially")
	o.flush()
	assert.Nil(t, val, "flush without update is a no-op")
	o.flush()
	assert.Nil(t, val, "multiple flushes without update is a no-op")

	o.put(&one)
	assert.Nil(t, val, "no flush immediately")
	o.flush()
	assert.Same(t, &one, val, "flush after update")
	o.flush()
	assert.Same(t, &one, val, "no change if no new update")

	o.put(&two)
	o.flush()
	assert.Same(t, &two, val, "flush after update")
	o.flush()
	assert.Same(t, &two, val, "no change if no new update")

	o.put(&one)
	o.put(&two)
	o.flush()
	assert.Same(t, &two, val, "should observe last update")
}

func TestTickUpdates(t *testing.T) {
	// This just exercises the ticker path.
	one := 1
	two := 2

	var mu sync.Mutex
	var val *int
	putVal := func(d *int) {
		mu.Lock()
		defer mu.Unlock()
		val = d
	}
	getVal := func() *int {
		mu.Lock()
		defer mu.Unlock()
		return val
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	o := newDelayedObserver(3*time.Millisecond, putVal)
	o.run(ctx)

	o.put(&one)
	assert.Eventually(t, func() bool { return getVal() == &one }, 100*time.Millisecond, 1*time.Millisecond)

	o.put(&two)
	assert.Eventually(t, func() bool { return getVal() == &two }, 100*time.Millisecond, 1*time.Millisecond)
}
