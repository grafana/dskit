package ring

import (
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

	o.observeUpdate(&one)
	assert.Nil(t, val, "no flush immediately")
	o.flush()
	assert.Same(t, &one, val, "flush after update")
	o.flush()
	assert.Same(t, &one, val, "no change if no new update")

	o.observeUpdate(&two)
	o.flush()
	assert.Same(t, &two, val, "flush after update")
	o.flush()
	assert.Same(t, &two, val, "no change if no new update")

	o.observeUpdate(&one)
	o.observeUpdate(&two)
	o.flush()
	assert.Same(t, &two, val, "should observe last update")
}
