package memberlist

import (
	"bytes"
	"testing"

	"github.com/hashicorp/memberlist"
	"github.com/stretchr/testify/require"
)

func TestInvalidates(t *testing.T) {
	const key = "ring"

	logger := testLogger{}
	messages := map[string]ringBroadcast{
		"b1": {key: key, content: []string{"A", "B", "C"}, version: 1, logger: logger},
		"b2": {key: key, content: []string{"A", "B", "C"}, version: 2, logger: logger},
		"b3": {key: key, content: []string{"A"}, version: 3, logger: logger},
		"b4": {key: key, content: []string{"A", "B"}, version: 4, logger: logger},
		"b5": {key: key, content: []string{"A", "B", "D"}, version: 5, logger: logger},
		"b6": {key: key, content: []string{"A", "B", "C", "D"}, version: 6, logger: logger},
	}

	checkInvalidate(t, messages, "b2", "b1", true, false)
	checkInvalidate(t, messages, "b3", "b1", false, false)
	checkInvalidate(t, messages, "b3", "b2", false, false)
	checkInvalidate(t, messages, "b4", "b1", false, false)
	checkInvalidate(t, messages, "b4", "b2", false, false)
	checkInvalidate(t, messages, "b4", "b3", true, false)
	checkInvalidate(t, messages, "b5", "b1", false, false)
	checkInvalidate(t, messages, "b5", "b2", false, false)
	checkInvalidate(t, messages, "b5", "b3", true, false)
	checkInvalidate(t, messages, "b5", "b4", true, false)
	checkInvalidate(t, messages, "b6", "b1", true, false)
	checkInvalidate(t, messages, "b6", "b2", true, false)
	checkInvalidate(t, messages, "b6", "b3", true, false)
	checkInvalidate(t, messages, "b6", "b4", true, false)
	checkInvalidate(t, messages, "b6", "b5", true, false)
}

func checkInvalidate(t *testing.T, messages map[string]ringBroadcast, key1, key2 string, firstInvalidatesSecond, secondInvalidatesFirst bool) {
	b1, ok := messages[key1]
	if !ok {
		t.Fatal("cannot find", key1)
	}

	b2, ok := messages[key2]
	if !ok {
		t.Fatal("cannot find", key2)
	}

	if b1.Invalidates(b2) != firstInvalidatesSecond {
		t.Errorf("%s.Invalidates(%s) returned %t. %s={%v, %d}, %s={%v, %d}", key1, key2, !firstInvalidatesSecond, key1, b1.content, b1.version, key2, b2.content, b2.version)
	}

	if b2.Invalidates(b1) != secondInvalidatesFirst {
		t.Errorf("%s.Invalidates(%s) returned %t. %s={%v, %d}, %s={%v, %d}", key2, key1, !secondInvalidatesFirst, key2, b2.content, b2.version, key1, b1.content, b1.version)
	}
}

func TestTransmitLimitedQueue(t *testing.T) {
	broadcasts := &memberlist.TransmitLimitedQueue{RetransmitMult: 3, NumNodes: func() int { return 5 }}

	// This message gets assigned internal id 1 by TransmitLimitedQueue.
	broadcasts.QueueBroadcast(ringBroadcast{key: key, content: []string{"A"}, version: 1, msg: []byte("A timestamp update")})

	// This message will have internal id 2. It invalidates the previous message, because "content" is the same and it has newer version (see (ringBroadcast).Invalidates method).
	// During invalidation TransmitLimitedQueue will reset its "id" generator to 0.
	broadcasts.QueueBroadcast(ringBroadcast{key: key, content: []string{"A"}, version: 2, msg: []byte("A left"), logger: testLogger{}})

	// New incoming message, it will get internal id 1.
	broadcasts.QueueBroadcast(ringBroadcast{key: key, content: []string{"B"}, version: 10, msg: []byte("B timestamp update")})

	// Another incoming message, with internal id 2.
	// Since this message has same internal id, number of transmits (0) and message length, it will REPLACE previous message with id=2.
	// (See (*limitedBroadcast).Less function for details.)
	broadcasts.QueueBroadcast(ringBroadcast{key: key, content: []string{"C"}, version: 20, msg: []byte("C left")})

	// We expect messages "A left, B timestamp update, C left", but due to bug with resetting id, our "A left" message was overwritten by "C left".
	t.Log("queued:", broadcasts.NumQueued())
	messages := broadcasts.GetBroadcasts(0, 1024)
	t.Log(string(bytes.Join(messages, []byte(", "))))

	// We expect 3 messages.
	require.Equal(t, 3, broadcasts.NumQueued())
}
