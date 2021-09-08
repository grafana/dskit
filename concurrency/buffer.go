package concurrency

import (
	"bytes"
	"sync"
)

// SyncBuffer is a struct that contains a buffer and a mutex lock to allow for synchronous access to the contained buffer.
type SyncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

// Write will write bytes to SyncBuffer within a mutex lock.
func (sb *SyncBuffer) Write(p []byte) (n int, err error) {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	return sb.buf.Write(p)
}

// String returns the SyncBuffer contents as a string.
func (sb *SyncBuffer) String() string {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	return sb.buf.String()
}
