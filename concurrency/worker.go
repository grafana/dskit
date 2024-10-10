package concurrency

import (
	"errors"
	"sync"
)

type ClosedAction int

const (
	PanicWhenClosed ClosedAction = iota
	ErrorWhenClosed
	SpawnNewGoroutineWhenClosed
)

// NewReusableGoroutinesPool creates a new worker pool with the given size.
// These workers will run the workloads passed through Go() calls.
// If all workers are busy, Go() will spawn a new goroutine to run the workload.
func NewReusableGoroutinesPool(size int) *ReusableGoroutinesPool {
	p := &ReusableGoroutinesPool{
		jobs: make(chan func()),
	}
	for i := 0; i < size; i++ {
		go func() {
			for f := range p.jobs {
				f()
			}
		}()
	}
	return p
}

func (p *ReusableGoroutinesPool) WithClosedAction(action ClosedAction) *ReusableGoroutinesPool {
	p.closedAction = action
	return p
}

type ReusableGoroutinesPool struct {
	jobsMu       sync.Mutex
	closed       bool
	closedAction ClosedAction
	jobs         chan func()
}

// Go will run the given function in a worker of the pool.
// For retrocompatibility, errors will be ignored if the pool is closed.
func (p *ReusableGoroutinesPool) Go(f func()) {
	_ = p.GoErr(f)
}

// GoErr will run the given function in a worker of the pool.
// If all workers are busy, Go() will spawn a new goroutine to run the workload.
// If the pool is closed, an error will be returned and the workload will be run or dropped according to the ClosedAction.
func (p *ReusableGoroutinesPool) GoErr(f func()) error {
	p.jobsMu.Lock()
	defer p.jobsMu.Unlock()

	if p.closed {
		switch p.closedAction {
		case PanicWhenClosed:
			panic("tried to run a workload on a closed ReusableGoroutinesPool. Use a different ClosedAction to avoid this panic.")
		case ErrorWhenClosed:
			msg := "tried to run a workload on a closed ReusableGoroutinesPool, dropping the workload"
			return errors.New(msg)
		case SpawnNewGoroutineWhenClosed:
			msg := "tried to run a workload on a closed ReusableGoroutinesPool, spawning a new goroutine to run the workload"
			go f()
			return errors.New(msg)
		}
	}

	select {
	case p.jobs <- f:
	default:
		go f()
	}

	return nil
}

// Close stops the workers of the pool.
// No new Go() calls should be performed after calling Close().
// Close does NOT wait for all jobs to finish, it is the caller's responsibility to ensure that in the provided workloads.
// Close is intended to be used in tests to ensure that no goroutines are leaked.
func (p *ReusableGoroutinesPool) Close() {
	p.jobsMu.Lock()
	defer p.jobsMu.Unlock()
	p.closed = true
	close(p.jobs)
}
