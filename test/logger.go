package test

import (
	"testing"
	"time"
)

type TestingLogger struct {
	t            testing.TB
	extraKeyvals []interface{}
}

func NewTestingLogger(t testing.TB, extraKeyvals ...interface{}) *TestingLogger {
	return &TestingLogger{
		t:            t,
		extraKeyvals: extraKeyvals,
	}
}

func (l *TestingLogger) Log(keyvals ...interface{}) error {
	// Prepend log with timestamp.
	mergedKeyvals := []interface{}{time.Now().String()}
	mergedKeyvals = append(mergedKeyvals, l.extraKeyvals...)
	mergedKeyvals = append(mergedKeyvals, keyvals...)

	l.t.Log(mergedKeyvals...)
	return nil
}
