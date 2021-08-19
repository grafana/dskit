package memberlist

type testLogger struct {
}

func (l testLogger) Log(keyvals ...interface{}) error {
	return nil
}
