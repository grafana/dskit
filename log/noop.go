// Provenance-includes-location: https://github.com/weaveworks/common/blob/main/logging/noop.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Weaveworks Ltd.

package log

// Noop logger.
func Noop() Interface {
	return noop{}
}

type noop struct{}

func (noop) Debugf(format string, args ...interface{}) {}
func (noop) Debugln(args ...interface{})               {}
func (noop) Infof(format string, args ...interface{})  {}
func (noop) Infoln(args ...interface{})                {}
func (noop) Warnf(format string, args ...interface{})  {}
func (noop) Warnln(args ...interface{})                {}
func (noop) Errorf(format string, args ...interface{}) {}
func (noop) Errorln(args ...interface{})               {}
func (noop) WithField(key string, value interface{}) Interface {
	return noop{}
}
func (noop) WithFields(Fields) Interface {
	return noop{}
}
