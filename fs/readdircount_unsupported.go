// Provenance-includes-location: https://github.com/weaveworks/common/blob/main/fs/readdircount_unsupported.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Weaveworks Ltd.

// +build !linux !amd64

package fs

// ReadDirCount, unoptimized version
func (realFS) ReadDirCount(path string) (int, error) {
	names, err := ReadDirNames(path)
	return len(names), err
}
