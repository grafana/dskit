package shard

import "unsafe"

// Buf will return an unsafe pointer to a string, as the name yolo.yoloBuf implies use at your own risk.
func yoloBuf(s string) []byte {
	return *((*[]byte)(unsafe.Pointer(&s)))
}
