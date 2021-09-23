package yolo

import "unsafe"

// Buf will return an unsafe pointer to a string, as the name yolo.buf implies use at your own risk.
func Buf(s string) []byte {
	return *((*[]byte)(unsafe.Pointer(&s)))
}
