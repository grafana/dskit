// GENERATED CODE - DO NOT EDIT
// This file was generated by protoc-gen-fastmarshal

package kv

import (
	"fmt"
	"sync/atomic"
	"github.com/CrowdStrike/csproto"
)

//------------------------------------------------------------------------------
// Custom Protobuf size/marshal/unmarshal code for MockMessage

// Size calculates and returns the size, in bytes, required to hold the contents of m using the Protobuf
// binary encoding.
func (m *MockMessage) Size() int {
	// nil message is always 0 bytes
	if m == nil {
		return 0
	}
	// return cached size, if present
	if csz := int(atomic.LoadInt32(&m.sizeCache)); csz > 0 {
		return csz
	}
	// calculate and cache
	var sz, l int
	_ = l // avoid unused variable

	// Id (string,optional)
	if l = len(m.Id); l > 0 {
		sz += csproto.SizeOfTagKey(1) + csproto.SizeOfVarint(uint64(l)) + l
	}
	// cache the size so it can be re-used in Marshal()/MarshalTo()
	atomic.StoreInt32(&m.sizeCache, int32(sz))
	return sz
}

// Marshal converts the contents of m to the Protobuf binary encoding and returns the result or an error.
func (m *MockMessage) Marshal() ([]byte, error) {
	siz := m.Size()
	if siz == 0 {
		return []byte{}, nil
	}
	buf := make([]byte, siz)
	err := m.MarshalTo(buf)
	return buf, err
}

// MarshalTo converts the contents of m to the Protobuf binary encoding and writes the result to dest.
func (m *MockMessage) MarshalTo(dest []byte) error {
	// nil message == no-op
	if m == nil {
		return nil
	}
	var (
		enc    = csproto.NewEncoder(dest)
		buf    []byte
		err    error
		extVal interface{}
	)
	// ensure no unused variables
	_ = enc
	_ = buf
	_ = err
	_ = extVal

	// Id (1,string,optional)
	if len(m.Id) > 0 {
		enc.EncodeString(1, m.Id)
	}
	return nil
}

// Unmarshal decodes a binary encoded Protobuf message from p and populates m with the result.
func (m *MockMessage) Unmarshal(p []byte) error {
	m.Reset()
	if len(p) == 0 {
		return nil
	}
	dec := csproto.NewDecoder(p)
	for dec.More() {
		tag, wt, err := dec.DecodeTag()
		if err != nil {
			return err
		}
		switch tag {
		case 1: // Id (string,optional)
			if wt != csproto.WireTypeLengthDelimited {
				return fmt.Errorf("incorrect wire type %v for field 'id' (tag=1), expected 2 (length-delimited)", wt)
			}
			if s, err := dec.DecodeString(); err != nil {
				return fmt.Errorf("unable to decode string value for field 'id' (tag=1): %w", err)
			} else {
				m.Id = s
			}

		default:
			if skipped, err := dec.Skip(tag, wt); err != nil {
				return fmt.Errorf("invalid operation skipping tag %v: %w", tag, err)
			} else {
				m.unknownFields = append(m.unknownFields, skipped...)
			}
		}
	}
	return nil
}
