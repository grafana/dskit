package proto

import (
	"bytes"
	"context"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/grafana/dskit/kv/memberlist"
)

func TestParseProtoReader(t *testing.T) {

	// 21 bytes compressed and 23 uncompressed
	req := &memberlist.KeyValuePair{
		Key:   "hello",
		Value: []byte("world"),
		Codec: "codec",
	}

	for _, tt := range []struct {
		name           string
		compression    CompressionType
		maxSize        int
		expectErr      bool
		useBytesBuffer bool
	}{
		{"rawSnappy", RawSnappy, 23, false, false},
		{"noCompression", NoCompression, 23, false, false},
		{"too big rawSnappy", RawSnappy, 20, true, false},
		{"too big decoded rawSnappy", RawSnappy, 10, true, false},
		{"too big noCompression", NoCompression, 10, true, false},

		{"bytesbuffer rawSnappy", RawSnappy, 23, false, true},
		{"bytesbuffer noCompression", NoCompression, 23, false, true},
		{"bytesbuffer too big rawSnappy", RawSnappy, 20, true, true},
		{"bytesbuffer too big decoded rawSnappy", RawSnappy, 10, true, true},
		{"bytesbuffer too big noCompression", NoCompression, 10, true, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			assert.Nil(t, SerializeProtoResponse(w, req, tt.compression))
			var fromWire memberlist.KeyValuePair

			reader := w.Result().Body
			if tt.useBytesBuffer {
				buf := bytes.Buffer{}
				_, err := buf.ReadFrom(reader)
				assert.Nil(t, err)
				reader = bytesBuffered{Buffer: &buf}
			}

			err := ParseProtoReader(context.Background(), reader, 0, tt.maxSize, &fromWire, tt.compression)
			if tt.expectErr {
				assert.NotNil(t, err)
				return
			}
			assert.Nil(t, err)
			assert.Equal(t, req, &fromWire)
		})
	}
}

type bytesBuffered struct {
	*bytes.Buffer
}

func (b bytesBuffered) Close() error {
	return nil
}

func (b bytesBuffered) BytesBuffer() *bytes.Buffer {
	return b.Buffer
}
