// Provenance-includes-location: https://github.com/weaveworks/common/blob/main/server/tls_config_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Weaveworks Ltd.

package server

import (
	"crypto/tls"
	"testing"

	"github.com/prometheus/exporter-toolkit/web"
	"github.com/stretchr/testify/require"
)

func Test_stringToCipherSuites(t *testing.T) {
	tests := []struct {
		name    string
		arg     string
		want    []web.Cipher
		wantErr bool
	}{
		{name: "blank", arg: "", want: nil},
		{name: "bad", arg: "not-a-cipher", wantErr: true},
		{name: "one", arg: "TLS_AES_256_GCM_SHA384", want: []web.Cipher{web.Cipher(tls.TLS_AES_256_GCM_SHA384)}},
		{name: "two", arg: "TLS_AES_256_GCM_SHA384,TLS_CHACHA20_POLY1305_SHA256",
			want: []web.Cipher{web.Cipher(tls.TLS_AES_256_GCM_SHA384), web.Cipher(tls.TLS_CHACHA20_POLY1305_SHA256)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stringToCipherSuites(tt.arg)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.want, got)
		})
	}
}

func Test_stringToTLSVersion(t *testing.T) {
	tests := []struct {
		name    string
		arg     string
		want    web.TLSVersion
		wantErr bool
	}{
		{name: "blank", arg: "", want: 0},
		{name: "bad", arg: "not-a-version", wantErr: true},
		{name: "VersionTLS12", arg: "VersionTLS12", want: tls.VersionTLS12},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stringToTLSVersion(tt.arg)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.want, got)
		})
	}
}
