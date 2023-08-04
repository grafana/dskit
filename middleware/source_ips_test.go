// Provenance-includes-location: https://github.com/weaveworks/common/blob/main/middleware/source_ips_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Weaveworks Ltd.

package middleware

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetSourceIPs(t *testing.T) {
	tests := []struct {
		name string
		req  *http.Request
		want string
	}{
		{
			name: "no header",
			req:  &http.Request{RemoteAddr: "192.168.1.100:3454"},
			want: "192.168.1.100",
		},
		{
			name: "no header and remote has no port",
			req:  &http.Request{RemoteAddr: "192.168.1.100"},
			want: "192.168.1.100",
		},
		{
			name: "no header, remote address is invalid",
			req:  &http.Request{RemoteAddr: "192.168.100"},
			want: "192.168.100",
		},
		{
			name: "X-Forwarded-For and single forward address",
			req: &http.Request{
				RemoteAddr: "192.168.1.100:3454",
				Header: map[string][]string{
					http.CanonicalHeaderKey(xForwardedFor): {"172.16.1.1"},
				},
			},
			want: "172.16.1.1, 192.168.1.100",
		},
		{
			name: "X-Forwarded-For and single forward address which is same as remote",
			req: &http.Request{
				RemoteAddr: "192.168.1.100:3454",
				Header: map[string][]string{
					http.CanonicalHeaderKey(xForwardedFor): {"192.168.1.100"},
				},
			},
			want: "192.168.1.100",
		},
		{
			name: "single IPv6 X-Forwarded-For address",
			req: &http.Request{
				RemoteAddr: "[2001:db9::1]:3454",
				Header: map[string][]string{
					http.CanonicalHeaderKey(xForwardedFor): {"2001:db8::1"},
				},
			},
			want: "2001:db8::1, 2001:db9::1",
		},
		{
			name: "single X-Forwarded-For address no RemoteAddr",
			req: &http.Request{
				Header: map[string][]string{
					http.CanonicalHeaderKey(xForwardedFor): {"172.16.1.1"},
				},
			},
			want: "172.16.1.1",
		},
		{
			name: "multiple X-Forwarded-For with remote",
			req: &http.Request{
				RemoteAddr: "192.168.1.100:3454",
				Header: map[string][]string{
					http.CanonicalHeaderKey(xForwardedFor): {"172.16.1.1, 10.10.13.20"},
				},
			},
			want: "172.16.1.1, 192.168.1.100",
		},
		{
			name: "multiple X-Forwarded-For with remote and no spaces",
			req: &http.Request{
				RemoteAddr: "192.168.1.100:3454",
				Header: map[string][]string{
					http.CanonicalHeaderKey(xForwardedFor): {"172.16.1.1,10.10.13.20,10.11.16.46"},
				},
			},
			want: "172.16.1.1, 192.168.1.100",
		},
		{
			name: "multiple X-Forwarded-For with IPv6 remote",
			req: &http.Request{
				RemoteAddr: "192.168.1.100:3454",
				Header: map[string][]string{
					http.CanonicalHeaderKey(xForwardedFor): {"[2001:db8:cafe::17]:4711, 10.10.13.20"},
				},
			},
			want: "2001:db8:cafe::17, 192.168.1.100",
		},
		{
			name: "no header, no remote",
			req:  &http.Request{},
			want: "",
		},
		{
			name: "X-Real-IP with IPv6 remote with port",
			req: &http.Request{
				RemoteAddr: "192.168.1.100:3454",
				Header: map[string][]string{
					http.CanonicalHeaderKey(xRealIP): {"[2001:db8:cafe::17]:4711"},
				},
			},
			want: "2001:db8:cafe::17, 192.168.1.100",
		},
		{
			name: "X-Real-IP with IPv4 remote",
			req: &http.Request{
				RemoteAddr: "192.168.1.100:3454",
				Header: map[string][]string{
					http.CanonicalHeaderKey(xRealIP): {"192.169.1.200"},
				},
			},
			want: "192.169.1.200, 192.168.1.100",
		},
		{
			name: "X-Real-IP with IPv4 remote and X-Forwarded-For",
			req: &http.Request{
				RemoteAddr: "192.168.1.100:3454",
				Header: map[string][]string{
					http.CanonicalHeaderKey(xForwardedFor): {"[2001:db8:cafe::17]:4711, 10.10.13.20"},
					http.CanonicalHeaderKey(xRealIP):       {"192.169.1.200"},
				},
			},
			want: "192.169.1.200, 192.168.1.100",
		},
		{
			name: "Forwarded with IPv4 remote",
			req: &http.Request{
				RemoteAddr: "192.168.1.100:3454",
				Header: map[string][]string{
					http.CanonicalHeaderKey(forwarded): {"for=192.169.1.200"},
				},
			},
			want: "192.169.1.200, 192.168.1.100",
		},
		{
			name: "Forwarded with IPv4 and proto and by fields",
			req: &http.Request{
				RemoteAddr: "192.168.1.100:3454",
				Header: map[string][]string{
					http.CanonicalHeaderKey(forwarded): {"for=192.0.2.60;proto=http;by=203.0.113.43"},
				},
			},
			want: "192.0.2.60, 192.168.1.100",
		},
		{
			name: "Forwarded with IPv6 and IPv4 remote",
			req: &http.Request{
				RemoteAddr: "192.168.1.100:3454",
				Header: map[string][]string{
					http.CanonicalHeaderKey(forwarded): {"for=[2001:db8:cafe::17]:4711,for=192.169.1.200"},
				},
			},
			want: "2001:db8:cafe::17, 192.168.1.100",
		},
		{
			name: "Forwarded with X-Real-IP and X-Forwarded-For",
			req: &http.Request{
				RemoteAddr: "192.168.1.100:3454",
				Header: map[string][]string{
					http.CanonicalHeaderKey(xForwardedFor): {"[2001:db8:cafe::17]:4711, 10.10.13.20"},
					http.CanonicalHeaderKey(xRealIP):       {"192.169.1.200"},
					http.CanonicalHeaderKey(forwarded):     {"for=[2001:db8:cafe::17]:4711,for=192.169.1.200"},
				},
			},
			want: "2001:db8:cafe::17, 192.168.1.100",
		},
		{
			name: "Forwarded returns hostname",
			req: &http.Request{
				RemoteAddr: "192.168.1.100:3454",
				Header: map[string][]string{
					http.CanonicalHeaderKey(forwarded): {"for=workstation.local"},
				},
			},
			want: "workstation.local, 192.168.1.100",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sourceIPs, err := NewSourceIPs("", "")
			require.NoError(t, err)

			if got := sourceIPs.Get(tt.req); got != tt.want {
				t.Errorf("GetSource() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetSourceIPsWithCustomRegex(t *testing.T) {
	tests := []struct {
		name string
		req  *http.Request
		want string
	}{
		{
			name: "no header",
			req:  &http.Request{RemoteAddr: "192.168.1.100:3454"},
			want: "192.168.1.100",
		},
		{
			name: "No matching entry in the header",
			req: &http.Request{
				RemoteAddr: "192.168.1.100:3454",
				Header: map[string][]string{
					http.CanonicalHeaderKey("SomeHeader"): {"not matching"},
				},
			},
			want: "192.168.1.100",
		},
		{
			name: "one matching entry in the header",
			req: &http.Request{
				RemoteAddr: "192.168.1.100:3454",
				Header: map[string][]string{
					http.CanonicalHeaderKey("SomeHeader"): {"172.16.1.1"},
				},
			},
			want: "172.16.1.1, 192.168.1.100",
		},
		{
			name: "multiple matching entries in the header, only first used",
			req: &http.Request{
				RemoteAddr: "192.168.1.100:3454",
				Header: map[string][]string{
					http.CanonicalHeaderKey("SomeHeader"): {"172.16.1.1", "172.16.2.1"},
				},
			},
			want: "172.16.1.1, 192.168.1.100",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sourceIPs, err := NewSourceIPs("SomeHeader", "((?:[0-9]{1,3}\\.){3}[0-9]{1,3})")
			require.NoError(t, err)

			if got := sourceIPs.Get(tt.req); got != tt.want {
				t.Errorf("GetSource() = %v, want %v", got, tt.want)
			}
		})
	}
}
func TestInvalid(t *testing.T) {
	sourceIPs, err := NewSourceIPs("Header", "")
	require.Empty(t, sourceIPs)
	require.Error(t, err)

	sourceIPs, err = NewSourceIPs("", "a(.*)b")
	require.Empty(t, sourceIPs)
	require.Error(t, err)

	sourceIPs, err = NewSourceIPs("Header", "[*")
	require.Empty(t, sourceIPs)
	require.Error(t, err)
}
