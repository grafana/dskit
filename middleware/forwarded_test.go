package middleware

import (
	"net/http"
	"testing"
)

func TestGetSource(t *testing.T) {
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
			if got := GetSource(tt.req); got != tt.want {
				t.Errorf("GetSource() = %v, want %v", got, tt.want)
			}
		})
	}
}
