package middleware

import (
	"net/http"
	"testing"
)

func TestGetSource(t *testing.T) {
	type args struct {
		req *http.Request
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "no X-FORWARDED_FOR header",
			args: args{
				req: &http.Request{RemoteAddr: "192.168.1.100:3454"},
			},
			want: "192.168.1.100",
		},
		{
			name: "no X-FORWARDED-FOR header, remote has no port",
			args: args{
				req: &http.Request{RemoteAddr: "192.168.1.100"},
			},
			want: "192.168.1.100",
		},
		{
			name: "no X-FORWARDED-FOR header, remote address is invalid",
			args: args{
				req: &http.Request{RemoteAddr: "192.168.100"},
			},
			want: "192.168.100",
		},
		{
			name: "single forward address",
			args: args{
				req: &http.Request{
					RemoteAddr: "192.168.1.100:3454",
					Header: map[string][]string{
						http.CanonicalHeaderKey("X-FORWARDED-FOR"): {"172.16.1.1"},
					},
				},
			},
			want: "172.16.1.1, 192.168.1.100",
		},
		{
			name: "single IPv6 forward address",
			args: args{
				req: &http.Request{
					RemoteAddr: "[2001:db9::1]:3454",
					Header: map[string][]string{
						http.CanonicalHeaderKey("X-FORWARDED-FOR"): {"2001:db8::1"},
					},
				},
			},
			want: "2001:db8::1, 2001:db9::1",
		},
		{
			name: "single forward address no RemoteAddr",
			args: args{
				req: &http.Request{
					Header: map[string][]string{
						http.CanonicalHeaderKey("X-FORWARDED-FOR"): {"172.16.1.1"},
					},
				},
			},
			want: "172.16.1.1",
		},
		{
			name: "multiple forward with remote",
			args: args{
				req: &http.Request{
					RemoteAddr: "192.168.1.100:3454",
					Header: map[string][]string{
						http.CanonicalHeaderKey("X-FORWARDED-FOR"): {"172.16.1.1, 10.10.13.20"},
					},
				},
			},
			want: "172.16.1.1, 10.10.13.20, 192.168.1.100",
		},
		{
			name: "no forward header, no remote",
			args: args{
				req: &http.Request{},
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetSource(tt.args.req); got != tt.want {
				t.Errorf("GetSource() = %v, want %v", got, tt.want)
			}
		})
	}
}
