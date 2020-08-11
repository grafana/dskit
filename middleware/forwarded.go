package middleware

import (
	"fmt"
	"net"
	"net/http"
)

// extractHost returns the Host IP address without any port information
func extractHost(address string) string {
	hostIP := net.ParseIP(address)
	if hostIP != nil {
		return hostIP.String()
	}
	var err error
	hostStr, _, err := net.SplitHostPort(address)
	if err != nil {
		// Invalid IP address, just return it so it shows up in the logs
		return address
	}
	return hostStr
}

// GetSource extracts the X-FORWARDED-FOR header from the given HTTP request
// and returns a string with it and the remote address
func GetSource(req *http.Request) string {
	fwd := req.Header.Get("X-FORWARDED-FOR")
	if fwd == "" {
		if req.RemoteAddr == "" {
			// No X-FORWARDED-FOR header and no RemoteAddr set so we want to return an empty string
			// Might as well just use the empty string set in req.RemoteAddr
			return req.RemoteAddr
		}
		return extractHost(req.RemoteAddr)
	}
	// If RemoteAddr is empty just return the header
	if req.RemoteAddr == "" {
		return fwd
	}
	// If both a header and RemoteAddr are present return them both, stripping off any port info from the RemoteAddr
	return fmt.Sprintf("%v, %v", fwd, extractHost(req.RemoteAddr))
}
