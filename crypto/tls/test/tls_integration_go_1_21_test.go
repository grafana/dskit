//go:build go1.21

package test

// The error message changed in Go 1.21.
const badCertificateErrorMessage = "remote error: tls: certificate required"
const mismatchCAAndCerts = "remote error: tls: unknown certificate authority"
