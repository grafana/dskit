package tls

import (
	"crypto/tls"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// certPEM and keyPEM are copied from the golang crypto/tls library
// https://github.com/golang/go/blob/7eb5941b95a588a23f18fa4c22fe42ff0119c311/src/crypto/tls/example_test.go#L127
const certPEM = `-----BEGIN CERTIFICATE-----
MIIBhTCCASugAwIBAgIQIRi6zePL6mKjOipn+dNuaTAKBggqhkjOPQQDAjASMRAw
DgYDVQQKEwdBY21lIENvMB4XDTE3MTAyMDE5NDMwNloXDTE4MTAyMDE5NDMwNlow
EjEQMA4GA1UEChMHQWNtZSBDbzBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABD0d
7VNhbWvZLWPuj/RtHFjvtJBEwOkhbN/BnnE8rnZR8+sbwnc/KhCk3FhnpHZnQz7B
5aETbbIgmuvewdjvSBSjYzBhMA4GA1UdDwEB/wQEAwICpDATBgNVHSUEDDAKBggr
BgEFBQcDATAPBgNVHRMBAf8EBTADAQH/MCkGA1UdEQQiMCCCDmxvY2FsaG9zdDo1
NDUzgg4xMjcuMC4wLjE6NTQ1MzAKBggqhkjOPQQDAgNIADBFAiEA2zpJEPQyz6/l
Wf86aX6PepsntZv2GYlA5UpabfT2EZICICpJ5h/iI+i341gBmLiAFQOyTDT+/wQc
6MF9+Yw1Yy0t
-----END CERTIFICATE-----`
const keyPEM = `-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIIrYSSNQFaA2Hwf1duRSxKtLYX5CB04fSeQ6tF1aY/PuoAoGCCqGSM49
AwEHoUQDQgAEPR3tU2Fta9ktY+6P9G0cWO+0kETA6SFs38GecTyudlHz6xvCdz8q
EKTcWGekdmdDPsHloRNtsiCa697B2O9IFA==
-----END EC PRIVATE KEY-----`

// caPEM is CA certificate of Let's Encrypt
// https://letsencrypt.org/certs/isrgrootx1.pem.txt
const caPEM = `-----BEGIN CERTIFICATE-----
MIIFazCCA1OgAwIBAgIRAIIQz7DSQONZRGPgu2OCiwAwDQYJKoZIhvcNAQELBQAw
TzELMAkGA1UEBhMCVVMxKTAnBgNVBAoTIEludGVybmV0IFNlY3VyaXR5IFJlc2Vh
cmNoIEdyb3VwMRUwEwYDVQQDEwxJU1JHIFJvb3QgWDEwHhcNMTUwNjA0MTEwNDM4
WhcNMzUwNjA0MTEwNDM4WjBPMQswCQYDVQQGEwJVUzEpMCcGA1UEChMgSW50ZXJu
ZXQgU2VjdXJpdHkgUmVzZWFyY2ggR3JvdXAxFTATBgNVBAMTDElTUkcgUm9vdCBY
MTCCAiIwDQYJKoZIhvcNAQEBBQADggIPADCCAgoCggIBAK3oJHP0FDfzm54rVygc
h77ct984kIxuPOZXoHj3dcKi/vVqbvYATyjb3miGbESTtrFj/RQSa78f0uoxmyF+
0TM8ukj13Xnfs7j/EvEhmkvBioZxaUpmZmyPfjxwv60pIgbz5MDmgK7iS4+3mX6U
A5/TR5d8mUgjU+g4rk8Kb4Mu0UlXjIB0ttov0DiNewNwIRt18jA8+o+u3dpjq+sW
T8KOEUt+zwvo/7V3LvSye0rgTBIlDHCNAymg4VMk7BPZ7hm/ELNKjD+Jo2FR3qyH
B5T0Y3HsLuJvW5iB4YlcNHlsdu87kGJ55tukmi8mxdAQ4Q7e2RCOFvu396j3x+UC
B5iPNgiV5+I3lg02dZ77DnKxHZu8A/lJBdiB3QW0KtZB6awBdpUKD9jf1b0SHzUv
KBds0pjBqAlkd25HN7rOrFleaJ1/ctaJxQZBKT5ZPt0m9STJEadao0xAH0ahmbWn
OlFuhjuefXKnEgV4We0+UXgVCwOPjdAvBbI+e0ocS3MFEvzG6uBQE3xDk3SzynTn
jh8BCNAw1FtxNrQHusEwMFxIt4I7mKZ9YIqioymCzLq9gwQbooMDQaHWBfEbwrbw
qHyGO0aoSCqI3Haadr8faqU9GY/rOPNk3sgrDQoo//fb4hVC1CLQJ13hef4Y53CI
rU7m2Ys6xt0nUW7/vGT1M0NPAgMBAAGjQjBAMA4GA1UdDwEB/wQEAwIBBjAPBgNV
HRMBAf8EBTADAQH/MB0GA1UdDgQWBBR5tFnme7bl5AFzgAiIyBpY9umbbjANBgkq
hkiG9w0BAQsFAAOCAgEAVR9YqbyyqFDQDLHYGmkgJykIrGF1XIpu+ILlaS/V9lZL
ubhzEFnTIZd+50xx+7LSYK05qAvqFyFWhfFQDlnrzuBZ6brJFe+GnY+EgPbk6ZGQ
3BebYhtF8GaV0nxvwuo77x/Py9auJ/GpsMiu/X1+mvoiBOv/2X/qkSsisRcOj/KK
NFtY2PwByVS5uCbMiogziUwthDyC3+6WVwW6LLv3xLfHTjuCvjHIInNzktHCgKQ5
ORAzI4JMPJ+GslWYHb4phowim57iaztXOoJwTdwJx4nLCgdNbOhdjsnvzqvHu7Ur
TkXWStAmzOVyyghqpZXjFaH3pO3JLF+l+/+sKAIuvtd7u+Nxe5AW0wdeRlN8NwdC
jNPElpzVmbUq4JUagEiuTDkHzsxHpFKVK7q4+63SM1N95R1NbdWhscdCb+ZAJzVc
oyi3B43njTOQ5yOf+1CceWxG1bQVs5ZufpsMljq4Ui0/1lvh+wjChP4kqKOJ2qxq
4RgqsahDYVvTH9w7jXbyLeiNdd8XM2w9U/t7y0Ff/9yi0GE44Za4rF2LN9d11TPA
mRGunUHBcnWEvgJBQl9nJEiU0Zsnvgc/ubhPgXRR4Xq37Z0j4r7g1SgEEzwxA57d
emyPxgcYxn/eR44/KJ4EBs+lVDR3veyJm+kXQ99b21/+jh5Xos1AnX5iItreGCc=
-----END CERTIFICATE-----`

type x509Paths struct {
	cert string
	key  string
	ca   string
}

func newTestX509Files(t *testing.T, cert, key, ca []byte) x509Paths {
	t.Helper()

	certsPath := t.TempDir()

	paths := x509Paths{
		cert: filepath.Join(certsPath, "cert.pem"),
		key:  filepath.Join(certsPath, "key.pem"),
		ca:   filepath.Join(certsPath, "ca.pem"),
	}

	if cert != nil {
		err := os.WriteFile(paths.cert, cert, 0600)
		require.NoError(t, err)
	}

	if key != nil {
		err := os.WriteFile(paths.key, key, 0600)
		require.NoError(t, err)
	}

	if ca != nil {
		err := os.WriteFile(paths.ca, ca, 0600)
		require.NoError(t, err)
	}

	return paths
}

func TestGetTLSConfig_ClientCerts(t *testing.T) {
	paths := newTestX509Files(t, []byte(certPEM), []byte(keyPEM), nil)

	// test working certificate passed
	c := &ClientConfig{
		CertPath: paths.cert,
		KeyPath:  paths.key,
	}
	tlsConfig, err := c.GetTLSConfig()
	assert.NoError(t, err)
	assert.Equal(t, false, tlsConfig.InsecureSkipVerify, "make sure we default to not skip verification")
	assert.Equal(t, 1, len(tlsConfig.Certificates), "ensure a certificate is returned")

	// expect error with key and cert swapped passed along
	c = &ClientConfig{
		CertPath: paths.key,
		KeyPath:  paths.cert,
	}
	_, err = c.GetTLSConfig()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to find certificate PEM data in certificate input, but did find a private key")

	// expect error with only key passed along
	c = &ClientConfig{
		KeyPath: paths.key,
	}
	_, err = c.GetTLSConfig()
	assert.EqualError(t, err, errCertMissing.Error())

	// expect error with only cert passed along
	c = &ClientConfig{
		CertPath: paths.cert,
	}
	_, err = c.GetTLSConfig()
	assert.EqualError(t, err, errKeyMissing.Error())
}

func TestGetTLSConfig_CA(t *testing.T) {
	paths := newTestX509Files(t, nil, nil, []byte(certPEM))

	// test single ca passed
	c := &ClientConfig{
		CAPath: paths.ca,
	}
	tlsConfig, err := c.GetTLSConfig()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tlsConfig.RootCAs.Subjects()), "ensure one CA is returned") //nolint:staticcheck
	assert.Equal(t, false, tlsConfig.InsecureSkipVerify, "make sure we default to not skip verification")

	// test two cas passed
	paths = newTestX509Files(t, nil, nil, []byte(certPEM+"\n"+caPEM))
	c = &ClientConfig{
		CAPath: paths.ca,
	}
	tlsConfig, err = c.GetTLSConfig()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(tlsConfig.RootCAs.Subjects()), "ensure two CAs are returned") //nolint:staticcheck
	assert.False(t, tlsConfig.InsecureSkipVerify, "make sure we default to not skip verification")

	// expect errors to be passed
	c = &ClientConfig{
		CAPath: paths.ca + "not-existing",
	}
	_, err = c.GetTLSConfig()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "error loading ca cert")
}

func TestGetTLSConfig_InsecureSkipVerify(t *testing.T) {
	c := &ClientConfig{
		InsecureSkipVerify: true,
	}
	tlsConfig, err := c.GetTLSConfig()
	assert.NoError(t, err)
	assert.True(t, tlsConfig.InsecureSkipVerify)
}

func TestGetTLSConfig_ServerName(t *testing.T) {
	c := &ClientConfig{
		ServerName: "myserver.com",
	}
	tlsConfig, err := c.GetTLSConfig()
	assert.NoError(t, err)
	assert.Equal(t, "myserver.com", tlsConfig.ServerName)
}

func TestGetTLSConfig_MinVersion(t *testing.T) {
	type test struct {
		desc            string
		MinVersion      string
		ExpectedVersion uint16
		RequireError    bool
	}

	table := []test{
		{
			desc:            "no version set",
			MinVersion:      "",
			ExpectedVersion: 0,
			RequireError:    false,
		},
		{
			desc:            "TLS v1.0 set",
			MinVersion:      "VersionTLS10",
			ExpectedVersion: tls.VersionTLS10,
			RequireError:    false,
		},
		{
			desc:            "TLS v1.1 set",
			MinVersion:      "VersionTLS11",
			ExpectedVersion: tls.VersionTLS11,
			RequireError:    false,
		},
		{
			desc:            "TLS v1.2 set",
			MinVersion:      "VersionTLS12",
			ExpectedVersion: tls.VersionTLS12,
			RequireError:    false,
		},
		{
			desc:            "TLS v1.3 set",
			MinVersion:      "VersionTLS13",
			ExpectedVersion: tls.VersionTLS13,
			RequireError:    false,
		},
		{
			desc:            "bad TLS version set",
			MinVersion:      "VersionTLS14",
			ExpectedVersion: 0,
			RequireError:    true,
		},
	}

	for _, tst := range table {
		tst := tst
		t.Run(tst.desc, func(t *testing.T) {
			t.Parallel()

			c := &ClientConfig{
				MinVersion: tst.MinVersion,
			}

			tlsConfig, err := c.GetTLSConfig()

			if tst.RequireError {
				assert.Error(t, err)
				assert.Nil(t, tlsConfig)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tst.ExpectedVersion, tlsConfig.MinVersion)
			}
		})
	}
}

func TestGetTLSConfig_CipherSuites(t *testing.T) {
	type test struct {
		desc                 string
		CipherSuites         string
		ExpectedCipherSuites []uint16
		RequireError         bool
	}

	cipherSuiteNames := "TLS_RSA_WITH_RC4_128_SHA," +
		"TLS_RSA_WITH_3DES_EDE_CBC_SHA," +
		"TLS_RSA_WITH_AES_128_CBC_SHA," +
		"TLS_RSA_WITH_AES_256_CBC_SHA," +
		"TLS_RSA_WITH_AES_128_CBC_SHA256," +
		"TLS_RSA_WITH_AES_128_GCM_SHA256," +
		"TLS_RSA_WITH_AES_256_GCM_SHA384," +
		"TLS_ECDHE_ECDSA_WITH_RC4_128_SHA," +
		"TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA," +
		"TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA," +
		"TLS_ECDHE_RSA_WITH_RC4_128_SHA," +
		"TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA," +
		"TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA," +
		"TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA," +
		"TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256," +
		"TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256," +
		"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256," +
		"TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256," +
		"TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384," +
		"TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384," +
		"TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256," +
		"TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256," +
		"TLS_AES_128_GCM_SHA256," +
		"TLS_AES_256_GCM_SHA384," +
		"TLS_CHACHA20_POLY1305_SHA256"

	table := []test{
		{
			desc:                 "no cipher suites set",
			CipherSuites:         "",
			ExpectedCipherSuites: nil,
			RequireError:         false,
		},
		{
			desc:         "all cipher suites set",
			CipherSuites: cipherSuiteNames,
			ExpectedCipherSuites: []uint16{
				tls.TLS_RSA_WITH_RC4_128_SHA,
				tls.TLS_RSA_WITH_3DES_EDE_CBC_SHA,
				tls.TLS_RSA_WITH_AES_128_CBC_SHA,
				tls.TLS_RSA_WITH_AES_256_CBC_SHA,
				tls.TLS_RSA_WITH_AES_128_CBC_SHA256,
				tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_ECDSA_WITH_RC4_128_SHA,
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
				tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
				tls.TLS_ECDHE_RSA_WITH_RC4_128_SHA,
				tls.TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA,
				tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
				tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256,
				tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,
				tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
				tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
				tls.TLS_AES_128_GCM_SHA256,
				tls.TLS_AES_256_GCM_SHA384,
				tls.TLS_CHACHA20_POLY1305_SHA256,
			},
			RequireError: false,
		},
		{
			desc:                 "bad cipher suites set",
			CipherSuites:         "TLS_NO_SUITE",
			ExpectedCipherSuites: nil,
			RequireError:         true,
		},
	}

	for _, tst := range table {
		tst := tst
		t.Run(tst.desc, func(t *testing.T) {
			t.Parallel()

			c := &ClientConfig{
				CipherSuites: tst.CipherSuites,
			}

			tlsConfig, err := c.GetTLSConfig()

			if tst.RequireError {
				assert.Error(t, err)
				assert.Nil(t, tlsConfig)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tst.ExpectedCipherSuites, tlsConfig.CipherSuites)
			}
		})
	}
}
