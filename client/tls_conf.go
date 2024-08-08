package client

import (
	"crypto/tls"
	"crypto/x509"
	"github.com/spirit-labs/tektite/asl/certutil"
	"github.com/spirit-labs/tektite/asl/errwrap"
	"os"
)

type TLSConfig struct {
	TrustedCertsPath string `help:"Path to a PEM encoded file containing certificate(s) of trusted servers and/or certificate authorities"`
	KeyPath          string `help:"Path to a PEM encoded file containing the client private key. Required with TLS client authentication"`
	CertPath         string `help:"Path to a PEM encoded file containing the client certificate. Required with TLS client authentication"`
	NoVerify         bool   `help:"Set to true to disable server certificate verification. WARNING use only for testing, setting this can expose you to man-in-the-middle attacks"`
}

func (t *TLSConfig) ToGoTlsConfig() (*tls.Config, error) {
	// Note, the Tektite HTTP2 HTTP API is HTTP2 only and requires TLS
	tlsConfig := &tls.Config{ // nolint: gosec
		MinVersion: tls.VersionTLS12,
	}
	if t.TrustedCertsPath != "" {
		rootCerts, err := os.ReadFile(t.TrustedCertsPath)
		if err != nil {
			return nil, err
		}
		rootCertPool := x509.NewCertPool()
		if ok := rootCertPool.AppendCertsFromPEM(rootCerts); !ok {
			return nil, errwrap.Errorf("failed to append root certs PEM (invalid PEM block?)")
		}
		tlsConfig.RootCAs = rootCertPool
	}
	if t.CertPath != "" {
		keyPair, err := certutil.CreateKeyPair(t.CertPath, t.KeyPath)
		if err != nil {
			return nil, err
		}
		tlsConfig.Certificates = []tls.Certificate{keyPair}
	}
	if t.NoVerify {
		tlsConfig.InsecureSkipVerify = true
	}
	return tlsConfig, nil
}
