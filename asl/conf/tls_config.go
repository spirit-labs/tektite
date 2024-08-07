package conf

import (
	"crypto/tls"
	"crypto/x509"
	"github.com/spirit-labs/tektite/asl/certutil"
	"github.com/spirit-labs/tektite/asl/errwrap"
	"os"
)

func CreateServerTLSConfig(config TLSConfig) (*tls.Config, error) {
	if !config.Enabled {
		return nil, nil
	}
	tlsConfig := &tls.Config{ // nolint: gosec
		MinVersion: tls.VersionTLS12,
	}
	keyPair, err := certutil.CreateKeyPair(config.CertPath, config.KeyPath)
	if err != nil {
		return nil, err
	}
	tlsConfig.Certificates = []tls.Certificate{keyPair}
	if config.ClientCertsPath != "" {
		clientCerts, err := os.ReadFile(config.ClientCertsPath)
		if err != nil {
			return nil, err
		}
		trustedCertPool := x509.NewCertPool()
		if ok := trustedCertPool.AppendCertsFromPEM(clientCerts); !ok {
			return nil, errwrap.Errorf("failed to append trusted certs PEM (invalid PEM block?)")
		}
		tlsConfig.ClientCAs = trustedCertPool
	}
	clientAuth, ok := clientAuthTypeMap[config.ClientAuth]
	if !ok {
		return nil, errwrap.Errorf("invalid tls client auth setting '%s'", config.ClientAuth)
	}
	if config.ClientCertsPath != "" && config.ClientAuth == "" {
		// If client certs provided then default to client auth required
		clientAuth = tls.RequireAndVerifyClientCert
	}
	tlsConfig.ClientAuth = clientAuth
	return tlsConfig, nil
}

var clientAuthTypeMap = map[string]tls.ClientAuthType{
	ClientAuthModeNoClientCert:               tls.NoClientCert,
	ClientAuthModeRequestClientCert:          tls.RequestClientCert,
	ClientAuthModeRequireAnyClientCert:       tls.RequireAnyClientCert,
	ClientAuthModeVerifyClientCertIfGiven:    tls.VerifyClientCertIfGiven,
	ClientAuthModeRequireAndVerifyClientCert: tls.RequireAndVerifyClientCert,
	ClientAuthModeUnspecified:                tls.NoClientCert,
}
