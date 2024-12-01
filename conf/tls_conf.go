package conf

import (
	"crypto/tls"
	"crypto/x509"
	"github.com/spirit-labs/tektite/asl/certutil"
	"github.com/spirit-labs/tektite/asl/errwrap"
	"os"
)

type TlsConf struct {
	Enabled              bool   `help:"is TLS enabled?" default:"false"`
	ServerCertFile       string `help:"path to tls server certificate file in pem format"`
	ServerPrivateKeyFile string `help:"path to tls server private key file in pem format"`
	ClientCertFile       string `help:"path to tls client certificate file in pem format"`
	ClientAuthType       string `help:"client certificate authentication mode. one of: no-client-cert, request-client-cert, require-any-client-cert, verify-client-cert-if-given, require-and-verify-client-cert"`
}

func (t *TlsConf) ToGoTlsConf() (*tls.Config, error) {
	if !t.Enabled {
		return nil, nil
	}
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}
	kp, err := certutil.CreateKeyPair(t.ServerCertFile, t.ServerPrivateKeyFile)
	if err != nil {
		return nil, err
	}
	tlsConfig.Certificates = []tls.Certificate{kp}
	if t.ClientCertFile != "" {
		clCerts, err := os.ReadFile(t.ClientCertFile)
		if err != nil {
			return nil, err
		}
		certPool := x509.NewCertPool()
		if ok := certPool.AppendCertsFromPEM(clCerts); !ok {
			return nil, errwrap.Errorf("failed to append client certs - is pem file invalid?")
		}
		tlsConfig.ClientCAs = certPool
		var clAuthType tls.ClientAuthType
		if t.ClientAuthType == "" {
			clAuthType = tls.RequireAndVerifyClientCert
		} else {
			var ok bool
			clAuthType, ok = clientAuthMapping[t.ClientAuthType]
			if !ok {
				return nil, errwrap.Errorf("tls client auth type configuration is invalid: '%s'", t.ClientAuthType)
			}
		}
		tlsConfig.ClientAuth = clAuthType
	} else {
		tlsConfig.ClientAuth = tls.NoClientCert
	}
	return tlsConfig, nil
}

var clientAuthMapping = map[string]tls.ClientAuthType{
	"no-client-cert":                 tls.NoClientCert,
	"request-client-cert":            tls.RequestClientCert,
	"require-any-client-cert":        tls.RequireAnyClientCert,
	"verify-client-cert-if-given":    tls.VerifyClientCertIfGiven,
	"require-and-verify-client-cert": tls.RequireAndVerifyClientCert,
}

type ClientTlsConf struct {
	Enabled              bool   `help:"is client TLS enabled?" default:"false"`
	ServerCertFile       string `help:"path to tls server certificate file in pem format"`
	ClientPrivateKeyFile string `help:"path to tls client private key file in pem format"`
	ClientCertFile       string `help:"path to tls client certificate file in pem format"`
}

func (c *ClientTlsConf) ToGoTlsConf() (*tls.Config, error) {
	if !c.Enabled {
		return nil, nil
	}
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}
	if c.ServerCertFile != "" {
		serverCerts, err := os.ReadFile(c.ServerCertFile)
		if err != nil {
			return nil, err
		}
		certPool := x509.NewCertPool()
		if ok := certPool.AppendCertsFromPEM(serverCerts); !ok {
			return nil, errwrap.Errorf("failed to append client certs - is pem file invalid?")
		}
		tlsConfig.RootCAs = certPool
	}
	if c.ClientCertFile != "" {
		kp, err := certutil.CreateKeyPair(c.ClientCertFile, c.ClientPrivateKeyFile)
		if err != nil {
			return nil, err
		}
		tlsConfig.Certificates = []tls.Certificate{kp}
	}
	return tlsConfig, nil
}
