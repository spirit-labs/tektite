// Copyright 2024 The Tektite Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tekclient

import (
	"crypto/tls"
	"crypto/x509"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/errors"
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
			return nil, errors.Errorf("failed to append root certs PEM (invalid PEM block?)")
		}
		tlsConfig.RootCAs = rootCertPool
	}
	if t.CertPath != "" {
		keyPair, err := common.CreateKeyPair(t.CertPath, t.KeyPath)
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
