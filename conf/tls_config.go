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

package conf

import (
	"crypto/tls"
	"crypto/x509"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/errors"
	"os"
)

func CreateServerTLSConfig(config TLSConfig) (*tls.Config, error) {
	if !config.Enabled {
		return nil, nil
	}
	tlsConfig := &tls.Config{ // nolint: gosec
		MinVersion: tls.VersionTLS12,
	}
	keyPair, err := common.CreateKeyPair(config.CertPath, config.KeyPath)
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
			return nil, errors.Errorf("failed to append trusted certs PEM (invalid PEM block?)")
		}
		tlsConfig.ClientCAs = trustedCertPool
	}
	clientAuth, ok := clientAuthTypeMap[config.ClientAuth]
	if !ok {
		return nil, errors.Errorf("invalid tls client auth setting '%s'", config.ClientAuth)
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
