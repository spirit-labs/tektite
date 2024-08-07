package certutil

import (
	"crypto/tls"
	"os"
)

func CreateKeyPair(certPath string, keyPath string) (tls.Certificate, error) {
	clientCert, err := os.ReadFile(certPath)
	if err != nil {
		return tls.Certificate{}, err
	}
	clientKey, err := os.ReadFile(keyPath)
	if err != nil {
		return tls.Certificate{}, err
	}
	keyPair, err := tls.X509KeyPair(clientCert, clientKey)
	if err != nil {
		return tls.Certificate{}, err
	}
	return keyPair, nil
}
