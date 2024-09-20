package transport

import (
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/client"
	"github.com/spirit-labs/tektite/common"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func init() {
	common.EnableTestPorts()
}

const (
	serverKeyPath   = "testdata/serverkey.pem"
	serverCertPath  = "testdata/servercert.pem"
	clientCertPath1 = "testdata/selfsignedclientcert.pem"
	clientKeyPath1  = "testdata/selfsignedclientkey.pem"
	clientCertPath2 = "testdata/selfsignedclientcert2.pem"
	clientKeyPath2  = "testdata/selfsignedclientkey2.pem"
)

func TestSocketTransportNoTls(t *testing.T) {
	serverFactor, connFactory := setup(t, conf.TLSConfig{}, nil)
	runTestCases(t, serverFactor, connFactory)
}

func TestSocketTransportServerTls(t *testing.T) {
	serverFactory, connFactory := setup(t, conf.TLSConfig{
		Enabled:  true,
		KeyPath:  serverKeyPath,
		CertPath: serverCertPath,
	}, &client.TLSConfig{
		TrustedCertsPath: serverCertPath,
	})
	runTestCases(t, serverFactory, connFactory)
}

func TestSocketTransportMutualTls(t *testing.T) {
	serverFactory, connFactory := setup(t, conf.TLSConfig{
		Enabled:         true,
		KeyPath:         serverKeyPath,
		CertPath:        serverCertPath,
		ClientCertsPath: clientCertPath1,
		ClientAuth:      conf.ClientAuthModeRequireAndVerifyClientCert,
	}, &client.TLSConfig{
		TrustedCertsPath: serverCertPath,
		KeyPath:          clientKeyPath1,
		CertPath:         clientCertPath1,
		NoVerify:         false,
	})
	runTestCases(t, serverFactory, connFactory)
}

func TestSocketTransportServerTlsUntrustedServer(t *testing.T) {
	serverTlsConf := conf.TLSConfig{
		Enabled:  true,
		KeyPath:  serverKeyPath,
		CertPath: serverCertPath,
	}
	address, err := common.AddressWithPort("localhost")
	require.NoError(t, err)
	server := NewSocketServer(address, serverTlsConf)
	err = server.Start()
	require.NoError(t, err)
	cl, err := NewSocketClient(&client.TLSConfig{})
	require.NoError(t, err)
	_, err = cl.CreateConnection(address)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "tls: failed to verify certificate: x509: certificate signed by unknown authority"))
}

func TestSocketTransportMutualTlsUntrustedClient(t *testing.T) {
	serverTlsConf := conf.TLSConfig{
		Enabled:         true,
		KeyPath:         serverKeyPath,
		CertPath:        serverCertPath,
		ClientCertsPath: clientCertPath1,
		ClientAuth:      conf.ClientAuthModeRequireAndVerifyClientCert,
	}
	clientTlsConf := &client.TLSConfig{
		TrustedCertsPath: serverCertPath,
		KeyPath:          clientKeyPath2,
		CertPath:         clientCertPath2,
		NoVerify:         false,
	}
	address, err := common.AddressWithPort("localhost")
	require.NoError(t, err)
	server := NewSocketServer(address, serverTlsConf)
	err = server.Start()
	require.NoError(t, err)
	cl, err := NewSocketClient(clientTlsConf)
	require.NoError(t, err)
	conn, err := cl.CreateConnection(address)
	require.NoError(t, err)
	_, err = conn.SendRPC(0, []byte("foo"))
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "tls: certificate required"))
}

func TestWriteErrorServerNotAvailable(t *testing.T) {
	cl, err := NewSocketClient(nil)
	require.NoError(t, err)
	_, err = cl.CreateConnection("localhost:365454")
	require.True(t, common.IsTektiteErrorWithCode(err, common.Unavailable))
}

func setup(t *testing.T, serverTlsConf conf.TLSConfig, clientTlsConf *client.TLSConfig) (ServerFactory, ConnectionFactory) {
	serverFactory := func(t *testing.T) Server {
		address, err := common.AddressWithPort("localhost")
		require.NoError(t, err)
		server := NewSocketServer(address, serverTlsConf)
		err = server.Start()
		require.NoError(t, err)
		return server
	}
	cl, err := NewSocketClient(clientTlsConf)
	require.NoError(t, err)
	connFactory := cl.CreateConnection
	return serverFactory, connFactory
}
