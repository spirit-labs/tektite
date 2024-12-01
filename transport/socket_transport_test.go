package transport

import (
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
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
	serverFactor, connFactory := setup(t, conf.TlsConf{}, nil)
	runTestCases(t, serverFactor, connFactory)
}

func TestSocketTransportServerTls(t *testing.T) {
	serverFactory, connFactory := setup(t, conf.TlsConf{
		Enabled:              true,
		ServerPrivateKeyFile: serverKeyPath,
		ServerCertFile:       serverCertPath,
	}, &conf.ClientTlsConf{
		Enabled:        true,
		ServerCertFile: serverCertPath,
	})
	runTestCases(t, serverFactory, connFactory)
}

func TestSocketTransportMutualTls(t *testing.T) {
	serverFactory, connFactory := setup(t, conf.TlsConf{
		Enabled:              true,
		ServerPrivateKeyFile: serverKeyPath,
		ServerCertFile:       serverCertPath,
		ClientCertFile:       clientCertPath1,
		ClientAuthType:       "require-and-verify-client-cert",
	}, &conf.ClientTlsConf{
		Enabled:              true,
		ServerCertFile:       serverCertPath,
		ClientPrivateKeyFile: clientKeyPath1,
		ClientCertFile:       clientCertPath1,
	})
	runTestCases(t, serverFactory, connFactory)
}

func TestSocketTransportServerTlsUntrustedServer(t *testing.T) {
	serverTlsConf := conf.TlsConf{
		Enabled:              true,
		ServerPrivateKeyFile: serverKeyPath,
		ServerCertFile:       serverCertPath,
	}
	address, err := common.AddressWithPort("localhost")
	require.NoError(t, err)
	server := NewSocketTransportServer(address, serverTlsConf)
	err = server.Start()
	require.NoError(t, err)
	cl, err := NewSocketClient(&conf.ClientTlsConf{
		Enabled: true,
	})
	require.NoError(t, err)
	_, err = cl.CreateConnection(address)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "tls: failed to verify certificate: x509: certificate signed by unknown authority"))
}

func TestSocketTransportMutualTlsUntrustedClient(t *testing.T) {
	serverTlsConf := conf.TlsConf{
		Enabled:              true,
		ServerPrivateKeyFile: serverKeyPath,
		ServerCertFile:       serverCertPath,
		ClientCertFile:       clientCertPath1,
		ClientAuthType:       "require-and-verify-client-cert",
	}
	clientTlsConf := &conf.ClientTlsConf{
		Enabled:              true,
		ServerCertFile:       serverCertPath,
		ClientPrivateKeyFile: clientKeyPath2,
		ClientCertFile:       clientCertPath2,
	}
	address, err := common.AddressWithPort("localhost")
	require.NoError(t, err)
	server := NewSocketTransportServer(address, serverTlsConf)
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

func setup(t *testing.T, serverTlsConf conf.TlsConf, clientTlsConf *conf.ClientTlsConf) (ServerFactory, ConnectionFactory) {
	serverFactory := func(t *testing.T) Server {
		address, err := common.AddressWithPort("localhost")
		require.NoError(t, err)
		server := NewSocketTransportServer(address, serverTlsConf)
		err = server.Start()
		require.NoError(t, err)
		return server
	}
	cl, err := NewSocketClient(clientTlsConf)
	require.NoError(t, err)
	connFactory := cl.CreateConnection
	return serverFactory, connFactory
}
