package kafkaserver2

import (
	"crypto/tls"
	"encoding/binary"
	"github.com/pkg/errors"
	auth "github.com/spirit-labs/tektite/auth2"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/sockserver"
	"net"
	"sync"
)

type KafkaServer struct {
	lock            sync.Mutex
	address         string
	tlsConf         conf.TlsConf
	socketServer    *sockserver.SocketServer
	saslAuthManager *auth.SaslAuthManager
	authType        AuthenticationType
	handlerFactory  HandlerFactory
	started         bool
}

type HandlerFactory func(ctx ConnectionContext) kafkaprotocol.RequestHandler

type ConnectionContext interface {
	AuthContext() *auth.Context
}

type AuthenticationType int

const (
	AuthenticationTypeNone         AuthenticationType = iota
	AuthenticationTypeSaslPlain    AuthenticationType = iota
	AuthenticationTypeSaslScram512 AuthenticationType = iota
	AuthenticationTypeMTls         AuthenticationType = iota
)

func NewKafkaServer(address string, tlsConf conf.TlsConf, authType AuthenticationType, handlerFactory HandlerFactory) *KafkaServer {
	return &KafkaServer{
		address:        address,
		tlsConf:        tlsConf,
		authType:       authType,
		handlerFactory: handlerFactory,
	}
}

func (k *KafkaServer) Start() error {
	k.lock.Lock()
	defer k.lock.Unlock()
	if k.started {
		return nil
	}
	socketServer := sockserver.NewSocketServer(k.address, k.tlsConf, k.createConnection)
	if err := socketServer.Start(); err != nil {
		return err
	}
	k.socketServer = socketServer
	k.started = true
	return nil
}

func (k *KafkaServer) Stop() error {
	k.lock.Lock()
	defer k.lock.Unlock()
	if !k.started {
		return nil
	}
	if err := k.socketServer.Stop(); err != nil {
		return err
	}
	k.started = false
	return nil
}

func (k *KafkaServer) ListenAddress() string {
	return k.socketServer.Address()
}

type ConnectionInfo struct {
	ClientAddress string
	Authenticated bool
	Principal string
}

func (k *KafkaServer) ConnectionInfos() []ConnectionInfo {
	k.lock.Lock()
	defer k.lock.Unlock()
	conns := k.socketServer.Connections()
	infos := make([]ConnectionInfo, 0, len(conns))
	for _, conn := range conns {
		kconn := conn.(*kafkaConnection)
		infos = append(infos, ConnectionInfo{
			ClientAddress: kconn.conn.RemoteAddr().String(),
			Authenticated: kconn.authContext.Authenticated,
			Principal: kconn.authContext.Principal,
		})
	}
	return infos
}

func (k *KafkaServer) createConnection(conn net.Conn) sockserver.ServerConnection {
	kc := &kafkaConnection{s: k, conn: conn}
	handler := k.handlerFactory(kc)
	kc.handler = handler
	return kc
}

type kafkaConnection struct {
	s           *KafkaServer
	lock        sync.Mutex
	conn        net.Conn
	authContext auth.Context
	handler     kafkaprotocol.RequestHandler
}

func (c *kafkaConnection) AuthContext() *auth.Context {
	return &c.authContext
}

func (c *kafkaConnection) HandleMessage(message []byte) error {
	if !c.authContext.Authenticated && c.s.authType == AuthenticationTypeMTls {
		if err := c.authoriseWithClientCert(); err != nil {
			return err
		}
	}
	apiKey := int16(binary.BigEndian.Uint16(message))
	authType := c.s.authType
	log.Debugf("handling api key: %d auth type is %d authenticated is %t", apiKey, authType, c.authContext.Authenticated)
	authenticated := authType == AuthenticationTypeNone || apiKey == kafkaprotocol.APIKeyAPIVersions ||
		apiKey == kafkaprotocol.APIKeySaslHandshake || apiKey == kafkaprotocol.APIKeySaslAuthenticate ||
		c.authContext.Authenticated
	if !authenticated {
		return errors.Errorf("cannot handle Kafka apiKey: %d as authentication type is %d but connection has not been authenticated", apiKey, authType)
	}
	return kafkaprotocol.HandleRequestBuffer(apiKey, message, c.handler, c.conn)
}

func (c *kafkaConnection) authoriseWithClientCert() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	tlsConn, ok := c.conn.(*tls.Conn)
	if !ok {
		return errors.New("cannot use TLS authentication on Kafka connection - connection is not TLS")
	}
	pcs := tlsConn.ConnectionState().PeerCertificates
	if len(pcs) == 0 {
		return errors.New("cannot use TLS authentication on Kafka connection - TLS is not configured for client certificates")
	}
	if len(pcs) > 1 {
		return errors.New("client has provided more than one certificate - please make sure only one cerftificate is provided")
	}
	principal := pcs[0].Subject.String()
	log.Infof("setting auth principal to %s", principal)
	c.authContext.Principal = principal
	c.authContext.Authenticated = true
	return nil
}
