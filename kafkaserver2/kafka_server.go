package kafkaserver2

import (
	"crypto/tls"
	"encoding/binary"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/auth"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	"github.com/spirit-labs/tektite/sockserver"
	"net"
	"sync"
)

type KafkaServer struct {
	address            string
	tlsConf            conf.TLSConfig
	socketServer       *sockserver.SocketServer
	saslAuthManager    *auth.SaslAuthManager
	authenticationType string
	handlerFactory     HandlerFactory
}

type HandlerFactory func(ctx ConnectionContext) kafkaprotocol.RequestHandler

type ConnectionContext interface {
	AuthContext() auth.Context
}

func NewKafkaServer(address string, tlsConf conf.TLSConfig, authenticationType string, handlerFactory HandlerFactory) *KafkaServer {
	return &KafkaServer{
		address:            address,
		tlsConf:            tlsConf,
		authenticationType: authenticationType,
		handlerFactory:     handlerFactory,
	}
}

func (k *KafkaServer) Start() error {
	socketServer := sockserver.NewSocketServer(k.address, k.tlsConf, k.createConnection)
	if err := socketServer.Start(); err != nil {
		return err
	}
	k.socketServer = socketServer
	return nil
}

func (k *KafkaServer) Stop() error {
	return k.socketServer.Stop()
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

func (c *kafkaConnection) AuthContext() auth.Context {
	return c.authContext
}

func (c *kafkaConnection) HandleMessage(message []byte) error {
	if !c.authContext.Authenticated && c.s.authenticationType == auth.AuthenticationTLS {
		if err := c.authoriseWithClientCert(); err != nil {
			return err
		}
	}
	apiKey := int16(binary.BigEndian.Uint16(message))
	authType := c.s.authenticationType
	authenticated := authType == "" || apiKey == kafkaprotocol.APIKeyAPIVersions ||
		apiKey == kafkaprotocol.APIKeySaslHandshake || apiKey == kafkaprotocol.APIKeySaslAuthenticate ||
		c.authContext.Authenticated
	if !authenticated {
		return errors.Errorf("cannot handle Kafka apiKey: %d as authentication type is %s but connection has not been authenticated", apiKey, authType)
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
	c.authContext.Principal = &principal
	c.authContext.Authenticated = true
	return nil
}
