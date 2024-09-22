package agent

import (
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/pusher"
)

type Conf struct {
	KafkaAddress       string
	TlsConf            conf.TLSConfig
	AuthenticationType string
	PusherConf         pusher.Conf
}
