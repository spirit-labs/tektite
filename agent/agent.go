package agent

import (
	"github.com/spirit-labs/tektite/kafkaserver2"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/pusher"
	"sync"
)

// Agent is currently just a place-holder
type Agent struct {
	lock        sync.Mutex
	cfg         Conf
	started     bool
	kafkaServer *kafkaserver2.KafkaServer
	tablePusher *pusher.TablePusher
}

func NewAgent(cfg Conf, objStore objstore.Client, clientFactory pusher.ControllerClientFactory,
	topicProvider pusher.TopicInfoProvider) (*Agent, error) {
	agent := &Agent{
		cfg: cfg,
	}
	rb, err := pusher.NewTablePusher(cfg.PusherConf, topicProvider, objStore, clientFactory)
	if err != nil {
		return nil, err
	}
	agent.tablePusher = rb
	return agent, nil
}

func (a *Agent) Start() error {
	a.lock.Lock()
	defer a.lock.Unlock()
	if a.started {
		return nil
	}
	kafkaServer := kafkaserver2.NewKafkaServer(a.cfg.KafkaAddress, a.cfg.TlsConf, a.cfg.AuthenticationType, a.newKafkaHandler)
	if err := kafkaServer.Start(); err != nil {
		return err
	}
	a.kafkaServer = kafkaServer
	a.started = true
	return nil
}

func (a *Agent) Stop() error {
	a.lock.Lock()
	defer a.lock.Unlock()
	if !a.started {
		return nil
	}
	if err := a.kafkaServer.Stop(); err != nil {
		return err
	}
	a.started = false
	return nil
}
