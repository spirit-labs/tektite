package agent

import (
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/control"
	"github.com/spirit-labs/tektite/pusher"
)

type Conf struct {
	ClusterListenerConfig   ListenerConfig
	KafkaListenerConfig     ListenerConfig
	ClusterMembershipConfig cluster.MembershipConf
	PusherConf              pusher.Conf
	ControllerConf          control.Conf
}

func NewConf() Conf {
	return Conf{
		ClusterMembershipConfig: cluster.NewMembershipConf(),
		PusherConf:              pusher.NewConf(),
		ControllerConf:          control.NewConf(),
	}
}

func (c *Conf) Validate() error {
	if err := c.ClusterListenerConfig.Validate(); err != nil {
		return err
	}
	if err := c.KafkaListenerConfig.Validate(); err != nil {
		return err
	}
	if err := c.ClusterMembershipConfig.Validate(); err != nil {
		return err
	}
	if err := c.PusherConf.Validate(); err != nil {
		return err
	}
	if err := c.ControllerConf.Validate(); err != nil {
		return err
	}
	return nil
}

type ListenerConfig struct {
	Address            string
	AdvertisedAddress  string
	TLSConfig          conf.TLSConfig
	AuthenticationType string
}

func (l *ListenerConfig) Validate() error {
	// TODO
	return nil
}
