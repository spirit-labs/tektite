package agent

import (
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/cluster"
	"github.com/spirit-labs/tektite/control"
	"github.com/spirit-labs/tektite/fetchcache"
	"github.com/spirit-labs/tektite/fetcher"
	"github.com/spirit-labs/tektite/group"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/pusher"
	"github.com/spirit-labs/tektite/tx"
)

type Conf struct {
	ClusterListenerConfig   ListenerConfig
	KafkaListenerConfig     ListenerConfig
	ClusterMembershipConfig cluster.MembershipConf
	PusherConf              pusher.Conf
	ControllerConf          control.Conf
	CompactionWorkersConf   lsm.CompactionWorkerServiceConf
	FetcherConf             fetcher.Conf
	FetchCacheConf          fetchcache.Conf
	GroupCoordinatorConf    group.Conf
	TxCoordinatorConf tx.Conf
}

func NewConf() Conf {
	return Conf{
		ClusterMembershipConfig: cluster.NewMembershipConf(),
		PusherConf:              pusher.NewConf(),
		ControllerConf:          control.NewConf(),
		CompactionWorkersConf:   lsm.NewCompactionWorkerServiceConf(),
		FetcherConf:             fetcher.NewConf(),
		FetchCacheConf:          fetchcache.NewConf(),
		GroupCoordinatorConf:    group.NewConf(),
		TxCoordinatorConf:       tx.NewConf(),
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
	if err := c.CompactionWorkersConf.Validate(); err != nil {
		return err
	}
	if err := c.FetcherConf.Validate(); err != nil {
		return err
	}
	if err := c.FetchCacheConf.Validate(); err != nil {
		return err
	}
	if err := c.GroupCoordinatorConf.Validate(); err != nil {
		return err
	}
	if err := c.TxCoordinatorConf.Validate(); err != nil {
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
