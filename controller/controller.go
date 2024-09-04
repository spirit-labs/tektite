package controller

import (
	"github.com/spirit-labs/tektite/replicator"
	"github.com/spirit-labs/tektite/transport"
)

type Controller struct {
	cfg        Config
	transport  transport.Transport
	replicator *replicator.Replicator
}

type Config struct {
	ReplicationFactor int
}

func New(transport transport.Transport, clusterState replicator.ClusterState, cfg Config) *Controller {
	repl := replicator.NewReplicator(transport, cfg.ReplicationFactor, clusterState)
	return &Controller{
		cfg:        cfg,
		transport:  transport,
		replicator: repl,
	}
}

func (c *Controller) Start() error {
	c.replicator.RegisterStateMachineFactory(getOffsetsCommandType, func() replicator.StateMachine {
		return newOffsetsCommandHandler()
	}, 100)
	return nil
}

func (c *Controller) Stop() error {
	c.replicator.Stop()
	return nil
}
