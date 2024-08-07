package cmdmgr

import (
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/asl/remoting"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/protos/clustermsgs"
)

type RemotingSignaller struct {
	mgr            *manager
	remotingClient *remoting.Client
	addresses      []string
}

func NewSignaller(mgr Manager, remotingServer remoting.Server, cfg *conf.Config) *RemotingSignaller {
	var addresses []string
	for i, address := range cfg.ClusterAddresses {
		if i != cfg.NodeID {
			addresses = append(addresses, address)
		}
	}
	signaller := &RemotingSignaller{
		mgr:            mgr.(*manager),
		remotingClient: remoting.NewClient(cfg.ClusterTlsConfig),
		addresses:      addresses,
	}
	remotingServer.RegisterMessageHandler(remoting.ClusterMessageCommandAvailableMessage, signaller)
	return signaller
}

func (r *RemotingSignaller) Start() error {
	return nil
}

func (r *RemotingSignaller) Stop() error {
	r.remotingClient.Stop()
	return nil
}

func (r *RemotingSignaller) CommandAvailable() {
	if err := r.remotingClient.Broadcast(&clustermsgs.CommandAvailableMessage{}, r.addresses...); err != nil {
		log.Warnf("failed to broadcast command available %v", err)
	}
}

func (r *RemotingSignaller) HandleMessage(_ remoting.MessageHolder, completionFunc func(remoting.ClusterMessage, error)) {
	if err := r.mgr.CheckProcessCommands(); err != nil {
		log.Errorf("failed to process commands %v", err)
	}
	completionFunc(nil, nil)
}
