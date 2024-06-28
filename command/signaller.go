// Copyright 2024 The Tektite Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package command

import (
	"github.com/spirit-labs/tektite/conf"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/protos/clustermsgs"
	"github.com/spirit-labs/tektite/remoting"
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
