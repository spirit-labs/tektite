package query

import (
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/asl/remoting"
	"github.com/spirit-labs/tektite/protos/clustermsgs"
)

type DefaultRemoting struct {
	remotingClient *remoting.Client
}

func NewDefaultRemoting(cfg *conf.Config) *DefaultRemoting {
	return &DefaultRemoting{remotingClient: remoting.NewClient(cfg.ClusterTlsConfig)}
}

func (d *DefaultRemoting) SendQueryMessageAsync(completionFunc func(remoting.ClusterMessage, error),
	request *clustermsgs.QueryMessage, serverAddress string) {
	d.remotingClient.SendRPCAsync(completionFunc, request, serverAddress)
}

func (d *DefaultRemoting) SendQueryResponse(request *clustermsgs.QueryResponse, serverAddress string) error {
	_, err := d.remotingClient.SendRPC(request, serverAddress)
	return err
}

func (d *DefaultRemoting) Close() {
	d.remotingClient.Stop()
}
