package proc

import (
	"errors"
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/asl/remoting"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/protos/clustermsgs"
	"github.com/spirit-labs/tektite/vmgr"
	"sync/atomic"
	"time"
)

const unavailabilityRetryInterval = 100 * time.Millisecond

func NewVmgrClient(cfg *conf.Config) *VersionManagerClient {
	return &VersionManagerClient{
		remotingClient:    remoting.NewClient(cfg.ClusterTlsConfig),
		remotingAddresses: cfg.ClusterAddresses,
	}
}

type VersionManagerClient struct {
	mgr               *ProcessorManager
	remotingClient    *remoting.Client
	remotingAddresses []string
	stopped           atomic.Bool
}

func (c *VersionManagerClient) SetProcessorManager(pm *ProcessorManager) {
	c.mgr = pm
}

func (c *VersionManagerClient) Start() error {
	return nil
}

func (c *VersionManagerClient) Stop() error {
	c.stopped.Store(true)
	c.remotingClient.Stop()
	return nil
}

func (c *VersionManagerClient) GetVersions() (int, int, int, error) {
	r, err := c.sendMsg(&clustermsgs.GetCurrentVersionMessage{}, true)
	if err != nil {
		return 0, 0, 0, err
	}
	resp := r.(*clustermsgs.VersionsMessage)
	return int(resp.CurrentVersion), int(resp.CompletedVersion), int(resp.FlushedVersion), nil
}

func (c *VersionManagerClient) VersionComplete(version int, requiredCompletions int, commandID int, doom bool, completionFunc func(error)) {
	// Retrying is handled in the processor
	leader, err := c.mgr.GetLeaderNode(vmgr.VersionManagerProcessorID)
	if err != nil {
		completionFunc(err)
		return
	}
	address := c.remotingAddresses[leader]
	c.remotingClient.SendRPCAsync(func(_ remoting.ClusterMessage, err error) {
		completionFunc(remoting.MaybeConvertError(err))
	}, &clustermsgs.VersionCompleteMessage{
		Version:             uint64(version),
		RequiredCompletions: uint64(requiredCompletions),
		CommandId:           int64(commandID),
		Doom:                doom,
	}, address)
}

func (c *VersionManagerClient) FailureDetected(liveProcessorCount int, clusterVersion int) error {
	_, err := c.sendMsg(&clustermsgs.FailureDetectedMessage{
		ProcessorCount: uint64(liveProcessorCount),
		ClusterVersion: uint64(clusterVersion),
	}, true)
	return err
}

func (c *VersionManagerClient) GetLastFailureFlushedVersion(clusterVersion int) (int, error) {
	r, err := c.sendMsg(&clustermsgs.GetLastFailureFlushedVersionMessage{
		ClusterVersion: uint64(clusterVersion),
	}, false)
	if err != nil {
		return 0, err
	}
	resp := r.(*clustermsgs.GetLastFailureFlushedVersionResponse)
	return int(resp.FlushedVersion), nil
}

func (c *VersionManagerClient) FailureComplete(liveProcessorCount int, clusterVersion int) error {
	_, err := c.sendMsg(&clustermsgs.FailureCompleteMessage{
		ProcessorCount: uint64(liveProcessorCount),
		ClusterVersion: uint64(clusterVersion),
	}, false)
	return err
}

func (c *VersionManagerClient) IsFailureComplete(clusterVersion int) (bool, error) {
	r, err := c.sendMsg(&clustermsgs.IsFailureCompleteMessage{
		ClusterVersion: uint64(clusterVersion),
	}, false)
	if err != nil {
		return false, err
	}
	resp := r.(*clustermsgs.IsFailureCompleteResponse)
	return resp.Complete, nil
}

func (c *VersionManagerClient) VersionFlushed(processorID int, version int, clusterVersion int) error {
	_, err := c.sendMsg(&clustermsgs.VersionFlushedMessage{
		ProcessorId:    uint32(processorID),
		Version:        uint64(version),
		ClusterVersion: uint64(clusterVersion),
	}, false)
	return err
}

func (c *VersionManagerClient) sendMsg(msg remoting.ClusterMessage, retry bool) (remoting.ClusterMessage, error) {
	for {
		leader, err := c.mgr.GetLeaderNode(vmgr.VersionManagerProcessorID)
		if err == nil {
			address := c.remotingAddresses[leader]
			var resp remoting.ClusterMessage
			resp, err = c.remotingClient.SendRPC(msg, address)
			if err == nil {
				return resp, err
			}
		}
		if !retry {
			return nil, err
		}
		if common.IsUnavailableError(remoting.MaybeConvertError(err)) {
			if c.stopped.Load() {
				return nil, errors.New("version manager client is stopped")
			}
			// We retry after delay
			time.Sleep(unavailabilityRetryInterval)
			continue
		}
		return nil, err
	}
}
