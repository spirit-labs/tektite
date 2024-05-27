package levels

import (
	"encoding/binary"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/protos/v1/clustermsgs"
	"github.com/spirit-labs/tektite/remoting"
	"github.com/spirit-labs/tektite/retention"
	"sync"
	"sync/atomic"
	"time"
)

type Client interface {
	GetTableIDsForRange(keyStart []byte, keyEnd []byte) (OverlappingTableIDs, uint64, []VersionRange, error)

	GetPrefixRetentions() ([]retention.PrefixRetention, error)

	RegisterL0Tables(registrationBatch RegistrationBatch) error

	ApplyChanges(registrationBatch RegistrationBatch) error

	RegisterPrefixRetentions(prefixRetentions []retention.PrefixRetention) error

	PollForJob() (*CompactionJob, error)

	RegisterDeadVersionRange(versionRange VersionRange, clusterName string, clusterVersion int) error

	StoreLastFlushedVersion(version int64) error

	LoadLastFlushedVersion() (int64, error)

	GetStats() (Stats, error)

	Start() error

	Stop() error
}

type ClientFactory interface {
	CreateLevelManagerClient() Client
}

type externalClient struct {
	remotingClient   *remoting.Client
	addresses        []string
	serverRetryDelay time.Duration
	leaderNode       int32
	bootstrapPos     int
	getAddressLock   sync.Mutex
}

func NewExternalClient(serverAddresses []string, tlsConf conf.TLSConfig, serverRetryDelay time.Duration) Client {
	return &externalClient{
		remotingClient:   remoting.NewClient(tlsConf),
		addresses:        serverAddresses,
		serverRetryDelay: serverRetryDelay,
		leaderNode:       -1,
	}
}

func (c *externalClient) GetTableIDsForRange(keyStart []byte, keyEnd []byte) (OverlappingTableIDs, uint64, []VersionRange, error) {
	req := &clustermsgs.LevelManagerGetTableIDsForRangeMessage{
		KeyStart: keyStart,
		KeyEnd:   keyEnd,
	}
	r, err := c.sendRpcWithRetryOnNoLeader(req)
	if err != nil {
		return nil, 0, nil, err
	}
	resp := r.(*clustermsgs.LevelManagerGetTableIDsForRangeResponse)
	otids := DeserializeOverlappingTableIDs(resp.Payload, 0)
	versionRanges := make([]VersionRange, len(resp.DeadVersions))
	for i, rng := range resp.DeadVersions {
		versionRanges[i] = VersionRange{
			VersionStart: rng.VersionStart,
			VersionEnd:   rng.VersionEnd,
		}
	}
	return otids, resp.LevelManagerNow, versionRanges, nil
}

func (c *externalClient) GetPrefixRetentions() ([]retention.PrefixRetention, error) {
	req := &clustermsgs.LevelManagerGetPrefixRetentionsMessage{}
	r, err := c.sendRpcWithRetryOnNoLeader(req)
	if err != nil {
		return nil, err
	}
	resp := r.(*clustermsgs.LevelManagerRawResponse)
	prefixRetentions, _ := retention.DeserializePrefixRetentions(resp.Payload, 0)
	return prefixRetentions, nil
}

func (c *externalClient) RegisterL0Tables(registrationBatch RegistrationBatch) error {
	buff := registrationBatch.Serialize(nil)
	req := &clustermsgs.LevelManagerL0AddRequest{Payload: buff}
	_, err := c.sendRpcWithRetryOnNoLeader(req)
	return err
}

func (c *externalClient) ApplyChanges(registrationBatch RegistrationBatch) error {
	bytes := make([]byte, 0, 256)
	bytes = append(bytes, ApplyChangesCommand)
	bytes = registrationBatch.Serialize(bytes)
	req := &clustermsgs.LevelManagerApplyChangesRequest{Payload: bytes}
	_, err := c.sendRpcWithRetryOnNoLeader(req)
	return err
}

func (c *externalClient) RegisterDeadVersionRange(versionRange VersionRange, clusterName string, clusterVersion int) error {
	bytes := make([]byte, 0, 17)
	bytes = append(bytes, RegisterDeadVersionRangeCommand)
	bytes = versionRange.Serialize(bytes)
	bytes = encoding.AppendStringToBufferLE(bytes, clusterName)
	bytes = encoding.AppendUint64ToBufferLE(bytes, uint64(clusterVersion))
	req := &clustermsgs.LevelManagerRegisterDeadVersionRangeRequest{Payload: bytes}
	_, err := c.sendRpcWithRetryOnNoLeader(req)
	return err
}

func (c *externalClient) RegisterPrefixRetentions(prefixRetentions []retention.PrefixRetention) error {
	bytes := make([]byte, 0, 256)
	bytes = retention.SerializePrefixRetentions(bytes, prefixRetentions)
	req := &clustermsgs.LevelManagerRegisterPrefixRetentionsRequest{Payload: bytes}
	_, err := c.sendRpcWithRetryOnNoLeader(req)
	return err
}

func (c *externalClient) PollForJob() (*CompactionJob, error) {
	req := &clustermsgs.CompactionPollMessage{}
	r, err := c.sendRpcWithRetryOnNoLeader(req)
	if err != nil {
		return nil, err
	}
	pollResp := r.(*clustermsgs.CompactionPollResponse)
	job := &CompactionJob{}
	job.Deserialize(pollResp.Job, 0)
	return job, nil
}

func (c *externalClient) StoreLastFlushedVersion(version int64) error {
	req := &clustermsgs.LevelManagerStoreLastFlushedVersionMessage{LastFlushedVersion: version}
	_, err := c.sendRpcWithRetryOnNoLeader(req)
	return err
}

func (c *externalClient) LoadLastFlushedVersion() (int64, error) {
	req := &clustermsgs.LevelManagerLoadLastFlushedVersionMessage{}
	r, err := c.sendRpcWithRetryOnNoLeader(req)
	if err != nil {
		return 0, err
	}
	resp := r.(*clustermsgs.LevelManagerLoadLastFlushedVersionResponse)
	return resp.LastFlushedVersion, nil
}

func (c *externalClient) GetStats() (Stats, error) {
	req := &clustermsgs.LevelManagerGetStatsMessage{}
	r, err := c.sendRpcWithRetryOnNoLeader(req)
	if err != nil {
		return Stats{}, err
	}
	resp := r.(*clustermsgs.LevelManagerGetStatsResponse)
	var stats Stats
	stats.Deserialize(resp.Payload, 0)
	return stats, nil
}

func (c *externalClient) Start() error {
	return nil
}

func (c *externalClient) Stop() error {
	c.remotingClient.Stop()
	return nil
}

func (c *externalClient) sendRpcWithRetryOnNoLeader(req remoting.ClusterMessage) (remoting.ClusterMessage, error) {
	for {
		var address string
		leaderNode := c.getLeaderNode()
		if leaderNode != -1 {
			address = c.addresses[c.leaderNode]
		} else {
			address = c.getBootstrapAddress()
		}
		resp, err := c.remotingClient.SendRPC(req, address)
		if err == nil {
			return resp, nil
		}
		var terr errors.TektiteError
		if errors.As(err, &terr) {
			if terr.Code == errors.LevelManagerNotLeaderNode {
				leaderNode := binary.LittleEndian.Uint32(terr.ExtraData)
				c.setLeaderNode(int(leaderNode))
				// We will retry on the correct leader
				continue
			}
		}
		c.setLeaderNode(-1)
		return nil, err
	}
}

func (c *externalClient) getBootstrapAddress() string {
	c.getAddressLock.Lock()
	defer c.getAddressLock.Unlock()
	address := c.addresses[c.bootstrapPos]
	c.bootstrapPos++
	if c.bootstrapPos == len(c.addresses) {
		c.bootstrapPos = 0
	}
	return address
}

func (c *externalClient) getLeaderNode() int {
	return int(atomic.LoadInt32(&c.leaderNode))
}

func (c *externalClient) setLeaderNode(leaderNode int) {
	atomic.StoreInt32(&c.leaderNode, int32(leaderNode))
}
