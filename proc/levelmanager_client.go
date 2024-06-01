package proc

import (
	"encoding/binary"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/levels"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/protos/v1/clustermsgs"
	"github.com/spirit-labs/tektite/remoting"
	"github.com/spirit-labs/tektite/retention"
	"sync/atomic"
	"time"
)

const localClientRequestRetryDelay = 100 * time.Millisecond

type LevelManagerLocalClient struct {
	cfg              *conf.Config
	remotingClient   *remoting.Client
	processorManager Manager
	stopped          atomic.Bool
}

func NewLevelManagerLocalClient(cfg *conf.Config) *LevelManagerLocalClient {
	return &LevelManagerLocalClient{
		cfg:            cfg,
		remotingClient: remoting.NewClient(cfg.ClusterTlsConfig),
	}
}

func (l *LevelManagerLocalClient) SetProcessorManager(processorManger Manager) {
	l.processorManager = processorManger
}

func (l *LevelManagerLocalClient) GetTableIDsForRange(keyStart []byte, keyEnd []byte) (levels.OverlappingTableIDs,
	uint64, []levels.VersionRange, error) {
	req := &clustermsgs.LevelManagerGetTableIDsForRangeMessage{
		KeyStart: keyStart,
		KeyEnd:   keyEnd,
	}
	r, err := l.sendLevelManagerRequestWithRetry(req)
	if err != nil {
		return nil, 0, nil, err
	}
	resp := r.(*clustermsgs.LevelManagerGetTableIDsForRangeResponse)
	bytes := resp.Payload
	otids := levels.DeserializeOverlappingTableIDs(bytes, 0)
	versionRanges := make([]levels.VersionRange, len(resp.DeadVersions))
	for i, rng := range resp.DeadVersions {
		versionRanges[i] = levels.VersionRange{
			VersionStart: rng.VersionStart,
			VersionEnd:   rng.VersionEnd,
		}
	}
	return otids, resp.LevelManagerNow, versionRanges, nil
}

func (l *LevelManagerLocalClient) GetPrefixRetentions() ([]retention.PrefixRetention, error) {
	if l.processorManager == nil {
		panic("processor manager not set")
	}
	req := &clustermsgs.LevelManagerGetPrefixRetentionsMessage{}
	r, err := l.sendLevelManagerRequest(req)
	if err != nil {
		return nil, err
	}
	resp := r.(*clustermsgs.LevelManagerRawResponse)
	bytes := resp.Payload
	prefixRetentions, _ := retention.DeserializePrefixRetentions(bytes, 0)
	return prefixRetentions, nil
}

func (l *LevelManagerLocalClient) RegisterL0Tables(registrationBatch levels.RegistrationBatch) error {
	if l.processorManager == nil {
		panic("processor manager not set")
	}
	buff := registrationBatch.Serialize(nil)
	req := &clustermsgs.LevelManagerL0AddRequest{Payload: buff}
	_, err := l.sendLevelManagerRequest(req)
	return err
}

func (l *LevelManagerLocalClient) ApplyChanges(registrationBatch levels.RegistrationBatch) error {
	bytes := make([]byte, 0, 256)
	bytes = append(bytes, levels.ApplyChangesCommand)
	bytes = registrationBatch.Serialize(bytes)
	return ingestCommandBatchSync(bytes, l.processorManager, *l.cfg.ProcessorCount)
}

func (l *LevelManagerLocalClient) RegisterDeadVersionRange(versionRange levels.VersionRange, clusterName string, clusterVersion int) error {
	bytes := make([]byte, 0, 17)
	bytes = append(bytes, levels.RegisterDeadVersionRangeCommand)
	bytes = versionRange.Serialize(bytes)
	bytes = encoding.AppendStringToBufferLE(bytes, clusterName)
	bytes = encoding.AppendUint64ToBufferLE(bytes, uint64(clusterVersion))
	return ingestCommandBatchSync(bytes, l.processorManager, *l.cfg.ProcessorCount)
}

func (l *LevelManagerLocalClient) RegisterPrefixRetentions(prefixRetentions []retention.PrefixRetention) error {
	bytes := levels.EncodeRegisterPrefixRetentionsCommand(prefixRetentions)
	return ingestCommandBatchSync(bytes, l.processorManager, *l.cfg.ProcessorCount)
}

func (l *LevelManagerLocalClient) PollForJob() (*levels.CompactionJob, error) {
	if l.processorManager == nil {
		panic("processor manager not set")
	}
	req := &clustermsgs.CompactionPollMessage{}
	r, err := l.sendLevelManagerRequest(req)
	if err != nil {
		return nil, err
	}
	resp := r.(*clustermsgs.CompactionPollResponse)
	job := &levels.CompactionJob{}
	job.Deserialize(resp.Job, 0)
	return job, nil
}

func (l *LevelManagerLocalClient) StoreLastFlushedVersion(version int64) error {
	if l.processorManager == nil {
		panic("processor manager not set")
	}
	bytes := make([]byte, 0, 256)
	bytes = append(bytes, levels.StoreLastFlushedVersionCommand)
	bytes = binary.LittleEndian.AppendUint64(bytes, uint64(version))
	return ingestCommandBatchSync(bytes, l.processorManager, *l.cfg.ProcessorCount)
}

func (l *LevelManagerLocalClient) LoadLastFlushedVersion() (int64, error) {
	if l.processorManager == nil {
		panic("processor manager not set")
	}
	req := &clustermsgs.LevelManagerLoadLastFlushedVersionMessage{}
	r, err := l.sendLevelManagerRequest(req)
	if err != nil {
		return 0, err
	}
	resp := r.(*clustermsgs.LevelManagerLoadLastFlushedVersionResponse)
	return resp.LastFlushedVersion, nil
}

func (l *LevelManagerLocalClient) GetStats() (levels.Stats, error) {
	if l.processorManager == nil {
		panic("processor manager not set")
	}
	req := &clustermsgs.LevelManagerGetStatsMessage{}
	r, err := l.sendLevelManagerRequest(req)
	if err != nil {
		return levels.Stats{}, err
	}
	resp := r.(*clustermsgs.LevelManagerGetStatsResponse)
	stats := levels.Stats{}
	stats.Deserialize(resp.Payload, 0)
	return stats, nil
}

func ingestCommandBatchSync(bytes []byte, forwarder BatchForwarder, processorID int) error {
	ch := make(chan error, 1)
	ingestCommandBatch(bytes, forwarder, processorID, func(err error) {
		ch <- err
	})
	return <-ch
}

func ingestCommandBatch(bytes []byte, forwarder BatchForwarder, processorID int, completionFunc func(error)) {
	builders := evbatch.CreateColBuilders(levels.CommandColumnTypes)
	builders[0].(*evbatch.BytesColBuilder).Append(bytes)
	evBatch := evbatch.NewBatchFromBuilders(levels.CommandSchema, builders...)
	pb := NewProcessBatch(processorID, evBatch, common.LevelManagerReceiverID, 0, -1)
	forwarder.ForwardBatch(pb, true, completionFunc)
}

func NewLevelManagerCommandIngestor(mgr Manager) func([]byte, func(error)) {
	m := mgr.(*ProcessorManager)
	processorID := *m.cfg.ProcessorCount // last one is the level-manager processor
	return func(bytes []byte, completionFunc func(error)) {
		ingestCommandBatch(bytes, m, processorID, completionFunc)
	}
}

func (l *LevelManagerLocalClient) Start() error {
	return nil
}

func (l *LevelManagerLocalClient) Stop() error {
	l.stopped.Store(true)
	l.remotingClient.Stop()
	return nil
}

func (l *LevelManagerLocalClient) sendLevelManagerRequestWithRetry(request remoting.ClusterMessage) (remoting.ClusterMessage, error) {
	for {
		resp, err := l.sendLevelManagerRequest(request)
		if err != nil {
			if l.stopped.Load() {
				return nil, errors.New("client is stopped")
			}
			if common.IsUnavailableError(err) {
				log.Warnf("failed to send levelManager request. Will retry. %v", err)
				time.Sleep(localClientRequestRetryDelay)
				continue
			}
			return nil, err
		}
		return resp, nil
	}
}

func (l *LevelManagerLocalClient) sendLevelManagerRequest(request remoting.ClusterMessage) (remoting.ClusterMessage, error) {
	processorID := *l.cfg.ProcessorCount // last processor is level-manager one
	leader, err := l.processorManager.GetLeaderNode(processorID)
	if err != nil {
		return nil, err
	}
	serverAddress := l.cfg.ClusterAddresses[leader]
	resp, err := l.remotingClient.SendRPC(request, serverAddress)
	if err != nil {
		return nil, remoting.MaybeConvertError(err)
	}
	return resp, nil
}
