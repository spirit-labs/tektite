package proc

import (
	"encoding/binary"
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/asl/errwrap"
	"github.com/spirit-labs/tektite/asl/remoting"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/levels"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/protos/clustermsgs"
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

func (l *LevelManagerLocalClient) QueryTablesInRange(keyStart []byte, keyEnd []byte) (levels.OverlappingTables, error) {
	req := &clustermsgs.LevelManagerGetTableIDsForRangeMessage{
		KeyStart: keyStart,
		KeyEnd:   keyEnd,
	}
	r, err := l.sendLevelManagerRequestWithRetry(req)
	if err != nil {
		return nil, err
	}
	resp := r.(*clustermsgs.LevelManagerGetTableIDsForRangeResponse)
	bytes := resp.Payload
	otids := levels.DeserializeOverlappingTables(bytes, 0)
	return otids, nil
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
	buff := make([]byte, 0, 256)
	buff = append(buff, levels.ApplyChangesCommand)
	buff = registrationBatch.Serialize(buff)
	return ingestCommandBatchSync(buff, l.processorManager, l.cfg.ProcessorCount)
}

func (l *LevelManagerLocalClient) RegisterDeadVersionRange(versionRange levels.VersionRange, clusterName string, clusterVersion int) error {
	bytes := make([]byte, 0, 17)
	bytes = append(bytes, levels.RegisterDeadVersionRangeCommand)
	bytes = versionRange.Serialize(bytes)
	bytes = encoding.AppendStringToBufferLE(bytes, clusterName)
	bytes = encoding.AppendUint64ToBufferLE(bytes, uint64(clusterVersion))
	return ingestCommandBatchSync(bytes, l.processorManager, l.cfg.ProcessorCount)
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
	return ingestCommandBatchSync(bytes, l.processorManager, l.cfg.ProcessorCount)
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

func (l *LevelManagerLocalClient) RegisterSlabRetention(slabID int, retention time.Duration) error {
	req := &clustermsgs.LevelManagerRegisterSlabRetentionMessage{SlabId: int64(slabID), Retention: int64(retention)}
	_, err := l.sendLevelManagerRequest(req)
	return err
}

func (l *LevelManagerLocalClient) UnregisterSlabRetention(slabID int) error {
	req := &clustermsgs.LevelManagerUnregisterSlabRetentionMessage{SlabId: int64(slabID)}
	_, err := l.sendLevelManagerRequest(req)
	return err
}

func (l *LevelManagerLocalClient) GetSlabRetention(slabID int) (time.Duration, error) {
	req := &clustermsgs.LevelManagerGetSlabRetentionMessage{SlabId: int64(slabID)}
	r, err := l.sendLevelManagerRequest(req)
	if err != nil {
		return 0, err
	}
	resp := r.(*clustermsgs.LevelManagerGetSlabRetentionResponse)
	return time.Duration(resp.Retention), nil
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
	processorID := m.cfg.ProcessorCount // last one is the level-manager processor
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
				return nil, errwrap.New("client is stopped")
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
	processorID := l.cfg.ProcessorCount // last processor is level-manager one
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
