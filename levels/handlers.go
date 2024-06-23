package levels

import (
	"encoding/binary"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/errors"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/protos/clustermsgs"
	"github.com/spirit-labs/tektite/remoting"
)

type getTableIDsInRangeMessageHandler struct {
	ms *LevelManagerService
}

func (g *getTableIDsInRangeMessageHandler) HandleMessage(holder remoting.MessageHolder) (remoting.ClusterMessage, error) {
	msg := holder.Message.(*clustermsgs.LevelManagerGetTableIDsForRangeMessage)
	g.ms.lock.RLock()
	defer g.ms.lock.RUnlock()
	if g.ms.levelManager == nil {
		return nil, createNotLeaderError(g.ms)
	}
	tids, deadVersions, err := g.ms.levelManager.GetTableIDsForRange(msg.KeyStart, msg.KeyEnd)
	if err != nil {
		return nil, err
	}
	var bytes []byte
	bytes = tids.Serialize(bytes)
	dead := make([]*clustermsgs.LevelManagerVersionRange, len(deadVersions))
	for i, rng := range deadVersions {
		dead[i] = &clustermsgs.LevelManagerVersionRange{
			VersionStart: rng.VersionStart,
			VersionEnd:   rng.VersionEnd,
		}
	}
	return &clustermsgs.LevelManagerGetTableIDsForRangeResponse{
		Payload:      bytes,
		DeadVersions: dead,
	}, nil
}

type getSlabRetentionHandler struct {
	ms *LevelManagerService
}

func (g *getSlabRetentionHandler) HandleMessage(holder remoting.MessageHolder) (remoting.ClusterMessage, error) {
	g.ms.lock.RLock()
	defer g.ms.lock.RUnlock()
	if g.ms.levelManager == nil {
		return nil, createNotLeaderError(g.ms)
	}
	msg := holder.Message.(*clustermsgs.LevelManagerGetSlabRetentionMessage)
	slabRetention, err := g.ms.levelManager.GetSlabRetention(int(msg.SlabId))
	if err != nil {
		return nil, err
	}
	return &clustermsgs.LevelManagerGetSlabRetentionResponse{Retention: int64(slabRetention)}, nil
}

type l0AddHandler struct {
	ms *LevelManagerService
}

func (l *l0AddHandler) HandleMessage(holder remoting.MessageHolder, completionFunc func(remoting.ClusterMessage, error)) {
	l.ms.lock.RLock()
	defer l.ms.lock.RUnlock()
	log.Infof("got lmgr RegisterL0Tables request")
	if l.ms.levelManager == nil {
		log.Infof("got lmgr RegisterL0Tables request -not leader")
		completionFunc(nil, createNotLeaderError(l.ms))
		return
	}
	msg := holder.Message.(*clustermsgs.LevelManagerL0AddRequest)
	regBatch := RegistrationBatch{}
	regBatch.Deserialize(msg.Payload, 0)
	l.ms.levelManager.RegisterL0Tables(regBatch, func(err error) {
		completionFunc(nil, err)
	})
}

// Only used with external client
type applyChangesHandler struct {
	ms *LevelManagerService
}

func (a *applyChangesHandler) HandleMessage(holder remoting.MessageHolder) (remoting.ClusterMessage, error) {
	a.ms.lock.RLock()
	defer a.ms.lock.RUnlock()
	if a.ms.levelManager == nil {
		return nil, createNotLeaderError(a.ms)
	}
	msg := holder.Message.(*clustermsgs.LevelManagerApplyChangesRequest)
	// This can be received on any node - we forward to the appropriate node
	return nil, a.ms.callCommandBatchIngestorSync(msg.Payload)
}

func (l *LevelManagerService) callCommandBatchIngestorSync(buff []byte) error {
	ch := make(chan error, 1)
	l.commandBatchIngestor(buff, func(err error) {
		ch <- err
	})
	return <-ch
}

type registerSlabRetentionHandler struct {
	ms *LevelManagerService
}

func (r *registerSlabRetentionHandler) HandleMessage(holder remoting.MessageHolder) (remoting.ClusterMessage, error) {
	r.ms.lock.RLock()
	defer r.ms.lock.RUnlock()
	if r.ms.levelManager == nil {
		return nil, createNotLeaderError(r.ms)
	}
	msg := holder.Message.(*clustermsgs.LevelManagerRegisterSlabRetentionMessage)
	buff := make([]byte, 0, 17)
	buff = append(buff, RegisterSlabRetentionCommand)
	buff = encoding.AppendUint64ToBufferLE(buff, uint64(msg.SlabId))
	buff = encoding.AppendUint64ToBufferLE(buff, uint64(msg.Retention))
	return nil, r.ms.callCommandBatchIngestorSync(buff)
}

type unregisterSlabRetentionHandler struct {
	ms *LevelManagerService
}

func (r *unregisterSlabRetentionHandler) HandleMessage(holder remoting.MessageHolder) (remoting.ClusterMessage, error) {
	r.ms.lock.RLock()
	defer r.ms.lock.RUnlock()
	if r.ms.levelManager == nil {
		return nil, createNotLeaderError(r.ms)
	}
	msg := holder.Message.(*clustermsgs.LevelManagerUnregisterSlabRetentionMessage)
	buff := make([]byte, 0, 17)
	buff = append(buff, UnregisterSlabRetentionCommand)
	buff = encoding.AppendUint64ToBufferLE(buff, uint64(msg.SlabId))
	return nil, r.ms.callCommandBatchIngestorSync(buff)
}

type loadLastFlushedVersionHandler struct {
	ms *LevelManagerService
}

func (l *loadLastFlushedVersionHandler) HandleMessage(_ remoting.MessageHolder) (remoting.ClusterMessage, error) {
	l.ms.lock.RLock()
	defer l.ms.lock.RUnlock()
	if l.ms.levelManager == nil {
		return nil, createNotLeaderError(l.ms)
	}
	lfv, err := l.ms.levelManager.LoadLastFlushedVersion()
	if err != nil {
		return nil, err
	}
	return &clustermsgs.LevelManagerLoadLastFlushedVersionResponse{LastFlushedVersion: lfv}, nil
}

type storeLastFlushedVersionHandler struct {
	ms *LevelManagerService
}

func (l *storeLastFlushedVersionHandler) HandleMessage(holder remoting.MessageHolder) (remoting.ClusterMessage, error) {
	l.ms.lock.RLock()
	defer l.ms.lock.RUnlock()
	if l.ms.levelManager == nil {
		return nil, createNotLeaderError(l.ms)
	}
	msg := holder.Message.(*clustermsgs.LevelManagerStoreLastFlushedVersionMessage)
	bytes := make([]byte, 0, 9)
	bytes = append(bytes, StoreLastFlushedVersionCommand)
	bytes = binary.LittleEndian.AppendUint64(bytes, uint64(msg.LastFlushedVersion))
	return nil, l.ms.callCommandBatchIngestorSync(bytes)
}

type getStatsHandler struct {
	ms *LevelManagerService
}

func (g *getStatsHandler) HandleMessage(_ remoting.MessageHolder) (remoting.ClusterMessage, error) {
	g.ms.lock.RLock()
	defer g.ms.lock.RUnlock()
	if g.ms.levelManager == nil {
		return nil, createNotLeaderError(g.ms)
	}
	stats := g.ms.levelManager.GetStats()
	buff := stats.Serialize(nil)
	return &clustermsgs.LevelManagerGetStatsResponse{Payload: buff}, nil
}

// Compaction handlers

type compactionPollMessageHandler struct {
	ms *LevelManagerService
}

func (g *compactionPollMessageHandler) HandleMessage(holder remoting.MessageHolder, completionFunc func(remoting.ClusterMessage, error)) {
	g.ms.lock.RLock()
	defer g.ms.lock.RUnlock()
	if g.ms.levelManager == nil {
		completionFunc(nil, createNotLeaderError(g.ms))
		return
	}
	g.ms.levelManager.pollForJob(holder.ConnectionID, func(job *CompactionJob, err error) {
		if err != nil {
			completionFunc(nil, err)
			return
		}
		buff := job.Serialize(nil)
		completionFunc(&clustermsgs.CompactionPollResponse{Job: buff}, nil)
	})
}

func (l *LevelManagerService) SetClusterMessageHandlers(remotingServer remoting.Server) {
	remotingServer.RegisterBlockingMessageHandler(remoting.ClusterMessageLevelManagerGetTableIDsForRangeMessage,
		&getTableIDsInRangeMessageHandler{ms: l})
	remotingServer.RegisterBlockingMessageHandler(remoting.ClusterMessageLevelManagerGetSlabRetentionMessage,
		&getSlabRetentionHandler{ms: l})
	remotingServer.RegisterBlockingMessageHandler(remoting.ClusterMessageLevelManagerApplyChangesMessage,
		&applyChangesHandler{ms: l})
	remotingServer.RegisterMessageHandler(remoting.ClusterMessageLevelManagerL0AddMessage, &l0AddHandler{ms: l})
	remotingServer.RegisterBlockingMessageHandler(remoting.ClusterMessageLevelManagerRegisterSlabRetentionMessage,
		&registerSlabRetentionHandler{ms: l})
	remotingServer.RegisterBlockingMessageHandler(remoting.ClusterMessageLevelManagerUnregisterSlabRetentionMessage,
		&unregisterSlabRetentionHandler{ms: l})
	remotingServer.RegisterMessageHandler(remoting.ClusterMessageCompactionPollMessage,
		&compactionPollMessageHandler{ms: l})
	remotingServer.RegisterBlockingMessageHandler(remoting.ClusterMessageLevelManagerStoreLastFlushedVersionMessage,
		&storeLastFlushedVersionHandler{ms: l})
	remotingServer.RegisterBlockingMessageHandler(remoting.ClusterMessageLevelManagerLoadLastFlushedVersionMessage,
		&loadLastFlushedVersionHandler{ms: l})
	remotingServer.RegisterBlockingMessageHandler(remoting.ClusterMessageLevelManagerGetStatsMessage,
		&getStatsHandler{ms: l})
	remotingServer.RegisterConnectionClosedHandler(l.connectionClosed)
}

func (l *LevelManagerService) connectionClosed(id int) {
	l.lock.RLock()
	defer l.lock.RUnlock()
	if l.levelManager == nil {
		return
	}
	l.levelManager.connectionClosed(id)
}

type commandBatchIngestor func(bytes []byte, completionFunc func(error))

func createNotLeaderError(lms *LevelManagerService) error {
	leaderNode, err := lms.leaderNodeProvider.GetLeaderNode(lms.cfg.ProcessorCount)
	if err != nil {
		return err
	}
	buff := make([]byte, 4)
	binary.LittleEndian.PutUint32(buff, uint32(leaderNode))
	terr := errors.NewTektiteErrorf(errors.LevelManagerNotLeaderNode, "no level manager on node")
	terr.ExtraData = buff
	return terr
}
