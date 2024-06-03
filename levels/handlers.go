package levels

import (
	"encoding/binary"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/protos/clustermsgs"
	"github.com/spirit-labs/tektite/remoting"
	"github.com/spirit-labs/tektite/retention"
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
	tids, lmNow, deadVersions, err := g.ms.levelManager.GetTableIDsForRange(msg.KeyStart, msg.KeyEnd)
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
		Payload:         bytes,
		LevelManagerNow: lmNow,
		DeadVersions:    dead,
	}, nil
}

type getPrefixRetentionsHandler struct {
	ms *LevelManagerService
}

func (g *getPrefixRetentionsHandler) HandleMessage(remoting.MessageHolder) (remoting.ClusterMessage, error) {
	g.ms.lock.RLock()
	defer g.ms.lock.RUnlock()
	if g.ms.levelManager == nil {
		return nil, createNotLeaderError(g.ms)
	}
	prefixRetentions, err := g.ms.levelManager.GetPrefixRetentions()
	if err != nil {
		return nil, err
	}
	bytes := make([]byte, 0, 256)
	bytes = retention.SerializePrefixRetentions(bytes, prefixRetentions)
	return &clustermsgs.LevelManagerRawResponse{Payload: bytes}, nil
}

type l0AddHandler struct {
	ms *LevelManagerService
}

func (l *l0AddHandler) HandleMessage(holder remoting.MessageHolder, completionFunc func(remoting.ClusterMessage, error)) {
	l.ms.lock.RLock()
	defer l.ms.lock.RUnlock()
	if l.ms.levelManager == nil {
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

type registerPrefixRetentionHandler struct {
	ms *LevelManagerService
}

func (r *registerPrefixRetentionHandler) HandleMessage(holder remoting.MessageHolder) (remoting.ClusterMessage, error) {
	r.ms.lock.RLock()
	defer r.ms.lock.RUnlock()
	if r.ms.levelManager == nil {
		return nil, createNotLeaderError(r.ms)
	}
	msg := holder.Message.(*clustermsgs.LevelManagerRegisterPrefixRetentionsRequest)
	buff := make([]byte, 0, 1+len(msg.Payload))
	buff = append(buff, RegisterPrefixRetentionsCommand)
	buff = append(buff, msg.Payload...)
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
	remotingServer.RegisterBlockingMessageHandler(remoting.ClusterMessageLevelManagerGetPrefixRetentionsMessage,
		&getPrefixRetentionsHandler{ms: l})
	remotingServer.RegisterBlockingMessageHandler(remoting.ClusterMessageLevelManagerApplyChangesMessage,
		&applyChangesHandler{ms: l})
	remotingServer.RegisterMessageHandler(remoting.ClusterMessageLevelManagerL0AddMessage, &l0AddHandler{ms: l})
	remotingServer.RegisterBlockingMessageHandler(remoting.ClusterMessageLevelManagerRegisterPrefixRetentionsMessage,
		&registerPrefixRetentionHandler{ms: l})
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

func EncodeRegisterPrefixRetentionsCommand(prefixRetentions []retention.PrefixRetention) []byte {
	buff := make([]byte, 0, 256)
	buff = append(buff, RegisterPrefixRetentionsCommand)
	buff = retention.SerializePrefixRetentions(buff, prefixRetentions)
	return buff
}

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
