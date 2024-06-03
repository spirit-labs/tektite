package server

import (
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/errors"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/protos/clustermsgs"
	"github.com/spirit-labs/tektite/remoting"
)

/*
Shutdown is a multiphased process which ensures that all data is flushed to permanent storage before the cluster is
terminated.
It is co-ordinated by the 'tek-shutdown' command which sends RPCs to all nodes in the cluster to perform each phase.
The phases are
1. Prevent streams being deployed/undeployed. Prevent any new replications being accepted, and wait for all in-progress replications to complete. Then prevent
all processors from accepting new ingests.
2. Wait for the current snapshot version and the version after that to complete. After this we know that all data
processing is complete and no more will occur. Any data will have been written to the store, but the data may not have been flushed to cloud storage yet.
3. Stop version manager. Flush all data-stores. This ensures all loclly written data has been written to cloud storage and the meta-data
stored in the level-manager.
4. Flush the level-manager, this ensures all meta-data written to the level-manager is flushed to permanent storage.
5. Freeze all processor managers to ensure no more cluster updates will be processed. This prevents nodes failing
over onto remaining nodes as they are terminated.
6. Actually close the servers and let main exit.

If the shutdown process does not complete successfully, e.g. a cluster failure occurs during the process, then the
tek-shutdown command will retry it from the beginning, until it succeeds.
*/

type shutdownMessageHandler struct {
	s *Server
}

func (s *shutdownMessageHandler) HandleMessage(messageHolder remoting.MessageHolder, completionFunc func(remoting.ClusterMessage, error)) {
	s.s.executePhase(messageHolder.Message.(*clustermsgs.ShutdownMessage), completionFunc)
}

func (s *Server) executePhase(msg *clustermsgs.ShutdownMessage, completionFunc func(remoting.ClusterMessage, error)) {
	s.lock.Lock()
	defer s.lock.Unlock()
	phase := int(msg.Phase)
	if phase > 1 {
		if s.shutDownPhase != phase-1 {
			// Phases must be completed in sequence, but the process can be restarted from phase 1 in case of failure
			completionFunc(nil, errors.NewTektiteErrorf(errors.ShutdownError,
				"shutdown not in phase sequence"))
			return
		}
		if s.processorManager.FailoverOccurred() {
			// cluster failure(s) happened during the shutdown process - this could leave the possibility of unflushed
			// data, so we terminate the shutdown process - it will be retried from the command.
			completionFunc(nil, errors.NewTektiteErrorf(errors.ShutdownError,
				"cluster failure(s) occurred - shutdown will be terminated"))
			return
		}
	}

	versionManagerFlushed := false
	levelManagerFlushed := false
	var err error
	log.Debugf("node %d executing shutdown phase %d", s.nodeID, phase)
	switch phase {
	case 1:
		s.streamManager.PrepareForShutdown()
		err = s.streamManager.StopIngest()
		if err == nil {
			s.processorManager.PrepareForShutdown()
		}
	case 2:
		// Wait for all processing to complete
		s.processorManager.WaitForProcessingToComplete()
	case 3:
		log.Debugf("node %d executing store flush", s.conf.NodeID)

		// All data has been written, we can flush. Note, completion of this stage does not mean all data is
		// actually flushed from the store, it may not be able to flush some data because version > last completed.
		// Until version manager is shutdown and further versions complete, the store still needs to
		// respond to last completed version broadcasts and flush versions.
		err = s.store.Flush(true, true)

		log.Debugf("node %d shutdown, store flush called, err: %v", s.conf.NodeID, err)
	case 4:
		// shutdown the version manager - this waits for specified version to be flushed and stores it in the
		// level manager
		log.Debugf("node %d shutdown, calling vmgr shutdown", s.conf.NodeID)
		versionManagerFlushed, err = s.versionManager.Shutdown()
		log.Debugf("node %d shutdown, version manager shutdown called, flushed: %t err: %v",
			s.conf.NodeID, versionManagerFlushed, err)
	case 5:
		// Now we can now shut down the store - this needs to be done before level manager is shut down, as there can
		// be a call register tables with level manager in progress. Stopping the store will wait for that to complete
		// before shutting down level manager
		log.Debug("shutting down store")
		err = s.store.Stop()
		if err == nil {
			// Now we must acquiesce the level manager replicator - this is necessary, so we don't get attempts to apply
			// actions on the level manager after it has shutdown
			err = s.processorManager.AcquiesceLevelManagerProcessor()
		}
		log.Debug("acquiesced level manager processor")
	case 6:
		levelManagerFlushed, err = s.levelManagerService.Shutdown()
	case 7:
		// freeze the processor manager - this means it won't handle any more cluster updates
		s.processorManager.Freeze()
		// tell the cluster manager client the cluster is shutting down - this prevents it handling any more cluster
		// changes, and means it will delete the cluster state key when stopped
		s.clusterStateManager.PrepareForShutdown()
	case 8:
		// We need to do this async - it will cause remoting connections to be closed, and connections cannot be closed
		// from the readloop of the connection (and we're called from there here), as they wait on a WaitGroup for the
		// readLoop to terminate
		common.Go(func() {
			if err := s.Shutdown(); err != nil {
				completionFunc(nil, err)
			} else {
				completionFunc(&clustermsgs.ShutdownResponse{Flushed: false}, nil)
			}
		})
		return
	}
	if err != nil {
		log.Debugf("shutdown on node %d returned error: %v", s.conf.NodeID, err)
		completionFunc(nil, err)
		return
	}

	s.shutDownPhase = phase
	completionFunc(&clustermsgs.ShutdownResponse{Flushed: levelManagerFlushed || versionManagerFlushed}, nil)
}

func (s *Server) WasShutdown() bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.shutdownComplete
}
