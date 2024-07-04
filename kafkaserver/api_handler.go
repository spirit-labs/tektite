package kafkaserver

import (
	"encoding/binary"
	"fmt"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/errors"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/types"
	"net"
	"strconv"
	"sync"
	"time"
)

const (
	APIKeyProduce          = 0
	APIKeyFetch            = 1
	APIKeyListOffsets      = 2
	APIKeyMetadata         = 3
	APIKeyOffsetCommit     = 8
	APIKeyOffsetFetch      = 9
	APIKeyFindCoordinator  = 10
	ApiKeyJoinGroup        = 11
	ApiKeyHeartbeat        = 12
	ApiKeyLeaveGroup       = 13
	ApiKeySyncGroup        = 14
	APIKeySaslHandshake    = 17
	APIKeyAPIVersions      = 18
	APIKeySaslAuthenticate = 36
)

const (
	ErrorCodeUnknownServerError          = -1
	ErrorCodeNone                        = 0
	ErrorCodeUnknownTopicOrPartition     = 3
	ErrorCodeLeaderNotAvailable          = 5
	ErrorCodeNotLeaderOrFollower         = 6
	ErrorCodeCoordinatorNotAvailable     = 15
	ErrorCodeNotCoordinator              = 16
	ErrorCodeIllegalGeneration           = 22
	ErrorCodeInconsistentGroupProtocol   = 23
	ErrorCodeUnknownMemberID             = 25
	ErrorCodeInvalidSessionTimeout       = 26
	ErrorCodeRebalanceInProgress         = 27
	ErrorCodeUnsupportedForMessageFormat = 43
	ErrorCodeGroupIDNotFound             = 69
)

func (c *connection) handleApi(clientID NullableString, apiKey int16, apiVersion int16, reqBuff []byte, respBuffHeaderSize int, complFunc func([]byte)) {
	log.Debugf("in handleApi apiKey:%d apiVersion:%d", apiKey, apiVersion)
	switch apiKey {
	case APIKeyProduce:
		complFunc(c.handleProduce(apiVersion, reqBuff, respBuffHeaderSize))
	case APIKeyFetch:
		complFunc(c.handleFetch(apiVersion, reqBuff, respBuffHeaderSize))
	case APIKeyOffsetCommit:
		complFunc(c.handleOffsetCommit(apiVersion, reqBuff, respBuffHeaderSize))
	case APIKeyOffsetFetch:
		complFunc(c.handleOffsetFetch(apiVersion, reqBuff, respBuffHeaderSize))
	case APIKeyListOffsets:
		complFunc(c.handleListOffsets(apiVersion, reqBuff, respBuffHeaderSize))
	case APIKeyMetadata:
		complFunc(c.handleMetadata(apiVersion, reqBuff, respBuffHeaderSize))
	case APIKeyFindCoordinator:
		complFunc(c.handleFindCoordinator(apiVersion, reqBuff, respBuffHeaderSize))
	case ApiKeyJoinGroup:
		c.handleJoinGroup(apiVersion, *clientID, reqBuff, respBuffHeaderSize, complFunc)
	case ApiKeyLeaveGroup:
		complFunc(c.handleLeaveGroup(apiVersion, reqBuff, respBuffHeaderSize))
	case ApiKeySyncGroup:
		c.handleSyncGroup(apiVersion, reqBuff, respBuffHeaderSize, complFunc)
	case ApiKeyHeartbeat:
		complFunc(c.handleHeartbeat(apiVersion, reqBuff, respBuffHeaderSize))
	case APIKeyAPIVersions:
		complFunc(c.handleAPIVersions(apiVersion, reqBuff, respBuffHeaderSize))
	default:
		panic(fmt.Sprintf("unsupported API key %d", apiKey))
	}
}

func (c *connection) handleProduce(apiVersion int16, reqBuff []byte, respBuffHeaderSize int) []byte {
	if apiVersion != 3 {
		panic(fmt.Sprintf("unsupported produce api version %d", apiVersion))
	}
	// transactionalID is next - we do not currently use this
	_, off := ReadNullableStringFromBytes(reqBuff)
	// acks is next - we currently only support acks = all (-1) so we do not use this
	off += 2
	// timeoutMs is next, we don't currently support it - ignore it
	off += 4
	numTopics := int(ReadInt32FromBytes(reqBuff[off:]))
	off += 4

	topicResults := make([]*topicProduceResult, numTopics)

	for i := 0; i < numTopics; i++ {
		topicName, bytesRead := ReadStringFromBytes(reqBuff[off:])

		off += bytesRead
		numPartitions := int(ReadInt32FromBytes(reqBuff[off:]))
		off += 4

		topicResult := newTopicProduceResult(topicName, numPartitions)
		topicResults[i] = topicResult

		for j := 0; j < numPartitions; j++ {
			partitionID := ReadInt32FromBytes(reqBuff[off:])
			off += 4

			topicResult.partitionIDs[j] = partitionID

			topicInfo, ok := c.s.metadataProvider.GetTopicInfo(topicName)
			if !ok {
				topicResult.partitionProduceComplete(j, ErrorCodeUnknownTopicOrPartition, 0, 0)
				continue
			}

			processorID, ok := topicInfo.ProduceInfoProvider.PartitionScheme().PartitionProcessorMapping[int(partitionID)]
			if !ok {
				panic("no processor for partition")
			}
			processor := c.s.procProvider.GetProcessor(processorID)
			if processor == nil {
				topicResult.partitionProduceComplete(j, ErrorCodeUnknownTopicOrPartition, 0, 0)
				continue
			}
			if !processor.IsLeader() {
				topicResult.partitionProduceComplete(j, ErrorCodeNotLeaderOrFollower, 0, 0)
				continue
			}

			var partitionFetcher *PartitionFetcher
			if topicInfo.ConsumeEnabled {
				// can be nil for produce only topic
				var err error
				partitionFetcher, err = c.s.fetcher.GetPartitionFetcher(&topicInfo, partitionID)
				if err != nil {
					log.Errorf("failed to fetch partition fetcher for partition %d: %v", partitionID, err)
					topicResult.partitionProduceComplete(j, ErrorCodeUnknownTopicOrPartition, 0, 0)
					continue
				}
			}

			var recordBatchBytes []byte
			recordBatchLength := int(ReadInt32FromBytes(reqBuff[off:]))
			off += 4
			if recordBatchLength < 58 {
				topicResult.partitionProduceComplete(j, ErrorCodeUnsupportedForMessageFormat, 0, 0)
				continue
			}
			if partitionFetcher != nil {
				var err error
				recordBatchBytes, err = partitionFetcher.Allocate(recordBatchLength)
				if err != nil {
					log.Errorf("failed to allocate buffer %v", err)
					topicResult.partitionProduceComplete(j, ErrorCodeUnknownServerError, 0, 0)
					continue
				}
			} else {
				recordBatchBytes = make([]byte, recordBatchLength)
			}
			copy(recordBatchBytes, reqBuff[off:off+recordBatchLength])
			off += recordBatchLength

			numRecords := int(binary.BigEndian.Uint32(recordBatchBytes[57:]))

			magic := recordBatchBytes[16]
			if magic != 2 {
				topicResult.partitionProduceComplete(j, ErrorCodeUnsupportedForMessageFormat, 0, 0)
				continue
			}

			index := j
			topicInfo.ProduceInfoProvider.IngestBatch(recordBatchBytes, processor, int(partitionID),
				func(err error) {
					processor.CheckInProcessorLoop()
					if err != nil {
						var errorCode int16
						if common.IsUnavailableError(err) {
							log.Warnf("failed to replicate produce batch %v", err)
							// This can occur, e.g. due to insufficient replicas available, or replicas are currently
							// syncing. It should resolve when sufficient nodes become available or sync completes.
							errorCode = ErrorCodeLeaderNotAvailable
						} else {
							log.Errorf("failed to replicate produce batch %v", err)
							errorCode = ErrorCodeUnknownServerError
						}
						topicResult.partitionProduceComplete(index, errorCode, 0, 0)
						return
					}
					offset, appendTime := topicInfo.ProduceInfoProvider.GetLastProducedInfo(int(partitionID))
					topicResult.partitionProduceComplete(index, ErrorCodeNone, offset, appendTime)
					if partitionFetcher != nil && topicInfo.CanCache {
						partitionFetcher.AddBatch(offset-int64(numRecords)+1, offset, recordBatchBytes)
					}
				})
		}
	}

	// Write the response
	respBuff := make([]byte, respBuffHeaderSize)
	respBuff = AppendInt32ToBytes(respBuff, int32(numTopics))
	for _, topicResult := range topicResults {
		respBuff = AppendStringBytes(respBuff, topicResult.topicName)
		respBuff = AppendInt32ToBytes(respBuff, int32(len(topicResult.partitionResults)))
		topicResult.waitResult()
		for j, partitionResult := range topicResult.partitionResults {
			partitionID := topicResult.partitionIDs[j]
			respBuff = AppendInt32ToBytes(respBuff, partitionID)
			respBuff = AppendInt16ToBytes(respBuff, partitionResult.errorCode)
			respBuff = AppendInt64ToBytes(respBuff, partitionResult.offset)
			respBuff = AppendInt64ToBytes(respBuff, partitionResult.appendTime)
		}
	}
	respBuff = AppendInt32ToBytes(respBuff, 0) // throttleTimeMs
	return respBuff
}

func newTopicProduceResult(topicName string, numPartitions int) *topicProduceResult {
	res := &topicProduceResult{
		topicName:        topicName,
		partitionIDs:     make([]int32, numPartitions),
		partitionResults: make([]partitionProduceResult, numPartitions),
	}
	res.wg.Add(numPartitions)
	return res
}

type topicProduceResult struct {
	lock             sync.Mutex
	topicName        string
	partitionIDs     []int32
	partitionResults []partitionProduceResult
	wg               sync.WaitGroup
}

type partitionProduceResult struct {
	errorCode  int16
	offset     int64
	appendTime int64
}

func (t *topicProduceResult) partitionProduceComplete(index int, errorCode int16, offset int64, appendTime int64) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.partitionResults[index] = partitionProduceResult{
		errorCode:  errorCode,
		offset:     offset,
		appendTime: appendTime,
	}
	t.wg.Done()
}

func (t *topicProduceResult) waitResult() {
	t.wg.Wait()
}

func (c *connection) handleFetch(apiVersion int16, reqBuff []byte, respBuffHeaderSize int) []byte {
	if apiVersion != 4 {
		panic(fmt.Sprintf("unsupported fetch api version %d", apiVersion))
	}
	off := 4 // skip past replicaID
	maxWaitMs := ReadInt32FromBytes(reqBuff[off:])
	off += 4
	minBytes := ReadInt32FromBytes(reqBuff[off:])
	off += 4
	maxBytes := ReadInt32FromBytes(reqBuff[off:])
	off += 4
	off++ // isolationLevel

	numTopics := int(ReadInt32FromBytes(reqBuff[off:]))
	off += 4

	topicResults := make([]*topicFetchResult, numTopics)

	var waiters []*Waiter
	hasData := false
	for i := 0; i < numTopics; i++ {
		topicName, bytesRead := ReadStringFromBytes(reqBuff[off:])
		off += bytesRead

		numPartitions := int(ReadInt32FromBytes(reqBuff[off:]))
		off += 4

		topicResult := newTopicFetchResult(topicName, numPartitions)
		topicResults[i] = topicResult

		topicInfo, ok := c.s.metadataProvider.GetTopicInfo(topicName)

		for j := 0; j < numPartitions; j++ {

			partitionID := ReadInt32FromBytes(reqBuff[off:])
			off += 4

			topicResult.partitionIDs[j] = partitionID

			fetchOffset := int64(binary.BigEndian.Uint64(reqBuff[off:]))
			off += 8
			partitionMaxBytes := ReadInt32FromBytes(reqBuff[off:])
			off += 4

			// Note that fetchMaxBytes is not a hard limit - total bytes returned can be greater than this
			// depending on number of partitions in fetch request and size of first batch available in partition
			fetchMaxBytes := maxBytes
			if partitionMaxBytes < maxBytes {
				fetchMaxBytes = partitionMaxBytes
			}

			if !ok {
				log.Error("sending back unknown topic or partition")
				topicResult.partitionFetchComplete(j, int16(ErrorCodeUnknownTopicOrPartition), 0, nil)
			} else {
				partitionFetcher, err := c.s.fetcher.GetPartitionFetcher(&topicInfo, partitionID)
				if err != nil {
					log.Error("fetching partition fetcher failed: %v", err)
					topicResult.partitionFetchComplete(j, int16(ErrorCodeUnknownTopicOrPartition), 0, nil)
				} else {
					index := j
					waiter := partitionFetcher.Fetch(fetchOffset, int(minBytes), int(fetchMaxBytes), time.Duration(maxWaitMs)*time.Millisecond,
						func(batches [][]byte, hwm int64, err error) {
							errorCode := ErrorCodeNone
							if err != nil {
								var kerr KafkaProtocolError
								if errors.As(err, &kerr) {
									errorCode = kerr.ErrorCode
								} else {
									errorCode = ErrorCodeUnknownServerError
								}
								log.Errorf("failed to execute fetch %v", err)
							}
							topicResult.partitionFetchComplete(index, int16(errorCode), hwm, batches)
						})
					if waiter != nil {
						waiters = append(waiters, waiter)
					} else {
						hasData = true
					}
				}
			}
		}
		if !hasData {
			for _, waiter := range waiters {
				waiter.schedule()
			}
		} else {
			for _, waiter := range waiters {
				waiter.complete()
			}
		}
	}

	// Write the response
	respBuff := make([]byte, respBuffHeaderSize)
	respBuff = AppendInt32ToBytes(respBuff, 0) // throttleTimeMs
	respBuff = AppendInt32ToBytes(respBuff, int32(numTopics))
	for _, topicResult := range topicResults {
		respBuff = AppendStringBytes(respBuff, topicResult.topicName)
		respBuff = AppendInt32ToBytes(respBuff, int32(len(topicResult.partitionResults)))
		topicResult.waitResult()
		for j, partitionResult := range topicResult.partitionResults {
			partitionID := topicResult.partitionIDs[j]
			respBuff = AppendInt32ToBytes(respBuff, partitionID)
			respBuff = AppendInt16ToBytes(respBuff, partitionResult.errorCode)
			respBuff = AppendInt64ToBytes(respBuff, partitionResult.highWaterMark)
			respBuff = AppendInt64ToBytes(respBuff, partitionResult.highWaterMark) // lastStableOffset
			respBuff = AppendInt32ToBytes(respBuff, 0)                             // num abortedTransactions
			if len(partitionResult.batches) == 0 {
				respBuff = AppendInt32ToBytes(respBuff, 0)
			} else {
				totSize := 0
				for _, batch := range partitionResult.batches {
					totSize += len(batch)
				}
				respBuff = AppendInt32ToBytes(respBuff, int32(totSize))
				for _, batch := range partitionResult.batches {
					respBuff = append(respBuff, batch...)
				}
			}
		}
	}
	return respBuff
}

func newTopicFetchResult(topicName string, numPartitions int) *topicFetchResult {
	res := &topicFetchResult{
		topicName:        topicName,
		partitionIDs:     make([]int32, numPartitions),
		partitionResults: make([]*partitionFetchResult, numPartitions),
	}
	res.wg.Add(numPartitions)
	return res
}

type topicFetchResult struct {
	lock             sync.Mutex
	topicName        string
	partitionIDs     []int32
	partitionResults []*partitionFetchResult
	wg               sync.WaitGroup
}

type partitionFetchResult struct {
	errorCode     int16
	highWaterMark int64
	batches       [][]byte
}

func (t *topicFetchResult) fillAllErrors(errorCode int16) {
	for i := 0; i < len(t.partitionResults); i++ {
		t.partitionResults[i].errorCode = errorCode
	}
}

func (t *topicFetchResult) partitionFetchComplete(index int, errorCode int16, hwm int64, batches [][]byte) {
	t.partitionResults[index] = &partitionFetchResult{
		errorCode:     errorCode,
		highWaterMark: hwm,
		batches:       batches,
	}
	t.wg.Done()
}

func (t *topicFetchResult) waitResult() {
	t.wg.Wait()
}

func (c *connection) handleMetadata(apiVersion int16, reqBuff []byte, respBuffHeaderSize int) []byte {
	if apiVersion != 3 {
		panic(fmt.Sprintf("unsupported metadata api version %d", apiVersion))
	}
	numTopics := int(ReadInt32FromBytes(reqBuff))
	reqBuff = reqBuff[4:]
	var topicNames []string
	for i := 0; i < numTopics; i++ {
		topicName, bytesRead := ReadStringFromBytes(reqBuff)
		topicNames = append(topicNames, topicName)
		reqBuff = reqBuff[bytesRead:]
	}

	respBuff := make([]byte, respBuffHeaderSize)
	respBuff = AppendInt32ToBytes(respBuff, 0) // throttleTimeMs
	brokerInfos := c.s.metadataProvider.BrokerInfos()
	respBuff = AppendInt32ToBytes(respBuff, int32(len(brokerInfos)))
	for _, brokerInfo := range brokerInfos {
		respBuff = AppendInt32ToBytes(respBuff, int32(brokerInfo.NodeID))
		respBuff = AppendStringBytes(respBuff, brokerInfo.Host)
		respBuff = AppendInt32ToBytes(respBuff, int32(brokerInfo.Port))
		respBuff = AppendNullableStringToBytes(respBuff, nil) // rack
	}
	respBuff = AppendNullableStringToBytes(respBuff, nil) // cluster id
	respBuff = AppendInt32ToBytes(respBuff, int32(c.s.metadataProvider.ControllerNodeID()))

	if len(topicNames) == 0 {
		// request for all topics
		topicInfos := c.s.metadataProvider.GetAllTopics()
		respBuff = AppendInt32ToBytes(respBuff, int32(len(topicInfos)))
		for _, topicInfo := range topicInfos {
			respBuff = writeTopicInfo(topicInfo, true, respBuff)
		}
	} else {
		respBuff = AppendInt32ToBytes(respBuff, int32(len(topicNames)))
		for _, topicName := range topicNames {
			topicInfo, ok := c.s.metadataProvider.GetTopicInfo(topicName)
			respBuff = writeTopicInfo(&topicInfo, ok, respBuff)
		}
	}
	return respBuff
}

func writeTopicInfo(topicInfo *TopicInfo, ok bool, respBuff []byte) []byte {
	if !ok {
		respBuff = AppendInt16ToBytes(respBuff, ErrorCodeUnknownTopicOrPartition)
	} else {
		respBuff = AppendInt16ToBytes(respBuff, ErrorCodeNone)
	}
	respBuff = AppendStringBytes(respBuff, topicInfo.Name)
	respBuff = append(respBuff, 0) // isInternal
	if !ok {
		respBuff = AppendInt32ToBytes(respBuff, 0)
		return respBuff
	}
	respBuff = AppendInt32ToBytes(respBuff, int32(len(topicInfo.Partitions)))
	for _, partitionInfo := range topicInfo.Partitions {
		respBuff = AppendInt16ToBytes(respBuff, ErrorCodeNone)
		respBuff = AppendInt32ToBytes(respBuff, int32(partitionInfo.ID))
		respBuff = AppendInt32ToBytes(respBuff, int32(partitionInfo.LeaderNodeID))
		// replica nodes
		respBuff = AppendInt32ToBytes(respBuff, int32(len(partitionInfo.ReplicaNodeIDs)))
		for _, replicaNodeID := range partitionInfo.ReplicaNodeIDs {
			respBuff = AppendInt32ToBytes(respBuff, int32(replicaNodeID))
		}
		// isr nodes
		respBuff = AppendInt32ToBytes(respBuff, int32(len(partitionInfo.ReplicaNodeIDs)))
		for _, replicaNodeID := range partitionInfo.ReplicaNodeIDs {
			respBuff = AppendInt32ToBytes(respBuff, int32(replicaNodeID))
		}
	}
	return respBuff
}

func (c *connection) handleAPIVersions(apiVersion int16, reqBuff []byte, respBuffHeaderSize int) []byte {
	if apiVersion >= 3 {
		clientSoftwareName, bytesRead := ReadCompactStringFromBytes(reqBuff)
		clientSoftwareVersion, _ := ReadCompactStringFromBytes(reqBuff[bytesRead:])
		log.Debugf("software name:%s version:%s", clientSoftwareName, clientSoftwareVersion)
	}

	respBuff := make([]byte, respBuffHeaderSize)
	respBuff = append(respBuff, 0, 0) // error code - 0, no error
	if apiVersion < 3 {
		respBuff = AppendInt32ToBytes(respBuff, int32(len(supportedAPIKeys)))
	} else {
		// for api version >= 3 array length is encoded as varint, although this doesn't seem to be documented
		// in the protocol docs
		// Also the Java client expects the actual number of api keys to be 1 + this value !
		respBuff = binary.AppendUvarint(respBuff, uint64(len(supportedAPIKeys))+1)
	}

	for key, versions := range supportedAPIKeys {
		respBuff = AppendInt16ToBytes(respBuff, key)
		respBuff = AppendInt16ToBytes(respBuff, versions.MinVersion)
		respBuff = AppendInt16ToBytes(respBuff, versions.MaxVersion)
		if apiVersion >= 3 {
			respBuff = append(respBuff, 0) // has extra byte for tag buffer
		}
	}
	if apiVersion >= 1 {
		respBuff = append(respBuff, 0, 0, 0, 0) // throttleTimeMs
	}
	if apiVersion >= 3 {
		respBuff = append(respBuff, 0) // tag buffer
	}
	return respBuff
}

func (c *connection) handleFindCoordinator(apiVersion int16, reqBuff []byte, respBuffHeaderSize int) []byte {
	if apiVersion != 0 {
		panic(fmt.Sprintf("unsupported FindCoordinator api version %d", apiVersion))
	}

	key, _ := ReadStringFromBytes(reqBuff)
	nodeID := c.s.groupCoordinator.FindCoordinator(key)
	address := c.s.cfg.KafkaServerListenerConfig.Addresses[nodeID]
	if len(c.s.cfg.KafkaServerListenerConfig.AdvertisedAddresses) > 0 {
		address = c.s.cfg.KafkaServerListenerConfig.AdvertisedAddresses[nodeID]
	}
	host, sPort, err := net.SplitHostPort(address)
	var port int
	if err == nil {
		port, err = strconv.Atoi(sPort)
	}
	if err != nil {
		// Should never happen as addresses will have been verified when returning broker infos in metadata request
		panic(err)
	}
	respBuff := make([]byte, respBuffHeaderSize)
	respBuff = AppendInt16ToBytes(respBuff, ErrorCodeNone)
	respBuff = AppendInt32ToBytes(respBuff, int32(nodeID))
	respBuff = AppendStringBytes(respBuff, host)
	respBuff = AppendInt32ToBytes(respBuff, int32(port))
	return respBuff
}

func (c *connection) handleJoinGroup(apiVersion int16, clientID string, reqBuff []byte, respBuffHeaderSize int, complFunc func([]byte)) {
	if apiVersion != 0 {
		panic(fmt.Sprintf("unsupported JoinGroup api version %d", apiVersion))
	}
	groupID, off := ReadStringFromBytes(reqBuff)
	sessionTimeoutMs := ReadInt32FromBytes(reqBuff[off:])
	off += 4
	memberID, bytesRead := ReadStringFromBytes(reqBuff[off:])
	off += bytesRead
	protocolType, bytesRead := ReadStringFromBytes(reqBuff[off:])
	off += bytesRead

	numProtocols := int(ReadInt32FromBytes(reqBuff[off:]))
	off += 4
	infos := make([]ProtocolInfo, numProtocols)
	for i := 0; i < numProtocols; i++ {
		protocolName, bytesRead := ReadStringFromBytes(reqBuff[off:])
		off += bytesRead
		metaDataSize := int(ReadInt32FromBytes(reqBuff[off:]))
		off += 4
		metaData := make([]byte, metaDataSize)
		toCopy := reqBuff[off : off+metaDataSize]
		copy(metaData, toCopy)
		infos[i] = ProtocolInfo{
			Name:     protocolName,
			Metadata: metaData,
		}
		off += metaDataSize
	}
	sessionTimeout := time.Duration(sessionTimeoutMs) * time.Millisecond
	rebalanceTimeout := 5 * time.Minute
	c.s.groupCoordinator.JoinGroup(apiVersion, groupID, clientID, memberID, protocolType, infos, sessionTimeout, rebalanceTimeout, func(result JoinResult) {
		respBuff := make([]byte, respBuffHeaderSize)
		respBuff = AppendInt16ToBytes(respBuff, int16(result.ErrorCode))
		respBuff = AppendInt32ToBytes(respBuff, int32(result.GenerationID))
		respBuff = AppendStringBytes(respBuff, result.ProtocolName)
		respBuff = AppendStringBytes(respBuff, result.LeaderMemberID)
		respBuff = AppendStringBytes(respBuff, result.MemberID)
		respBuff = AppendInt32ToBytes(respBuff, int32(len(result.Members)))
		for _, member := range result.Members {
			respBuff = AppendStringBytes(respBuff, member.MemberID)
			respBuff = AppendInt32ToBytes(respBuff, int32(len(member.MetaData)))
			respBuff = append(respBuff, member.MetaData...)
		}
		complFunc(respBuff)
	})
}

func (c *connection) handleSyncGroup(apiVersion int16, reqBuff []byte, respBuffHeaderSize int, complFunc func([]byte)) {
	if apiVersion != 0 {
		panic(fmt.Sprintf("unsupported SyncGroup api version %d", apiVersion))
	}
	groupID, off := ReadStringFromBytes(reqBuff)
	generationID := int(ReadInt32FromBytes(reqBuff[off:]))
	off += 4
	memberID, bytesRead := ReadStringFromBytes(reqBuff[off:])
	off += bytesRead
	numAssignments := int(ReadInt32FromBytes(reqBuff[off:]))
	off += 4
	assignments := make([]AssignmentInfo, numAssignments)
	for i := 0; i < numAssignments; i++ {
		assignmentMemberID, bytesRead := ReadStringFromBytes(reqBuff[off:])
		off += bytesRead
		l := int(ReadInt32FromBytes(reqBuff[off:]))
		off += 4
		assignment := make([]byte, l)
		copy(assignment, reqBuff[off:off+l])
		off += l
		assignments[i] = AssignmentInfo{
			MemberID:   assignmentMemberID,
			Assignment: assignment,
		}
	}
	c.s.groupCoordinator.SyncGroup(groupID, memberID, generationID, assignments, func(errorCode int, assignment []byte) {
		respBuff := make([]byte, respBuffHeaderSize)
		respBuff = AppendInt16ToBytes(respBuff, int16(errorCode))
		respBuff = AppendInt32ToBytes(respBuff, int32(len(assignment)))
		respBuff = append(respBuff, assignment...)
		complFunc(respBuff)
	})
}

func (c *connection) handleHeartbeat(apiVersion int16, reqBuff []byte, respBuffHeaderSize int) []byte {
	if apiVersion != 0 {
		panic(fmt.Sprintf("unsupported Heartbeat api version %d", apiVersion))
	}
	groupID, off := ReadStringFromBytes(reqBuff)
	generationID := int(ReadInt32FromBytes(reqBuff[off:]))
	off += 4
	memberID, _ := ReadStringFromBytes(reqBuff[off:])
	c.s.groupCoordinator.HeartbeatGroup(groupID, memberID, generationID)
	respBuff := make([]byte, respBuffHeaderSize)
	respBuff = AppendInt16ToBytes(respBuff, ErrorCodeNone)
	return respBuff
}

func (c *connection) handleLeaveGroup(apiVersion int16, reqBuff []byte, respBuffHeaderSize int) []byte {
	if apiVersion != 0 {
		panic(fmt.Sprintf("unsupported JoinGroup api version %d", apiVersion))
	}
	groupID, off := ReadStringFromBytes(reqBuff)
	memberID, off := ReadStringFromBytes(reqBuff[off:])
	leaveInfos := []MemberLeaveInfo{{MemberID: memberID}}
	errorCode := c.s.groupCoordinator.LeaveGroup(groupID, leaveInfos)
	respBuff := make([]byte, respBuffHeaderSize)
	respBuff = AppendInt16ToBytes(respBuff, errorCode)
	return respBuff
}

func (c *connection) handleListOffsets(apiVersion int16, reqBuff []byte, respBuffHeaderSize int) []byte {
	if apiVersion != 1 {
		panic(fmt.Sprintf("unsupported ListOffsets api version %d", apiVersion))
	}
	// We ignore replicaID
	off := 4
	numTopics := int(ReadInt32FromBytes(reqBuff[off:]))
	off += 4

	topicNames := make([]string, numTopics)
	type partitionOffset struct {
		partitionID  int32
		timestamp    int64
		resOffset    int64
		resTimestamp int64
		errorCode    int16
	}
	partitionOffsets := make([][]partitionOffset, numTopics)
	for i := 0; i < numTopics; i++ {
		topicName, bytesRead := ReadStringFromBytes(reqBuff[off:])
		topicNames[i] = topicName
		off += bytesRead
		numPartitions := int(ReadInt32FromBytes(reqBuff[off:]))
		partitionOffsets[i] = make([]partitionOffset, numPartitions)
		off += 4
		for j := 0; j < numPartitions; j++ {
			partitionIndex := ReadInt32FromBytes(reqBuff[off:])
			off += 4
			timestamp := ReadInt64FromBytes(reqBuff[off:])
			off += 8
			partitionOffsets[i][j].partitionID = partitionIndex
			partitionOffsets[i][j].timestamp = timestamp
		}
	}

	for i, topicName := range topicNames {
		topicInfo, ok := c.s.metadataProvider.GetTopicInfo(topicName)
		if !ok {
			for j := 0; j < len(partitionOffsets[i]); j++ {
				partitionOffsets[i][j].errorCode = ErrorCodeUnknownTopicOrPartition
			}
			continue
		}
		for j, partitionOff := range partitionOffsets[i] {
			timestamp := partitionOff.timestamp
			var resOffset, resTimestamp int64
			var ok bool
			if timestamp == -2 || timestamp == -4 {
				resOffset, resTimestamp, ok = topicInfo.ConsumerInfoProvider.EarliestOffset(int(partitionOff.partitionID))
			} else if timestamp == -1 {
				var err error
				resOffset, resTimestamp, ok, err = topicInfo.ConsumerInfoProvider.LatestOffset(int(partitionOff.partitionID))
				if err != nil {
					log.Errorf("failed to get latest offset %v", err)
					partitionOffsets[i][j].errorCode = ErrorCodeUnknownServerError
					continue
				}
			} else {
				resOffset, resTimestamp, ok = topicInfo.ConsumerInfoProvider.OffsetByTimestamp(types.NewTimestamp(timestamp), int(partitionOff.partitionID))
			}
			if !ok {
				partitionOffsets[i][j].errorCode = ErrorCodeUnknownTopicOrPartition
			} else {
				partitionOffsets[i][j].resOffset = resOffset
				partitionOffsets[i][j].resTimestamp = resTimestamp
			}
		}
	}

	respBuff := make([]byte, respBuffHeaderSize)
	respBuff = AppendInt32ToBytes(respBuff, int32(numTopics))
	for i, topicName := range topicNames {
		respBuff = AppendStringBytes(respBuff, topicName)
		offsets := partitionOffsets[i]
		respBuff = AppendInt32ToBytes(respBuff, int32(len(offsets)))
		for _, partitionOffset := range offsets {
			respBuff = AppendInt32ToBytes(respBuff, partitionOffset.partitionID)
			respBuff = AppendInt16ToBytes(respBuff, partitionOffset.errorCode)
			respBuff = AppendInt64ToBytes(respBuff, partitionOffset.resTimestamp)
			respBuff = AppendInt64ToBytes(respBuff, partitionOffset.resOffset)
		}
	}

	return respBuff
}

func (c *connection) handleOffsetCommit(apiVersion int16, reqBuff []byte, respBuffHeaderSize int) []byte {
	if apiVersion != 2 {
		panic(fmt.Sprintf("unsupported OffsetCommit api version %d", apiVersion))
	}
	groupID, bytesRead := ReadStringFromBytes(reqBuff)
	off := bytesRead
	generationID := ReadInt32FromBytes(reqBuff[off:])
	off += 4
	memberID, bytesRead := ReadStringFromBytes(reqBuff[off:])
	off += bytesRead
	off += 8 // retentionTimeMs

	numTopics := int(ReadInt32FromBytes(reqBuff[off:]))
	off += 4
	topicNames := make([]string, numTopics)
	partitionIDs := make([][]int32, numTopics)
	offsets := make([][]int64, numTopics)
	for i := 0; i < numTopics; i++ {
		topicName, bytesRead := ReadStringFromBytes(reqBuff[off:])
		off += bytesRead
		topicNames[i] = topicName

		numPartitions := ReadInt32FromBytes(reqBuff[off:])
		off += 4

		partitionIDs[i] = make([]int32, numPartitions)
		offsets[i] = make([]int64, numPartitions)
		for j := 0; j < int(numPartitions); j++ {
			partitionIndex := ReadInt32FromBytes(reqBuff[off:])
			off += 4
			partitionIDs[i][j] = partitionIndex

			committedOffset := ReadInt64FromBytes(reqBuff[off:])
			off += 8
			offsets[i][j] = committedOffset
			_, bytesRead := ReadNullableStringFromBytes(reqBuff[off:]) //committedMetadata
			off += bytesRead

		}
	}

	errorCodes := c.s.groupCoordinator.OffsetCommit(groupID, memberID, int(generationID), topicNames, partitionIDs, offsets)

	respBuff := make([]byte, respBuffHeaderSize)
	respBuff = AppendInt32ToBytes(respBuff, int32(numTopics))
	for i, topicName := range topicNames {
		respBuff = AppendStringBytes(respBuff, topicName)
		partitions := partitionIDs[i]
		respBuff = AppendInt32ToBytes(respBuff, int32(len(partitions)))
		for j, partitionID := range partitions {
			respBuff = AppendInt32ToBytes(respBuff, partitionID)
			respBuff = AppendInt16ToBytes(respBuff, errorCodes[i][j])
		}
	}

	return respBuff
}

func (c *connection) handleOffsetFetch(apiVersion int16, reqBuff []byte, respBuffHeaderSize int) []byte {
	if apiVersion != 1 {
		panic(fmt.Sprintf("unsupported OffsetFetch api version %d", apiVersion))
	}

	groupID, bytesRead := ReadStringFromBytes(reqBuff)

	off := bytesRead
	numTopics := int(ReadInt32FromBytes(reqBuff[off:]))
	off += 4
	topicNames := make([]string, numTopics)
	partitionIDs := make([][]int32, numTopics)
	for i := 0; i < numTopics; i++ {
		topicName, bytesRead := ReadStringFromBytes(reqBuff[off:])
		topicNames[i] = topicName
		off += bytesRead
		numPartitions := int(ReadInt32FromBytes(reqBuff[off:]))
		off += 4
		partitions := make([]int32, numPartitions)
		for j := 0; j < numPartitions; j++ {
			partitionIndex := ReadInt32FromBytes(reqBuff[off:])
			partitions[j] = partitionIndex
			off += 4
		}
		partitionIDs[i] = partitions
	}

	respBuff := make([]byte, respBuffHeaderSize)

	offsets, errorCodes, topLevelErrorCode := c.s.groupCoordinator.OffsetFetch(groupID, topicNames, partitionIDs)

	if topLevelErrorCode == ErrorCodeNone {
		respBuff = AppendInt32ToBytes(respBuff, int32(numTopics))
		for i, topicName := range topicNames {
			respBuff = AppendStringBytes(respBuff, topicName)
			partitions := partitionIDs[i]
			respBuff = AppendInt32ToBytes(respBuff, int32(len(partitions)))
			for j, partitionID := range partitions {
				respBuff = AppendInt32ToBytes(respBuff, partitionID)
				errorCode := errorCodes[i][j]
				if errorCode == ErrorCodeNone {
					respBuff = AppendInt64ToBytes(respBuff, offsets[i][j])
				} else {
					respBuff = AppendInt64ToBytes(respBuff, -1)
				}
				respBuff = AppendNullableStringToBytes(respBuff, nil)
				respBuff = AppendInt16ToBytes(respBuff, errorCode)
			}
		}
	} else {
		panic("handle this")
	}
	return respBuff
}

var supportedAPIKeys = map[int16]ApiVersion{
	APIKeyProduce:          {MinVersion: 3, MaxVersion: 3},
	APIKeyFetch:            {MinVersion: 4, MaxVersion: 4},
	APIKeyAPIVersions:      {MinVersion: 0, MaxVersion: 3},
	APIKeySaslAuthenticate: {},
	APIKeyMetadata:         {MinVersion: 3, MaxVersion: 3},
	APIKeyFindCoordinator:  {MinVersion: 0, MaxVersion: 0},
	ApiKeyJoinGroup:        {MinVersion: 0, MaxVersion: 0},
	ApiKeySyncGroup:        {MinVersion: 0, MaxVersion: 0},
	ApiKeyHeartbeat:        {MinVersion: 0, MaxVersion: 0},
	APIKeyListOffsets:      {MinVersion: 1, MaxVersion: 1},
	APIKeyOffsetCommit:     {MinVersion: 2, MaxVersion: 2},
	APIKeyOffsetFetch:      {MinVersion: 1, MaxVersion: 1},
	ApiKeyLeaveGroup:       {MinVersion: 0, MaxVersion: 0},
}

type ApiVersion struct {
	MinVersion int16
	MaxVersion int16
}

func requestHeaderVersion(apiKey int16, apiVersion int16) int16 {
	switch apiKey {
	case APIKeyProduce:
		if apiVersion >= 9 {
			return 2
		} else {
			return 1
		}
	case APIKeyFetch:
		if apiVersion >= 12 {
			return 2
		} else {
			return 1
		}
	case APIKeyListOffsets:
		if apiVersion >= 6 {
			return 2
		} else {
			return 1
		}
	case APIKeyMetadata:
		if apiVersion >= 9 {
			return 2
		} else {
			return 1
		}
	case APIKeyOffsetCommit:
		if apiVersion >= 8 {
			return 2
		} else {
			return 1
		}
	case APIKeyOffsetFetch:
		if apiVersion >= 6 {
			return 2
		} else {
			return 1
		}
	case APIKeyFindCoordinator:
		if apiVersion >= 3 {
			return 2
		} else {
			return 1
		}
	case ApiKeyJoinGroup:
		if apiVersion >= 6 {
			return 2
		} else {
			return 1
		}
	case ApiKeyHeartbeat:
		if apiVersion >= 4 {
			return 2
		} else {
			return 1
		}
	case ApiKeyLeaveGroup:
		if apiVersion >= 4 {
			return 2
		} else {
			return 1
		}
	case ApiKeySyncGroup:
		if apiVersion >= 4 {
			return 2
		} else {
			return 1
		}
	case APIKeySaslHandshake:
		return 1
	case APIKeyAPIVersions:
		if apiVersion >= 3 {
			return 2
		} else {
			return 1
		}
	case APIKeySaslAuthenticate: // SaslAuthenticate
		if apiVersion >= 2 {
			return 2
		} else {
			return 1
		}
	default:
		panic(fmt.Sprintf("unexpected api key %d", apiKey))
	}
}

func responseHeaderVersion(apiKey int16, apiVersion int16) int16 {
	switch apiKey {
	case APIKeyProduce:
		if apiVersion >= 9 {
			return 1
		} else {
			return 0
		}
	case APIKeyFetch:
		if apiVersion >= 12 {
			return 1
		} else {
			return 0
		}
	case APIKeyListOffsets:
		if apiVersion >= 6 {
			return 1
		} else {
			return 0
		}
	case APIKeyMetadata:
		if apiVersion >= 9 {
			return 1
		} else {
			return 0
		}
	case APIKeyOffsetCommit:
		if apiVersion >= 8 {
			return 1
		} else {
			return 0
		}
	case APIKeyOffsetFetch:
		if apiVersion >= 6 {
			return 1
		} else {
			return 0
		}
	case APIKeyFindCoordinator:
		if apiVersion >= 3 {
			return 1
		} else {
			return 0
		}
	case ApiKeyJoinGroup:
		if apiVersion >= 6 {
			return 1
		} else {
			return 0
		}
	case ApiKeyHeartbeat:
		if apiVersion >= 4 {
			return 1
		} else {
			return 0
		}
	case ApiKeyLeaveGroup:
		if apiVersion >= 4 {
			return 1
		} else {
			return 0
		}
	case ApiKeySyncGroup:
		if apiVersion >= 4 {
			return 1
		} else {
			return 0
		}
	case APIKeySaslHandshake:
		return 0
	case APIKeyAPIVersions:
		// ApiVersionsResponse always includes a v0 header.
		// See KIP-511 for details.
		return 0
	case APIKeySaslAuthenticate:
		if apiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	default:
		panic(fmt.Sprintf("unexpected api key %d", apiKey))
	}
}
