package kafkaserver

import (
	"github.com/spirit-labs/tektite/clustmgr"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/opers"
	"github.com/spirit-labs/tektite/proc"
	"net"
	"strconv"
	"sync"
)

type metaDataProvider struct {
	lock        sync.RWMutex
	nodeID      int
	brokerInfos []BrokerInfo
	topicInfos  map[string]*TopicInfo
	gotAllInfos bool
	procMgr     proc.Manager
	streamMgr   opers.StreamManager
}

func NewMetaDataProvider(cfg *conf.Config, procMgr proc.Manager, streamMgr opers.StreamManager) (MetadataProvider, error) {
	brokerInfos := make([]BrokerInfo, len(cfg.KafkaServerAddresses))
	for nodeID, address := range cfg.KafkaServerAddresses {
		host, sPort, err := net.SplitHostPort(address)
		var port int
		if err == nil {
			port, err = strconv.Atoi(sPort)
		}
		if err != nil {
			return nil, errors.NewTektiteErrorf(errors.InvalidConfiguration, "invalid KafkaListenAddress: %s", address)
		}
		brokerInfos[nodeID] = BrokerInfo{
			NodeID: nodeID,
			Host:   host,
			Port:   port,
		}
	}
	provider := &metaDataProvider{
		nodeID:      cfg.NodeID,
		brokerInfos: brokerInfos,
		topicInfos:  map[string]*TopicInfo{},
		procMgr:     procMgr,
		streamMgr:   streamMgr,
	}
	procMgr.RegisterStateHandler(provider.ClusterStateHandler)
	streamMgr.RegisterChangeListener(provider.Invalidate)

	return provider, nil
}

func (m *metaDataProvider) ControllerNodeID() int {
	// There's no controller in tektite
	return 0
}

func (m *metaDataProvider) BrokerInfos() []BrokerInfo {
	return m.brokerInfos
}

func (m *metaDataProvider) GetTopicInfo(topicName string) (TopicInfo, bool) {
	m.lock.RLock()
	runlocked := false
	defer func() {
		if !runlocked {
			m.lock.RUnlock()
		}
	}()
	topicInfo, ok := m.topicInfos[topicName]
	if ok {
		return *topicInfo, true
	}

	m.lock.RUnlock()
	runlocked = true
	// Upgrade lock
	m.lock.Lock()
	defer m.lock.Unlock()
	topicInfo, ok = m.topicInfos[topicName]
	if ok {
		return *topicInfo, true
	}
	kafkaEndpoint := m.streamMgr.GetKafkaEndpoint(topicName)
	if kafkaEndpoint == nil {
		return TopicInfo{}, false
	}

	topicInfo = m.createTopicInfo(kafkaEndpoint)
	m.topicInfos[topicName] = topicInfo

	return *topicInfo, true
}

func (m *metaDataProvider) GetAllTopics() []*TopicInfo {
	m.lock.RLock()
	if !m.gotAllInfos {
		m.lock.RUnlock()
		m.lock.Lock()
		defer m.lock.Unlock()
		var topicInfos []*TopicInfo
		endpoints := m.streamMgr.GetAllKafkaEndpoints()
		for _, endpoint := range endpoints {
			topicInfo := m.createTopicInfo(endpoint)
			m.topicInfos[topicInfo.Name] = topicInfo
			topicInfos = append(topicInfos, topicInfo)
		}
		m.gotAllInfos = true
		return topicInfos
	}
	var topicInfos []*TopicInfo
	for _, topicInfo := range topicInfos {
		topicInfos = append(topicInfos, topicInfo)
	}
	m.lock.RUnlock()
	return topicInfos
}

func (m *metaDataProvider) createTopicInfo(kafkaEndpoint *opers.KafkaEndpointInfo) *TopicInfo {
	var partitionInfos []PartitionInfo
	var mapping map[int]int
	if kafkaEndpoint.InEndpoint != nil {
		mapping = kafkaEndpoint.InEndpoint.GetPartitionProcessorMapping()
	} else {
		mapping = kafkaEndpoint.OutEndpoint.GetPartitionProcessorMapping()
	}
	for partitionID, procID := range mapping {
		groupState, ok := m.procMgr.GetGroupState(procID)
		if ok {
			leaderNodeID := -1
			var replicaNodeIDs []int
			for _, groupNode := range groupState.GroupNodes {
				if groupNode.Leader {
					leaderNodeID = groupNode.NodeID
				}
				replicaNodeIDs = append(replicaNodeIDs, groupNode.NodeID)
			}
			if leaderNodeID != -1 {
				partitionInfos = append(partitionInfos, PartitionInfo{
					ID:             partitionID,
					LeaderNodeID:   leaderNodeID,
					ReplicaNodeIDs: replicaNodeIDs,
				})
			}
		}
	}
	// We can only cache the record batch if there is no processing done in the stream, as the processing can change
	// the bytes. So we check whether the stream is just a kafka in followed by a kafka out.
	canCache := kafkaEndpoint.InEndpoint != nil && kafkaEndpoint.OutEndpoint != nil &&
		kafkaEndpoint.InEndpoint.BaseOperator.GetDownStreamOperators()[0] == kafkaEndpoint.OutEndpoint
	topicInfo := &TopicInfo{
		Name:                 kafkaEndpoint.Name,
		ProduceEnabled:       kafkaEndpoint.InEndpoint != nil,
		ConsumeEnabled:       kafkaEndpoint.OutEndpoint != nil,
		CanCache:             canCache,
		ProduceInfoProvider:  kafkaEndpoint.InEndpoint,
		ConsumerInfoProvider: kafkaEndpoint.OutEndpoint,
		Partitions:           partitionInfos,
	}
	return topicInfo
}

func (m *metaDataProvider) ClusterStateHandler(_ clustmgr.ClusterState) error {
	m.Invalidate("", false)
	return nil
}

func (m *metaDataProvider) Invalidate(_ string, _ bool) {
	m.lock.Lock()
	defer m.lock.Unlock()
	// Cluster state or stream state has changed - invalidate
	m.topicInfos = map[string]*TopicInfo{}
	m.gotAllInfos = false
}
