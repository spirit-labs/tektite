package agent

import (
	"errors"
	"fmt"
	"github.com/spirit-labs/tektite/apiclient"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/parthash"
	"github.com/spirit-labs/tektite/topicmeta"
	"github.com/spirit-labs/tektite/transport"
	"github.com/stretchr/testify/require"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func TestMetadataAllTopicsNonMatchingClientID(t *testing.T) {

	cfg := NewConf()
	numAgents := 5
	agents, tearDown := setupAgents(t, cfg, numAgents, func(i int) string {
		return "az1"
	})
	defer tearDown(t)
	numTopics := 10
	setupNumTopics(t, numTopics, agents[0])

	// Should return all
	resp := sendMetadataRequest(t, agents[0], &kafkaprotocol.MetadataRequest{}, "")
	verifyBrokers(t, agents, resp)
	verifyTopics(t, numTopics, agents, resp)
}

func TestMetadataAllTopicsMatchingSingleAZ(t *testing.T) {

	cfg := NewConf()
	numAgents := 5
	agents, tearDown := setupAgents(t, cfg, numAgents, func(i int) string {
		return "az1"
	})
	defer tearDown(t)
	numTopics := 10
	setupNumTopics(t, numTopics, agents[0])

	// Should return all
	resp := sendMetadataRequest(t, agents[0], &kafkaprotocol.MetadataRequest{}, "az1")
	verifyBrokers(t, agents, resp)
	verifyTopics(t, numTopics, agents, resp)
}

func TestMetadataOnlySameAZ(t *testing.T) {

	cfg := NewConf()
	numAgents := 10
	agents, tearDown := setupAgents(t, cfg, numAgents, func(i int) string {
		return fmt.Sprintf("az-%t", i%2 == 0)
	})
	defer tearDown(t)
	numTopics := 10
	setupNumTopics(t, numTopics, agents[0])

	var expectedAgents1 []*Agent
	var expectedAgents2 []*Agent
	for i, agent := range agents {
		if i%2 == 0 {
			expectedAgents1 = append(expectedAgents1, agent)
		} else {
			expectedAgents2 = append(expectedAgents2, agent)
		}
	}

	// Send requests with a client id which doesn't match any AZs - it should choose the first AZ
	resp := sendMetadataRequest(t, agents[0], &kafkaprotocol.MetadataRequest{}, "foo")
	verifyBrokers(t, expectedAgents1, resp)
	verifyTopics(t, numTopics, expectedAgents1, resp)

	resp = sendMetadataRequest(t, agents[0], &kafkaprotocol.MetadataRequest{}, "tek_az=foo")
	verifyBrokers(t, expectedAgents1, resp)
	verifyTopics(t, numTopics, expectedAgents1, resp)

	resp = sendMetadataRequest(t, agents[0], &kafkaprotocol.MetadataRequest{}, "ws_az=foo")
	verifyBrokers(t, expectedAgents1, resp)
	verifyTopics(t, numTopics, expectedAgents1, resp)

	// Send requests matching the first AZ
	resp = sendMetadataRequest(t, agents[0], &kafkaprotocol.MetadataRequest{}, "tek_az=az-true")
	verifyBrokers(t, expectedAgents1, resp)
	verifyTopics(t, numTopics, expectedAgents1, resp)

	resp = sendMetadataRequest(t, agents[0], &kafkaprotocol.MetadataRequest{}, "ws_az=az-true")
	verifyBrokers(t, expectedAgents1, resp)
	verifyTopics(t, numTopics, expectedAgents1, resp)

	// Send requests matching the second AZ
	resp = sendMetadataRequest(t, agents[0], &kafkaprotocol.MetadataRequest{}, "tek_az=az-false")
	verifyBrokers(t, expectedAgents2, resp)
	verifyTopics(t, numTopics, expectedAgents2, resp)

	resp = sendMetadataRequest(t, agents[0], &kafkaprotocol.MetadataRequest{}, "ws_az=az-false")
	verifyBrokers(t, expectedAgents2, resp)
	verifyTopics(t, numTopics, expectedAgents2, resp)
}

func TestMetadataNoTopics(t *testing.T) {

	cfg := NewConf()
	numAgents := 5
	agents, tearDown := setupAgents(t, cfg, numAgents, func(i int) string {
		return "az-1"
	})
	defer tearDown(t)
	setupNumTopics(t, 10, agents[0])

	req := &kafkaprotocol.MetadataRequest{}
	// empty array means return no topics
	req.Topics = []kafkaprotocol.MetadataRequestMetadataRequestTopic{}
	resp := sendMetadataRequest(t, agents[0], req, "az-1")

	verifyBrokers(t, agents, resp)

	// Should be no topics returned
	require.Equal(t, 0, len(resp.Topics))
}

func TestMetadataSingleTopic(t *testing.T) {

	cfg := NewConf()
	numAgents := 5
	agents, tearDown := setupAgents(t, cfg, numAgents, func(i int) string {
		return "az1"
	})
	defer tearDown(t)
	numTopics := 10
	setupNumTopics(t, numTopics, agents[0])

	for i := 0; i < numTopics; i++ {
		req := &kafkaprotocol.MetadataRequest{}
		topicName := fmt.Sprintf("topic-%05d", i)
		req.Topics = []kafkaprotocol.MetadataRequestMetadataRequestTopic{
			{
				Name: common.StrPtr(topicName),
			},
		}
		resp := sendMetadataRequest(t, agents[0], req, "")
		verifyBrokers(t, agents, resp)
		require.Equal(t, 1, len(resp.Topics))
		verifySingleTopic(t, topicName, topicmeta.TopicIDSequenceBase+i, 1+i*2, agents, resp.Topics[0])
	}
}

func TestMetadataSingleTopicUnknown(t *testing.T) {

	cfg := NewConf()
	numAgents := 5
	agents, tearDown := setupAgents(t, cfg, numAgents, func(i int) string {
		return "az1"
	})
	defer tearDown(t)
	setupNumTopics(t, 10, agents[0])
	req := &kafkaprotocol.MetadataRequest{}
	req.Topics = []kafkaprotocol.MetadataRequestMetadataRequestTopic{
		{
			Name: common.StrPtr("unknown"),
		},
	}
	resp := sendMetadataRequest(t, agents[0], req, "")
	verifyBrokers(t, agents, resp)
	require.Equal(t, 1, len(resp.Topics))
	require.Equal(t, kafkaprotocol.ErrorCodeUnknownTopicOrPartition, int(resp.Topics[0].ErrorCode))
}

func TestMetadataControllerUnavailable(t *testing.T) {

	cfg := NewConf()
	numAgents := 5
	agents, tearDown := setupAgents(t, cfg, numAgents, func(i int) string {
		return "az1"
	})
	defer tearDown(t)
	setupNumTopics(t, 10, agents[0])
	req := &kafkaprotocol.MetadataRequest{}
	req.Topics = []kafkaprotocol.MetadataRequestMetadataRequestTopic{
		{
			Name: common.StrPtr(fmt.Sprintf("topic-%05d", 0)),
		},
	}
	unavail := common.NewTektiteErrorf(common.Unavailable, "injected unavailable")
	agents[0].controlClientCache.SetInjectedError(unavail)
	resp := sendMetadataRequest(t, agents[0], req, "")
	require.Equal(t, 1, len(resp.Topics))
	require.Equal(t, kafkaprotocol.ErrorCodeUnknownTopicOrPartition, int(resp.Topics[0].ErrorCode))
}

func TestMetadataControllerUnexpectedError(t *testing.T) {

	cfg := NewConf()
	numAgents := 5
	agents, tearDown := setupAgents(t, cfg, numAgents, func(i int) string {
		return "az1"
	})
	defer tearDown(t)
	setupNumTopics(t, 10, agents[0])
	req := &kafkaprotocol.MetadataRequest{}
	req.Topics = []kafkaprotocol.MetadataRequestMetadataRequestTopic{
		{
			Name: common.StrPtr(fmt.Sprintf("topic-%05d", 0)),
		},
	}
	err := errors.New("injected error")
	agents[0].controlClientCache.SetInjectedError(err)
	resp := sendMetadataRequest(t, agents[0], req, "")
	require.Equal(t, 1, len(resp.Topics))
	require.Equal(t, kafkaprotocol.ErrorCodeUnknownServerError, int(resp.Topics[0].ErrorCode))
}

func TestGetAzFromClientID(t *testing.T) {
	require.Equal(t, "", getAZFromClientID("foo"))
	require.Equal(t, "", getAZFromClientID(""))
	require.Equal(t, "az-1", getAZFromClientID("tek_az=az-1"))
	require.Equal(t, "az-2", getAZFromClientID("ws_az=az-2"))
}

func setupAgents(t *testing.T, cfg Conf, numAgents int, azPicker func(int) string) ([]*Agent, func(t *testing.T)) {
	objStore := dev.NewInMemStore(0)
	inMemMemberships := NewInMemClusterMemberships()
	inMemMemberships.Start()
	localTransports := transport.NewLocalTransports()
	var agents []*Agent
	var tearDowns []func(t *testing.T)
	for i := 0; i < numAgents; i++ {
		cfgCopy := cfg
		az := azPicker(i)
		cfgCopy.FetchCacheConf.AzInfo = az
		agent, tearDown := setupAgentWithArgs(t, cfgCopy, objStore, inMemMemberships, localTransports)
		agents = append(agents, agent)
		tearDowns = append(tearDowns, tearDown)
	}
	return agents, func(t *testing.T) {
		stopWg := &sync.WaitGroup{}
		stopWg.Add(len(tearDowns))
		for _, tearDown := range tearDowns {
			td := tearDown
			go func() {
				td(t)
				stopWg.Done()
			}()
		}
		stopWg.Wait()
	}
}

func setupNumTopics(t *testing.T, numTopics int, agent *Agent) {
	var topicInfos []topicmeta.TopicInfo
	for i := 0; i < numTopics; i++ {
		topicInfos = append(topicInfos, topicmeta.TopicInfo{
			ID:             topicmeta.TopicIDSequenceBase + i,
			Name:           fmt.Sprintf("topic-%05d", i),
			PartitionCount: 1 + i*2,
		})
	}
	setupTopics(t, agent, topicInfos)
}

func sendMetadataRequest(t *testing.T, agent *Agent, req *kafkaprotocol.MetadataRequest, clientID string) *kafkaprotocol.MetadataResponse {
	cl, err := apiclient.NewKafkaApiClientWithClientID(clientID)
	require.NoError(t, err)
	conn, err := cl.NewConnection(agent.Conf().KafkaListenerConfig.Address)
	require.NoError(t, err)
	defer func() {
		err := conn.Close()
		require.NoError(t, err)
	}()
	resp := &kafkaprotocol.MetadataResponse{}
	r, err := conn.SendRequest(req, kafkaprotocol.APIKeyMetadata, 1, resp)
	require.NoError(t, err)
	return r.(*kafkaprotocol.MetadataResponse)
}

func verifyTopics(t *testing.T, numTopics int, agents []*Agent, resp *kafkaprotocol.MetadataResponse) {
	// sort the returned topics
	topicsCp := make([]kafkaprotocol.MetadataResponseMetadataResponseTopic, len(resp.Topics))
	copy(topicsCp, resp.Topics)
	sort.SliceStable(topicsCp, func(i, j int) bool {
		return strings.Compare(common.SafeDerefStringPtr(topicsCp[i].Name), common.SafeDerefStringPtr(topicsCp[j].Name)) < 0
	})
	require.Equal(t, numTopics, len(topicsCp))
	for i, topic := range topicsCp {
		require.Equal(t, kafkaprotocol.ErrorCodeNone, int(topic.ErrorCode))
		topicName := fmt.Sprintf("topic-%05d", i)
		verifySingleTopic(t, topicName, topicmeta.TopicIDSequenceBase+i, 1+i*2, agents, topic)
	}
}

func verifySingleTopic(t *testing.T, topicName string, topicID int, partitionCount int, agents []*Agent, topic kafkaprotocol.MetadataResponseMetadataResponseTopic) {
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(topic.ErrorCode))
	require.Equal(t, topicName, common.SafeDerefStringPtr(topic.Name))
	require.Equal(t, partitionCount, len(topic.Partitions))
	for j, partition := range topic.Partitions {
		require.Equal(t, j, int(partition.PartitionIndex))
		partHash, err := parthash.CreatePartitionHash(topicID, int(partition.PartitionIndex))
		require.NoError(t, err)
		index := common.CalcMemberForHash(partHash, len(agents))
		leader := agents[index]
		require.Equal(t, partition.LeaderId, leader.MemberID())
	}
}

func verifyBrokers(t *testing.T, agents []*Agent, resp *kafkaprotocol.MetadataResponse) {
	require.Equal(t, len(agents), len(resp.Brokers))
	// sort them
	sort.SliceStable(agents, func(i, j int) bool {
		return agents[i].MemberID() < agents[j].MemberID()
	})
	brokersCp := make([]kafkaprotocol.MetadataResponseMetadataResponseBroker, len(resp.Brokers))
	copy(brokersCp, resp.Brokers)
	sort.SliceStable(brokersCp, func(i, j int) bool {
		return brokersCp[i].NodeId < brokersCp[j].NodeId
	})
	for i, agent := range agents {
		broker := brokersCp[i]
		require.Equal(t, agent.MemberID(), broker.NodeId)
		address, port := splitHostPort(t, agent.cfg.KafkaListenerConfig.Address)
		require.Equal(t, address, common.SafeDerefStringPtr(broker.Host))
		require.Equal(t, port, int(broker.Port))
	}
}

func splitHostPort(t *testing.T, address string) (string, int) {
	host, sPort, err := net.SplitHostPort(address)
	require.NoError(t, err)
	port, err := strconv.Atoi(sPort)
	require.NoError(t, err)
	return host, port
}

func TestDescribeCluster(t *testing.T) {
	cfg := NewConf()
	numAgents := 5
	agents, tearDown := setupAgents(t, cfg, numAgents, func(i int) string {
		return "az1"
	})
	defer tearDown(t)
	numTopics := 10
	setupNumTopics(t, numTopics, agents[0])

	cl, err := apiclient.NewKafkaApiClient()
	require.NoError(t, err)
	conn, err := cl.NewConnection(agents[0].Conf().KafkaListenerConfig.Address)
	require.NoError(t, err)
	defer func() {
		err := conn.Close()
		require.NoError(t, err)
	}()

	req := &kafkaprotocol.DescribeClusterRequest{}

	resp := &kafkaprotocol.DescribeClusterResponse{}
	r, err := conn.SendRequest(req, kafkaprotocol.ApiKeyDescribeCluster, 0, resp)
	require.NoError(t, err)
	resp, ok := r.(*kafkaprotocol.DescribeClusterResponse)
	require.True(t, ok)

	require.Equal(t, len(agents), len(resp.Brokers))
	sort.SliceStable(agents, func(i, j int) bool {
		return agents[i].MemberID() < agents[j].MemberID()
	})
	brokersCp := make([]kafkaprotocol.DescribeClusterResponseDescribeClusterBroker, len(resp.Brokers))
	copy(brokersCp, resp.Brokers)
	sort.SliceStable(brokersCp, func(i, j int) bool {
		return brokersCp[i].BrokerId < brokersCp[j].BrokerId
	})
	for i, agent := range agents {
		broker := brokersCp[i]
		require.Equal(t, agent.MemberID(), broker.BrokerId)
		address, port := splitHostPort(t, agent.cfg.KafkaListenerConfig.Address)
		require.Equal(t, address, common.SafeDerefStringPtr(broker.Host))
		require.Equal(t, port, int(broker.Port))
	}
}
