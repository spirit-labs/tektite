package agent

import (
	"github.com/spirit-labs/tektite/acls"
	auth "github.com/spirit-labs/tektite/auth2"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/control"
	"github.com/spirit-labs/tektite/kafkaencoding"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/topicmeta"
	"net"
	"strconv"
	"strings"
)

func (a *Agent) HandleDescribeClusterRequest(completionFunc func(resp *kafkaprotocol.DescribeClusterResponse) error) error {
	resp := &kafkaprotocol.DescribeClusterResponse{
		EndpointType: 1,
		ClusterId:    common.StrPtr(a.cfg.ClusterName),
		ControllerId: a.MemberID(),
		Brokers:      nil,
	}
	clusterMetadata := a.controller.GetClusterMeta()
	resp.Brokers = make([]kafkaprotocol.DescribeClusterResponseDescribeClusterBroker, len(clusterMetadata))
	for i, agentMeta := range clusterMetadata {
		resp.Brokers[i].BrokerId = agentMeta.ID
		host, port, err := addressToHostPort(agentMeta.KafkaAddress)
		if err != nil {
			return err
		}
		resp.Brokers[i].Host = common.StrPtr(host)
		resp.Brokers[i].Port = port
	}
	return completionFunc(resp)
}

func (a *Agent) HandleMetadataRequest(authContext *auth.Context, hdr *kafkaprotocol.RequestHeader,
	req *kafkaprotocol.MetadataRequest) (*kafkaprotocol.MetadataResponse, error) {
	resp := &kafkaprotocol.MetadataResponse{}
	resp.Topics = make([]kafkaprotocol.MetadataResponseMetadataResponseTopic, len(req.Topics))
	for i, topicData := range req.Topics {
		resp.Topics[i].Name = topicData.Name
	}
	err := a.handleMetadataRequest(authContext, hdr, req, resp)
	if err != nil {
		log.Warnf("failed to handle metadata request: %v", err)
		if len(resp.Topics) > 0 {
			// We can fill in error on topics
			errCode := kafkaencoding.ErrorCodeForError(err, kafkaprotocol.ErrorCodeUnknownTopicOrPartition)
			for i := range resp.Topics {
				resp.Topics[i].ErrorCode = errCode
			}
		} else {
			// The request had no topics - and the error code is on the topic in the response, but we need
			// to send an error back, so we just send it back on an "unknown" topic
			log.Warnf("failed to handle metadata request: %v", err)
			resp.Topics = make([]kafkaprotocol.MetadataResponseMetadataResponseTopic, 1)
			resp.Topics[0].Name = common.StrPtr("unknown")
			resp.Topics[0].ErrorCode = int16(kafkaprotocol.ErrorCodeUnknownTopicOrPartition)
			return nil, err
		}
	}
	return resp, nil
}

const (
	tekAzPrefix    = "tek_az="
	wsAzPrefix     = "ws_az="
	lenTekAzPrefix = len(tekAzPrefix)
	lenWsAzPrefix  = len(wsAzPrefix)
)

func getAZFromClientID(clientID string) string {
	ind := strings.LastIndex(clientID, tekAzPrefix) + lenTekAzPrefix
	if ind == lenTekAzPrefix-1 {
		// We support compat with WS too
		ind = strings.LastIndex(clientID, wsAzPrefix) + lenWsAzPrefix
		if ind == lenWsAzPrefix-1 {
			return ""
		}
	}
	return clientID[ind:]
}

func getAgentsInAz(az string, agents []control.AgentMeta) []control.AgentMeta {
	var agentsInSameAz []control.AgentMeta
	for _, meta := range agents {
		if meta.Location == az {
			agentsInSameAz = append(agentsInSameAz, meta)
		}
	}
	return agentsInSameAz
}

func (a *Agent) getAgentsInSameAz(hdr *kafkaprotocol.RequestHeader) ([]control.AgentMeta, error) {
	clientID := common.SafeDerefStringPtr(hdr.ClientId)
	az := getAZFromClientID(clientID)
	if az == "" {
		log.Warnf("Kafka client connecting with a ClientID (\"%s\") which does not contain availability zone. This means there may be unwanted cross AZ traffic. Please append tek_az=<availability zone> to the client id.",
			clientID)
	}

	clusterMetadata := a.controller.GetClusterMeta()
	if len(clusterMetadata) == 0 {
		// Send back an unavailable so the client retries
		return nil, common.NewTektiteErrorf(common.Unavailable, "no cluster metadata available")
	}
	// Find agents in same AZ
	agents := getAgentsInAz(az, clusterMetadata)
	if len(agents) == 0 {
		// nothing for client requested AZ - choose first AZ.
		azOther := clusterMetadata[0].Location
		log.Warnf("There are no agents available for request availability zone: %s - availability zone %s will be chosen instead", az, azOther)
		agents = getAgentsInAz(azOther, clusterMetadata)
	}
	return agents, nil
}

func (a *Agent) handleMetadataRequest(authContext *auth.Context, hdr *kafkaprotocol.RequestHeader, req *kafkaprotocol.MetadataRequest, resp *kafkaprotocol.MetadataResponse) error {
	agents, err := a.getAgentsInSameAz(hdr)
	if err != nil {
		return err
	}
	resp.Brokers = make([]kafkaprotocol.MetadataResponseMetadataResponseBroker, len(agents))
	for i, agent := range agents {
		host, port, err := addressToHostPort(agent.KafkaAddress)
		if err != nil {
			return err
		}
		resp.Brokers[i] = kafkaprotocol.MetadataResponseMetadataResponseBroker{
			Host:   &host,
			Port:   port,
			NodeId: agent.ID,
		}
	}
	client, err := a.controlClientCache.GetClient()
	if err != nil {
		return err
	}
	// In version 1 and higher, an empty array indicates "request metadata for no topics," and a null array is used to
	// indicate "request metadata for all topics."
	if req.Topics == nil {
		// request for all topics
		topicInfos, err := client.GetAllTopicInfos()
		if err != nil {
			return err
		}
		resp.Topics = make([]kafkaprotocol.MetadataResponseMetadataResponseTopic, 0, len(topicInfos))
		for _, topicInfo := range topicInfos {
			authorised := true
			if authContext != nil {
				// Only return topics user is authorised to see
				authorised, err = authContext.Authorize(acls.ResourceTypeTopic, topicInfo.Name, acls.OperationDescribe)
				if err != nil {
					return err
				}
			}
			if authorised {
				top, err := a.populateTopicMetadata(&topicInfo, agents)
				if err != nil {
					return err
				}
				resp.Topics = append(resp.Topics, *top)
			}
		}
	} else {
		for i, top := range req.Topics {
			topicName := common.SafeDerefStringPtr(top.Name)
			topicInfo, _, exists, err := client.GetTopicInfo(topicName)
			if err != nil {
				return err
			}
			if !exists {
				if req.AllowAutoTopicCreation && a.cfg.EnableTopicAutoCreate && hdr.RequestApiVersion >= 4 {
					// auto create topic
					if authContext != nil {
						authorised, err := authContext.Authorize(acls.ResourceTypeTopic, topicName, acls.OperationCreate)
						if err != nil {
							return err
						}
						if !authorised {
							resp.Topics[i].ErrorCode = kafkaprotocol.ErrorCodeTopicAuthorizationFailed
							continue
						}
					}
					if err := client.CreateOrUpdateTopic(topicmeta.TopicInfo{
						Name:               topicName,
						PartitionCount:     a.cfg.DefaultPartitionCount,
						RetentionTime:      a.cfg.DefaultTopicRetentionTime,
						UseServerTimestamp: a.cfg.DefaultUseServerTimestamp,
					}, true); err != nil {
						return err
					}
					topicInfo, _, exists, err = client.GetTopicInfo(topicName)
					if err != nil {
						return err
					}
					if !exists {
						log.Warnf("topic does not exist after auto creation!")
						resp.Topics[i].ErrorCode = kafkaprotocol.ErrorCodeUnknownTopicOrPartition
						continue
					}
				} else {
					resp.Topics[i].ErrorCode = kafkaprotocol.ErrorCodeUnknownTopicOrPartition
					continue
				}
			}
			authorised := true
			if authContext != nil {
				authorised, err = authContext.Authorize(acls.ResourceTypeTopic, topicName, acls.OperationDescribe)
				if err != nil {
					return err
				}
				if !authorised {
					resp.Topics[i].ErrorCode = kafkaprotocol.ErrorCodeTopicAuthorizationFailed
				}
			}
			if authorised {
				top, err := a.populateTopicMetadata(&topicInfo, agents)
				if err != nil {
					return err
				}
				resp.Topics[i] = *top
			}
		}
	}
	return err
}

func addressToHostPort(address string) (string, int32, error) {
	host, sPort, err := net.SplitHostPort(address)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(sPort)
	if err != nil {
		return "", 0, err
	}
	return host, int32(port), nil
}

func (a *Agent) IsLeader(topicID int, partitionID int) (bool, error) {
	partHash, err := a.partitionHashes.GetPartitionHash(topicID, partitionID)
	if err != nil {
		return false, err
	}
	agentsSameAz := a.controller.GetClusterMetaThisAz()
	index := common.CalcMemberForHash(partHash, len(agentsSameAz))
	leader := agentsSameAz[index]
	return leader.ID == a.MemberID(), nil
}

func (a *Agent) populateTopicMetadata(topicInfo *topicmeta.TopicInfo, agents []control.AgentMeta) (*kafkaprotocol.MetadataResponseMetadataResponseTopic, error) {
	var topic kafkaprotocol.MetadataResponseMetadataResponseTopic
	topic.Name = &topicInfo.Name
	topic.Partitions = make([]kafkaprotocol.MetadataResponseMetadataResponsePartition, topicInfo.PartitionCount)
	for i := 0; i < topicInfo.PartitionCount; i++ {
		var part kafkaprotocol.MetadataResponseMetadataResponsePartition
		part.PartitionIndex = int32(i)
		partHash, err := a.partitionHashes.GetPartitionHash(topicInfo.ID, i)
		if err != nil {
			return nil, err
		}
		// choose leader
		index := common.CalcMemberForHash(partHash, len(agents))
		leader := agents[index]
		part.LeaderId = leader.ID
		// We don't fill in the replica nodes -if a produce returns NotLeaderOrFollower then the client will request
		// metadata again and get the correct leader
		topic.Partitions[i] = part
	}
	return &topic, nil
}
