package agent

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/acls"
	"github.com/spirit-labs/tektite/apiclient"
	"github.com/spirit-labs/tektite/auth"
	auth2 "github.com/spirit-labs/tektite/auth2"
	"github.com/spirit-labs/tektite/client"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	"github.com/spirit-labs/tektite/kafkaserver2"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/stretchr/testify/require"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestAuthorisation(t *testing.T) {
	cfg := NewConf()
	cfg.KafkaListenerConfig.TLSConfig = conf.TlsConf{
		Enabled:              true,
		ServerPrivateKeyFile: serverKeyPath,
		ServerCertFile:       serverCertPath,
	}
	cfg.AuthType = kafkaserver2.AuthenticationTypeSaslPlain
	authTimeout := 1 * time.Millisecond
	cfg.UserAuthCacheTimeout = authTimeout
	agents, tearDown := setupAgents(t, cfg, 1, func(i int) string {
		return "az1"
	})
	defer tearDown(t)
	agent := agents[0]
	topicName := makeRandomTopic(t, agent)
	createAdminUser(t, agent)
	cl, err := apiclient.NewKafkaApiClientWithTls(&client.TLSConfig{
		TrustedCertsPath: serverCertPath,
	})
	require.NoError(t, err)
	conn, err := cl.NewConnection(agent.cfg.KafkaListenerConfig.Address)
	require.NoError(t, err)
	defer func() {
		err := conn.Close()
		require.NoError(t, err)
	}()
	authenticateConnection(t, conn)

	for _, tc := range authorisationTestCases {
		t.Run(tc.name, func(t *testing.T) {
			deleteAllAcls(t, agent)
			tc.testFunc(t, topicName, conn, agent, authTimeout)
		})
	}
}

func deleteAllAcls(t *testing.T, agent *Agent) {
	cl, err := agent.controller.Client()
	require.NoError(t, err)
	err = cl.DeleteAcls(acls.ResourceTypeAny, "", acls.ResourcePatternTypeAny, "", "",
		acls.OperationAny, acls.PermissionAny)
	require.NoError(t, err)
}

var authorisationTestCases = []authTestCase{
	{name: "testTopicReadPermission", testFunc: testTopicReadPermission},
	{name: "testTopicWritePermission", testFunc: testTopicWritePermission},
	{name: "testTopicCreatePermission", testFunc: testTopicCreatePermission},
	{name: "testTopicDeletePermission", testFunc: testTopicDeletePermission},
	{name: "testTopicAlterPermission", testFunc: testTopicAlterPermission},
	{name: "testTopicDescribePermission", testFunc: testTopicDescribePermission},

	{name: "testClusterCreatePermission", testFunc: testClusterCreatePermission},
	{name: "testClusterAlterPermission", testFunc: testClusterAlterPermission},
	{name: "testClusterDescribePermission", testFunc: testClusterDescribePermission},

	{name: "testGroupDeletePermission", testFunc: testGroupDeletePermission},
	{name: "testGroupDescribePermission", testFunc: testGroupDescribePermission},
	{name: "testGroupReadPermission", testFunc: testGroupReadPermission},

	{name: "testTransactionalIdDescribePermission", testFunc: testTransactionalIdDescribePermission},
	{name: "testTransactionalIdWritePermission", testFunc: testTransactionalIdWritePermission},
}

type authTestCase struct {
	name     string
	testFunc func(t *testing.T, topicName string, conn *apiclient.KafkaApiConnection, agent *Agent, authTimeout time.Duration)
}

func testTopicReadPermission(t *testing.T, topicName string, conn *apiclient.KafkaApiConnection, agent *Agent, authTimeout time.Duration) {
	sendFetchExpectErrCode(t, conn, topicName, kafkaprotocol.ErrorCodeTopicAuthorizationFailed)
	memberID := uuid.New().String()
	groupID := fmt.Sprintf("group-%s", uuid.New().String())
	agent.groupCoordinator.CreateGroupWithMember(groupID, memberID)
	defer agent.groupCoordinator.DeleteGroup(groupID)
	// Need to add an acl to prevent group auth being denied
	createAcl(t, agent, acls.AclEntry{
		Principal:           "User:admin",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationRead,
		ResourceType:        acls.ResourceTypeGroup,
		ResourceName:        groupID,
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
		Host:                "*",
	})
	sendOffsetCommitExpectErrCode(t, conn, topicName, groupID, memberID, kafkaprotocol.ErrorCodeTopicAuthorizationFailed)
	// TODO test TxnOffsetCommit when we support transactions
	time.Sleep(authTimeout) // timeout cached auth
	// Now add an acl to allow access
	createAcl(t, agent, acls.AclEntry{
		Principal:           "User:admin",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationRead,
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        topicName,
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
		Host:                "*",
	})
	// should now succeed
	sendFetchExpectErrCode(t, conn, topicName, kafkaprotocol.ErrorCodeNone)
	// We expect to get coordinator not available - that's because the epoch is invalid - that doesn't matter we are
	// just testing auth here
	sendOffsetCommitExpectErrCode(t, conn, topicName, groupID, memberID, kafkaprotocol.ErrorCodeCoordinatorNotAvailable)
}

func sendFetchExpectErrCode(t *testing.T, conn *apiclient.KafkaApiConnection, topicName string, expectedErrCode int) {
	req := kafkaprotocol.FetchRequest{
		Topics: []kafkaprotocol.FetchRequestFetchTopic{
			{
				Topic: common.StrPtr(topicName),
				Partitions: []kafkaprotocol.FetchRequestFetchPartition{
					{
						Partition:   0,
						FetchOffset: 0,
					},
				},
			},
		},
	}
	resp := &kafkaprotocol.FetchResponse{}
	r, err := conn.SendRequest(&req, kafkaprotocol.APIKeyFetch, 4, resp)
	require.NoError(t, err)
	resp = r.(*kafkaprotocol.FetchResponse)
	require.Equal(t, 1, len(resp.Responses))
	topicResp := resp.Responses[0]
	require.Equal(t, 1, len(topicResp.Partitions))
	partResp := topicResp.Partitions[0]
	require.Equal(t, expectedErrCode, int(partResp.ErrorCode))
}

func sendOffsetCommitExpectErrCode(t *testing.T, conn *apiclient.KafkaApiConnection, topicName string, groupID string,
	memberID string, expectedErrCode int) {
	req := kafkaprotocol.OffsetCommitRequest{
		GroupId:  common.StrPtr(groupID),
		MemberId: common.StrPtr(memberID),
		Topics: []kafkaprotocol.OffsetCommitRequestOffsetCommitRequestTopic{
			{
				Name: common.StrPtr(topicName),
				Partitions: []kafkaprotocol.OffsetCommitRequestOffsetCommitRequestPartition{
					{
						PartitionIndex:  1,
						CommittedOffset: 1234,
					},
				},
			},
		},
	}
	resp := &kafkaprotocol.OffsetCommitResponse{}
	r, err := conn.SendRequest(&req, kafkaprotocol.APIKeyOffsetCommit, 2, resp)
	require.NoError(t, err)
	resp = r.(*kafkaprotocol.OffsetCommitResponse)
	require.Equal(t, 1, len(resp.Topics))
	topicResp := resp.Topics[0]
	require.Equal(t, 1, len(topicResp.Partitions))
	partResp := topicResp.Partitions[0]
	require.Equal(t, expectedErrCode, int(partResp.ErrorCode))
}

func testTopicWritePermission(t *testing.T, topicName string, conn *apiclient.KafkaApiConnection, agent *Agent, authTimeout time.Duration) {
	sendProduceExpectErrCode(t, conn, topicName, nil, kafkaprotocol.ErrorCodeTopicAuthorizationFailed)
	// TODO test AddPartitionsToTx when we support transactions
	time.Sleep(authTimeout) // timeout cached auth
	// Now add an acl to allow access
	createAcl(t, agent, acls.AclEntry{
		Principal:           "User:admin",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationWrite,
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        topicName,
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
		Host:                "*",
	})
	// should now succeed
	sendProduceExpectErrCode(t, conn, topicName, nil, kafkaprotocol.ErrorCodeNone)
}

func sendProduceExpectErrCode(t *testing.T, conn *apiclient.KafkaApiConnection, topicName string, transactionalID *string,
	expectedErrCode int) {
	batch := testutils.CreateKafkaRecordBatchWithIncrementingKVs(0, 1)
	req := kafkaprotocol.ProduceRequest{
		TransactionalId: transactionalID,
		TopicData: []kafkaprotocol.ProduceRequestTopicProduceData{
			{
				Name: common.StrPtr(topicName),
				PartitionData: []kafkaprotocol.ProduceRequestPartitionProduceData{
					{
						Index:   0,
						Records: batch,
					},
				},
			},
		},
	}
	resp := &kafkaprotocol.ProduceResponse{}
	r, err := conn.SendRequest(&req, kafkaprotocol.APIKeyProduce, 3, resp)
	require.NoError(t, err)
	resp = r.(*kafkaprotocol.ProduceResponse)
	require.Equal(t, 1, len(resp.Responses))
	topicResp := resp.Responses[0]
	require.Equal(t, 1, len(topicResp.PartitionResponses))
	partResp := topicResp.PartitionResponses[0]
	require.Equal(t, expectedErrCode, int(partResp.ErrorCode))
}

func testTopicCreatePermission(t *testing.T, topicName string, conn *apiclient.KafkaApiConnection, agent *Agent, authTimeout time.Duration) {
	sendCreateTopicsExpectErrCode(t, conn, kafkaprotocol.ErrorCodeTopicAuthorizationFailed)
	// TODO test metadata with create topic when we support that
	time.Sleep(authTimeout) // timeout cached auth
	// Now add an acl to allow access
	createAcl(t, agent, acls.AclEntry{
		Principal:           "User:admin",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationCreate,
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "*",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
		Host:                "*",
	})
	// should now succeed
	sendCreateTopicsExpectErrCode(t, conn, kafkaprotocol.ErrorCodeNone)
}

func sendCreateTopicsExpectErrCode(t *testing.T, conn *apiclient.KafkaApiConnection, expectedErrCode int) {
	newTopicName := fmt.Sprintf("topic-%s", uuid.New().String())
	req := kafkaprotocol.CreateTopicsRequest{
		Topics: []kafkaprotocol.CreateTopicsRequestCreatableTopic{
			{
				Name:          common.StrPtr(newTopicName),
				NumPartitions: 10,
			},
		},
	}
	resp := &kafkaprotocol.CreateTopicsResponse{}
	r, err := conn.SendRequest(&req, kafkaprotocol.APIKeyCreateTopics, 5, resp)
	require.NoError(t, err)
	resp = r.(*kafkaprotocol.CreateTopicsResponse)
	require.Equal(t, 1, len(resp.Topics))
	topicResp := resp.Topics[0]
	require.Equal(t, expectedErrCode, int(topicResp.ErrorCode))
}

func testTopicDeletePermission(t *testing.T, topicName string, conn *apiclient.KafkaApiConnection, agent *Agent, authTimeout time.Duration) {
	topicToDelete := fmt.Sprintf("topic-%s", uuid.New().String())
	makeTopic(t, agent, topicToDelete, 10)
	sendDeleteTopicsExpectErrCode(t, conn, topicToDelete, kafkaprotocol.ErrorCodeTopicAuthorizationFailed)
	// TODO test metadata with create topic when we support that
	time.Sleep(authTimeout) // timeout cached auth
	// Now add an acl to allow access
	createAcl(t, agent, acls.AclEntry{
		Principal:           "User:admin",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationDelete,
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "*",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
		Host:                "*",
	})
	// should now succeed
	sendDeleteTopicsExpectErrCode(t, conn, topicToDelete, kafkaprotocol.ErrorCodeNone)
}

func sendDeleteTopicsExpectErrCode(t *testing.T, conn *apiclient.KafkaApiConnection, topicToDelete string, expectedErrCode int) {
	req := kafkaprotocol.DeleteTopicsRequest{
		TopicNames: []*string{common.StrPtr(topicToDelete)},
	}
	resp := &kafkaprotocol.DeleteTopicsResponse{}
	r, err := conn.SendRequest(&req, kafkaprotocol.APIKeyDeleteTopics, 5, resp)
	require.NoError(t, err)
	resp = r.(*kafkaprotocol.DeleteTopicsResponse)
	require.Equal(t, 1, len(resp.Responses))
	topicResp := resp.Responses[0]
	require.Equal(t, expectedErrCode, int(topicResp.ErrorCode))
}

func testTopicAlterPermission(t *testing.T, topicName string, conn *apiclient.KafkaApiConnection, agent *Agent, authTimeout time.Duration) {
	sendCreatePartitionsExpectErrCode(t, conn, topicName, kafkaprotocol.ErrorCodeTopicAuthorizationFailed)
	time.Sleep(authTimeout) // timeout cached auth
	// Now add an acl to allow access
	createAcl(t, agent, acls.AclEntry{
		Principal:           "User:admin",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAlter,
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "*",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
		Host:                "*",
	})
	// should now succeed
	sendCreatePartitionsExpectErrCode(t, conn, topicName, kafkaprotocol.ErrorCodeNone)
}

func sendCreatePartitionsExpectErrCode(t *testing.T, conn *apiclient.KafkaApiConnection, topicName string, expectedErrCode int) {
	req := kafkaprotocol.CreatePartitionsRequest{
		Topics: []kafkaprotocol.CreatePartitionsRequestCreatePartitionsTopic{
			{
				Name:  common.StrPtr(topicName),
				Count: 1000,
			},
		},
	}
	resp := &kafkaprotocol.CreatePartitionsResponse{}
	r, err := conn.SendRequest(&req, kafkaprotocol.ApiKeyCreatePartitions, 0, resp)
	require.NoError(t, err)
	resp = r.(*kafkaprotocol.CreatePartitionsResponse)
	require.Equal(t, 1, len(resp.Results))
	topicResp := resp.Results[0]
	require.Equal(t, expectedErrCode, int(topicResp.ErrorCode))
}

func testTopicDescribePermission(t *testing.T, topicName string, conn *apiclient.KafkaApiConnection, agent *Agent, authTimeout time.Duration) {
	sendListOffsetsExpectErrCode(t, conn, topicName, kafkaprotocol.ErrorCodeTopicAuthorizationFailed)
	sendMetadataExpectErrCode(t, conn, topicName, kafkaprotocol.ErrorCodeTopicAuthorizationFailed)
	groupID := fmt.Sprintf("group-%s", uuid.New().String())
	agent.groupCoordinator.CreateGroupWithMember(groupID, uuid.New().String())
	defer agent.groupCoordinator.DeleteGroup(groupID)
	// Need to add acl to prevent group describe being denied
	createAcl(t, agent, acls.AclEntry{
		Principal:           "User:admin",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationDescribe,
		ResourceType:        acls.ResourceTypeGroup,
		ResourceName:        groupID,
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
		Host:                "*",
	})
	sendOffsetFetchExpectErrorCode(t, conn, topicName, groupID, kafkaprotocol.ErrorCodeTopicAuthorizationFailed)
	time.Sleep(authTimeout) // timeout cached auth
	// Now add an acl to allow access
	createAcl(t, agent, acls.AclEntry{
		Principal:           "User:admin",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationDescribe,
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "*",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
		Host:                "*",
	})
	// should now succeed
	sendListOffsetsExpectErrCode(t, conn, topicName, kafkaprotocol.ErrorCodeNone)
	sendMetadataExpectErrCode(t, conn, topicName, kafkaprotocol.ErrorCodeNone)
	sendOffsetFetchExpectErrorCode(t, conn, topicName, groupID, kafkaprotocol.ErrorCodeNone)
}

func sendListOffsetsExpectErrCode(t *testing.T, conn *apiclient.KafkaApiConnection, topicName string, expectedErrCode int) {
	req := kafkaprotocol.ListOffsetsRequest{
		Topics: []kafkaprotocol.ListOffsetsRequestListOffsetsTopic{
			{
				Name: common.StrPtr(topicName),
				Partitions: []kafkaprotocol.ListOffsetsRequestListOffsetsPartition{
					{
						PartitionIndex: 1,
						Timestamp:      -2,
					},
				},
			},
		},
	}
	resp := &kafkaprotocol.ListOffsetsResponse{}
	r, err := conn.SendRequest(&req, kafkaprotocol.APIKeyListOffsets, 1, resp)
	require.NoError(t, err)
	resp = r.(*kafkaprotocol.ListOffsetsResponse)
	require.Equal(t, 1, len(resp.Topics))
	topicResp := resp.Topics[0]
	require.Equal(t, 1, len(topicResp.Partitions))
	partResp := topicResp.Partitions[0]
	require.Equal(t, expectedErrCode, int(partResp.ErrorCode))
}

func sendMetadataExpectErrCode(t *testing.T, conn *apiclient.KafkaApiConnection, topicName string, expectedErrCode int) {
	req := kafkaprotocol.MetadataRequest{
		Topics: []kafkaprotocol.MetadataRequestMetadataRequestTopic{
			{
				Name: common.StrPtr(topicName),
			},
		},
	}
	resp := &kafkaprotocol.MetadataResponse{}
	r, err := conn.SendRequest(&req, kafkaprotocol.APIKeyMetadata, 1, resp)
	require.NoError(t, err)
	resp = r.(*kafkaprotocol.MetadataResponse)
	require.Equal(t, 1, len(resp.Topics))
	topicResp := resp.Topics[0]
	require.Equal(t, expectedErrCode, int(topicResp.ErrorCode))

	// Also send a metadata to list all topics - unauthorised ones should be filtered out
	req = kafkaprotocol.MetadataRequest{}
	resp = &kafkaprotocol.MetadataResponse{}
	r, err = conn.SendRequest(&req, kafkaprotocol.APIKeyMetadata, 1, resp)
	require.NoError(t, err)
	resp = r.(*kafkaprotocol.MetadataResponse)
	if expectedErrCode == kafkaprotocol.ErrorCodeNone {
		found := false
		for _, topic := range resp.Topics {
			if common.SafeDerefStringPtr(topic.Name) == topicName {
				require.Equal(t, kafkaprotocol.ErrorCodeNone, int(topic.ErrorCode))
				found = true
				break
			}
		}
		require.True(t, found)
	} else {
		require.Equal(t, 0, len(resp.Topics))
	}
}

func sendOffsetFetchExpectErrorCode(t *testing.T, conn *apiclient.KafkaApiConnection, topicName string, groupID string,
	expectedErrCode int) {
	req := kafkaprotocol.OffsetFetchRequest{
		GroupId: common.StrPtr(groupID),
		Topics: []kafkaprotocol.OffsetFetchRequestOffsetFetchRequestTopic{
			{
				Name:             common.StrPtr(topicName),
				PartitionIndexes: []int32{1},
			},
		},
	}
	resp := &kafkaprotocol.OffsetFetchResponse{}
	r, err := conn.SendRequest(&req, kafkaprotocol.APIKeyOffsetFetch, 1, resp)
	require.NoError(t, err)
	resp = r.(*kafkaprotocol.OffsetFetchResponse)
	require.Equal(t, 1, len(resp.Topics))
	topicResp := resp.Topics[0]
	require.Equal(t, 1, len(topicResp.Partitions))
	partResp := topicResp.Partitions[0]
	require.Equal(t, expectedErrCode, int(partResp.ErrorCode))
}

func testClusterCreatePermission(t *testing.T, _ string, conn *apiclient.KafkaApiConnection, agent *Agent, authTimeout time.Duration) {
	sendCreateTopicsExpectErrCode(t, conn, kafkaprotocol.ErrorCodeTopicAuthorizationFailed)
	// TODO test metadata with create topic when we support that
	time.Sleep(authTimeout) // timeout cached auth
	// Now add an acl to allow access
	createAcl(t, agent, acls.AclEntry{
		Principal:           "User:admin",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationCreate,
		ResourceType:        acls.ResourceTypeCluster,
		ResourceName:        acls.ClusterResourceName,
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
		Host:                "*",
	})
	// should now succeed
	sendCreateTopicsExpectErrCode(t, conn, kafkaprotocol.ErrorCodeNone)
}

func testClusterAlterPermission(t *testing.T, _ string, conn *apiclient.KafkaApiConnection, agent *Agent, authTimeout time.Duration) {
	sendCreateAclsExpectErrorCode(t, conn, kafkaprotocol.ErrorCodeClusterAuthorizationFailed)
	sendDeleteAclsExpectErrorCode(t, conn, kafkaprotocol.ErrorCodeClusterAuthorizationFailed)
	username := "some-user"
	password := "some-password"
	sendPutUserCredsExpectErrorCode(t, conn, kafkaprotocol.ErrorCodeClusterAuthorizationFailed, username, password)
	sendDeleteUserExpectErrorCode(t, conn, kafkaprotocol.ErrorCodeClusterAuthorizationFailed, username)

	time.Sleep(authTimeout) // timeout cached auth
	// Now add an acl to allow access
	createAcl(t, agent, acls.AclEntry{
		Principal:           "User:admin",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAlter,
		ResourceType:        acls.ResourceTypeCluster,
		ResourceName:        acls.ClusterResourceName,
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
		Host:                "*",
	})
	// should now succeed
	sendCreateAclsExpectErrorCode(t, conn, kafkaprotocol.ErrorCodeNone)
	sendDeleteAclsExpectErrorCode(t, conn, kafkaprotocol.ErrorCodeNone)
	sendPutUserCredsExpectErrorCode(t, conn, kafkaprotocol.ErrorCodeNone, username, password)
	sendDeleteUserExpectErrorCode(t, conn, kafkaprotocol.ErrorCodeNone, username)
}

func sendCreateAclsExpectErrorCode(t *testing.T, conn *apiclient.KafkaApiConnection, expectedErrCode int) {
	req := kafkaprotocol.CreateAclsRequest{
		Creations: []kafkaprotocol.CreateAclsRequestAclCreation{
			{
				ResourceType:        int8(acls.ResourceTypeTopic),
				ResourceName:        common.StrPtr("foo"),
				ResourcePatternType: int8(acls.ResourcePatternTypeLiteral),
				Principal:           common.StrPtr(fmt.Sprintf("User:%s", uuid.New().String())),
				Host:                common.StrPtr("*"),
				Operation:           int8(acls.OperationCreate),
				PermissionType:      int8(acls.PermissionAllow),
			},
		},
	}
	resp := &kafkaprotocol.CreateAclsResponse{}
	r, err := conn.SendRequest(&req, kafkaprotocol.ApiKeyCreateAcls, 3, resp)
	require.NoError(t, err)
	resp = r.(*kafkaprotocol.CreateAclsResponse)
	require.Equal(t, 1, len(resp.Results))
	res := resp.Results[0]
	require.Equal(t, expectedErrCode, int(res.ErrorCode))
}

func sendDeleteAclsExpectErrorCode(t *testing.T, conn *apiclient.KafkaApiConnection, expectedErrCode int) {
	req := kafkaprotocol.DeleteAclsRequest{
		Filters: []kafkaprotocol.DeleteAclsRequestDeleteAclsFilter{
			{
				ResourceTypeFilter: int8(acls.ResourceTypeTopic),
				ResourceNameFilter: common.StrPtr("foo"),
				PatternTypeFilter:  int8(acls.ResourcePatternTypeLiteral),
				PrincipalFilter:    common.StrPtr(fmt.Sprintf("User:%s", uuid.New().String())),
				HostFilter:         common.StrPtr("*"),
				Operation:          int8(acls.OperationCreate),
				PermissionType:     int8(acls.PermissionAllow),
			},
		},
	}
	resp := &kafkaprotocol.DeleteAclsResponse{}
	r, err := conn.SendRequest(&req, kafkaprotocol.ApiKeyDeleteAcls, 3, resp)
	require.NoError(t, err)
	resp = r.(*kafkaprotocol.DeleteAclsResponse)
	require.Equal(t, 1, len(resp.FilterResults))
	res := resp.FilterResults[0]
	require.Equal(t, expectedErrCode, int(res.ErrorCode))
}

func sendPutUserCredsExpectErrorCode(t *testing.T, conn *apiclient.KafkaApiConnection, expectedErrCode int,
	username string, password string) {
	storedKey, serverKey, salt := auth2.CreateUserScramCreds(password, auth2.AuthenticationSaslScramSha512)
	req := kafkaprotocol.PutUserCredentialsRequest{
		Username:  common.StrPtr(username),
		StoredKey: storedKey,
		ServerKey: serverKey,
		Salt:      common.StrPtr(salt),
	}
	resp := &kafkaprotocol.PutUserCredentialsResponse{}
	r, err := conn.SendRequest(&req, kafkaprotocol.ApiKeyPutUserCredentialsRequest, 0, resp)
	require.NoError(t, err)
	resp = r.(*kafkaprotocol.PutUserCredentialsResponse)
	require.Equal(t, expectedErrCode, int(resp.ErrorCode))
}

func sendDeleteUserExpectErrorCode(t *testing.T, conn *apiclient.KafkaApiConnection, expectedErrCode int,
	username string) {
	req := kafkaprotocol.DeleteUserRequest{
		Username: common.StrPtr(username),
	}
	resp := &kafkaprotocol.DeleteUserResponse{}
	r, err := conn.SendRequest(&req, kafkaprotocol.ApiKeyDeleteUserRequest, 0, resp)
	require.NoError(t, err)
	resp = r.(*kafkaprotocol.DeleteUserResponse)
	require.Equal(t, expectedErrCode, int(resp.ErrorCode))
}

func testClusterDescribePermission(t *testing.T, _ string, conn *apiclient.KafkaApiConnection, agent *Agent, authTimeout time.Duration) {
	sendDescribeAclsExpectErrorCode(t, conn, kafkaprotocol.ErrorCodeClusterAuthorizationFailed)
	numGroups := 10
	var groupIds []string
	for i := 0; i < numGroups; i++ {
		groupId := fmt.Sprintf("group-%s", uuid.New().String())
		memberID := uuid.New().String()
		agent.groupCoordinator.CreateGroupWithMember(groupId, memberID)
		groupIds = append(groupIds, groupId)
	}
	defer func() {
		for _, groupId := range groupIds {
			agent.groupCoordinator.DeleteGroup(groupId)
		}
	}()
	// No groups should be returned as no cluster auth
	sendListGroups(t, conn, nil)
	time.Sleep(authTimeout) // timeout cached auth
	// Now add an acl to allow access
	createAcl(t, agent, acls.AclEntry{
		Principal:           "User:admin",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationDescribe,
		ResourceType:        acls.ResourceTypeCluster,
		ResourceName:        acls.ClusterResourceName,
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
		Host:                "*",
	})
	// should now succeed
	sendDescribeAclsExpectErrorCode(t, conn, kafkaprotocol.ErrorCodeNone)
	// All groups should be returned as has cluster auth
	sendListGroups(t, conn, groupIds)
}

func sendDescribeAclsExpectErrorCode(t *testing.T, conn *apiclient.KafkaApiConnection, expectedErrCode int) {
	req := kafkaprotocol.DescribeAclsRequest{
		ResourceTypeFilter: int8(acls.ResourceTypeTopic),
		ResourceNameFilter: common.StrPtr("foo"),
		PatternTypeFilter:  int8(acls.ResourcePatternTypeLiteral),
		PrincipalFilter:    common.StrPtr(fmt.Sprintf("User:%s", uuid.New().String())),
		HostFilter:         common.StrPtr("*"),
		Operation:          int8(acls.OperationCreate),
		PermissionType:     int8(acls.PermissionAllow),
	}
	resp := &kafkaprotocol.DescribeAclsResponse{}
	r, err := conn.SendRequest(&req, kafkaprotocol.ApiKeyDescribeAcls, 3, resp)
	require.NoError(t, err)
	resp = r.(*kafkaprotocol.DescribeAclsResponse)
	require.Equal(t, expectedErrCode, int(resp.ErrorCode))
}

func sendListGroups(t *testing.T, conn *apiclient.KafkaApiConnection, expectedGroups []string) {
	req := kafkaprotocol.ListGroupsRequest{}
	resp := &kafkaprotocol.ListGroupsResponse{}
	r, err := conn.SendRequest(&req, kafkaprotocol.ApiKeyListGroups, 5, resp)
	require.NoError(t, err)
	resp = r.(*kafkaprotocol.ListGroupsResponse)
	sort.SliceStable(resp.Groups, func(i, j int) bool {
		return common.SafeDerefStringPtr(resp.Groups[i].GroupId) < common.SafeDerefStringPtr(resp.Groups[j].GroupId)
	})
	sort.SliceStable(expectedGroups, func(i, j int) bool {
		return expectedGroups[i] < expectedGroups[j]
	})
	require.Equal(t, len(expectedGroups), len(resp.Groups))
	for i, expected := range expectedGroups {
		require.Equal(t, expected, common.SafeDerefStringPtr(resp.Groups[i].GroupId))
	}
}

func testGroupDeletePermission(t *testing.T, _ string, conn *apiclient.KafkaApiConnection, agent *Agent, authTimeout time.Duration) {
	groupID := "some-group"
	sendGroupDeleteExpectErrorCode(t, conn, kafkaprotocol.ErrorCodeGroupAuthorizationFailed, agent, groupID)
	time.Sleep(authTimeout) // timeout cached auth
	// Now add an acl to allow access
	createAcl(t, agent, acls.AclEntry{
		Principal:           "User:admin",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationDelete,
		ResourceType:        acls.ResourceTypeGroup,
		ResourceName:        groupID,
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
		Host:                "*",
	})
	// should now succeed
	// Will get ErrorCodeCoordinatorNotAvailable as epoch is invalid but the auth check will have been done
	sendGroupDeleteExpectErrorCode(t, conn, kafkaprotocol.ErrorCodeCoordinatorNotAvailable, agent, groupID)
}

func sendGroupDeleteExpectErrorCode(t *testing.T, conn *apiclient.KafkaApiConnection, expectedErrCode int,
	agent *Agent, groupID string) {
	agent.groupCoordinator.CreateGroupWithMember(groupID, "foo")
	defer agent.groupCoordinator.DeleteGroup(groupID)
	req := kafkaprotocol.DeleteGroupsRequest{
		GroupsNames: []*string{common.StrPtr(groupID)},
	}
	resp := &kafkaprotocol.DeleteGroupsResponse{}
	r, err := conn.SendRequest(&req, kafkaprotocol.ApiKeyDeleteGroups, 0, resp)
	require.NoError(t, err)
	resp = r.(*kafkaprotocol.DeleteGroupsResponse)
	require.Equal(t, 1, len(resp.Results))
	require.Equal(t, expectedErrCode, int(resp.Results[0].ErrorCode))
}

func testGroupDescribePermission(t *testing.T, topicName string, conn *apiclient.KafkaApiConnection, agent *Agent, authTimeout time.Duration) {
	groupID := fmt.Sprintf("group-%s", uuid.New().String())
	agent.groupCoordinator.CreateGroupWithMember(groupID, uuid.New().String())
	defer agent.groupCoordinator.DeleteGroup(groupID)
	sendGroupDescribeExpectErrorCode(t, conn, kafkaprotocol.ErrorCodeGroupAuthorizationFailed, agent, groupID)
	sendFindCoordinatorExpectErrorCode(t, conn, kafkaprotocol.ErrorCodeGroupAuthorizationFailed, agent, groupID, 0)
	// Group should not be visible
	sendListGroups(t, conn, nil)
	// create an acl to give topic auth
	createAcl(t, agent, acls.AclEntry{
		Principal:           "User:admin",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationDescribe,
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        topicName,
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
		Host:                "*",
	})
	sendOffsetFetchExpectErrorCode(t, conn, topicName, groupID, kafkaprotocol.ErrorCodeGroupAuthorizationFailed)
	time.Sleep(authTimeout) // timeout cached auth
	// Now add an acl to allow access
	createAcl(t, agent, acls.AclEntry{
		Principal:           "User:admin",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationDescribe,
		ResourceType:        acls.ResourceTypeGroup,
		ResourceName:        groupID,
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
		Host:                "*",
	})
	// should now succeed
	sendGroupDescribeExpectErrorCode(t, conn, kafkaprotocol.ErrorCodeNone, agent, groupID)
	sendFindCoordinatorExpectErrorCode(t, conn, kafkaprotocol.ErrorCodeNone, agent, groupID, 0)
	// Group should now be visible
	sendListGroups(t, conn, []string{groupID})
	sendOffsetFetchExpectErrorCode(t, conn, topicName, groupID, kafkaprotocol.ErrorCodeNone)
}

func sendGroupDescribeExpectErrorCode(t *testing.T, conn *apiclient.KafkaApiConnection, expectedErrCode int,
	agent *Agent, groupID string) {
	req := kafkaprotocol.DescribeGroupsRequest{
		Groups: []*string{common.StrPtr(groupID)},
	}
	resp := &kafkaprotocol.DescribeGroupsResponse{}
	r, err := conn.SendRequest(&req, kafkaprotocol.ApiKeyDescribeGroups, 0, resp)
	require.NoError(t, err)
	resp = r.(*kafkaprotocol.DescribeGroupsResponse)
	require.Equal(t, 1, len(resp.Groups))
	require.Equal(t, expectedErrCode, int(resp.Groups[0].ErrorCode))
}

func sendFindCoordinatorExpectErrorCode(t *testing.T, conn *apiclient.KafkaApiConnection, expectedErrCode int,
	agent *Agent, key string, keyType int8) {
	req := kafkaprotocol.FindCoordinatorRequest{
		Key:     common.StrPtr(key),
		KeyType: keyType,
	}
	resp := &kafkaprotocol.FindCoordinatorResponse{}
	r, err := conn.SendRequest(&req, kafkaprotocol.APIKeyFindCoordinator, 1, resp)
	require.NoError(t, err)
	resp = r.(*kafkaprotocol.FindCoordinatorResponse)
	require.Equal(t, expectedErrCode, int(resp.ErrorCode))
}

func testGroupReadPermission(t *testing.T, topicName string, conn *apiclient.KafkaApiConnection, agent *Agent, authTimeout time.Duration) {
	groupID := fmt.Sprintf("group-%s", uuid.New().String())
	memberID := uuid.New().String()
	agent.groupCoordinator.CreateGroupWithMember(groupID, memberID)
	defer agent.groupCoordinator.DeleteGroup(groupID)
	sendJoinGroupExpectErrorCode(t, conn, kafkaprotocol.ErrorCodeGroupAuthorizationFailed, groupID)
	sendSyncGroupExpectErrorCode(t, conn, kafkaprotocol.ErrorCodeGroupAuthorizationFailed, groupID)
	sendHeartbeatExpectErrorCode(t, conn, kafkaprotocol.ErrorCodeGroupAuthorizationFailed, groupID)
	sendOffsetCommitExpectErrCode(t, conn, topicName, groupID, memberID, kafkaprotocol.ErrorCodeGroupAuthorizationFailed)
	sendLeaveGroupExpectErrCode(t, conn, groupID, kafkaprotocol.ErrorCodeGroupAuthorizationFailed)
	time.Sleep(authTimeout) // timeout cached auth
	// Now add an acl to allow access
	createAcl(t, agent, acls.AclEntry{
		Principal:           "User:admin",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationRead,
		ResourceType:        acls.ResourceTypeGroup,
		ResourceName:        groupID,
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
		Host:                "*",
	})
	// should now succeed
	sendJoinGroupExpectErrorCode(t, conn, kafkaprotocol.ErrorCodeNone, groupID)
	sendSyncGroupExpectErrorCode(t, conn, kafkaprotocol.ErrorCodeIllegalGeneration, groupID)
	// Actually get illegal generation - that's ok, we went through auth
	sendHeartbeatExpectErrorCode(t, conn, kafkaprotocol.ErrorCodeIllegalGeneration, groupID)
	sendOffsetCommitExpectErrCode(t, conn, topicName, groupID, memberID, kafkaprotocol.ErrorCodeIllegalGeneration)
	sendLeaveGroupExpectErrCode(t, conn, groupID, kafkaprotocol.ErrorCodeNone)
}

func sendJoinGroupExpectErrorCode(t *testing.T, conn *apiclient.KafkaApiConnection, expectedErrCode int,
	groupID string) {
	req := kafkaprotocol.JoinGroupRequest{
		GroupId:      common.StrPtr(groupID),
		MemberId:     common.StrPtr(uuid.New().String()),
		ProtocolType: common.StrPtr("protocol1"),
	}
	resp := &kafkaprotocol.JoinGroupResponse{}
	r, err := conn.SendRequest(&req, kafkaprotocol.ApiKeyJoinGroup, 0, resp)
	require.NoError(t, err)
	resp = r.(*kafkaprotocol.JoinGroupResponse)
	require.Equal(t, expectedErrCode, int(resp.ErrorCode))
}

func sendSyncGroupExpectErrorCode(t *testing.T, conn *apiclient.KafkaApiConnection, expectedErrCode int,
	groupID string) {
	req := kafkaprotocol.SyncGroupRequest{
		GroupId:      common.StrPtr(groupID),
		MemberId:     common.StrPtr(uuid.New().String()),
		ProtocolType: common.StrPtr("protocol1"),
	}
	resp := &kafkaprotocol.SyncGroupResponse{}
	r, err := conn.SendRequest(&req, kafkaprotocol.ApiKeySyncGroup, 0, resp)
	require.NoError(t, err)
	resp = r.(*kafkaprotocol.SyncGroupResponse)
	require.Equal(t, expectedErrCode, int(resp.ErrorCode))
}

func sendHeartbeatExpectErrorCode(t *testing.T, conn *apiclient.KafkaApiConnection, expectedErrCode int,
	groupID string) {
	req := kafkaprotocol.HeartbeatRequest{
		GroupId:  common.StrPtr(groupID),
		MemberId: common.StrPtr(uuid.New().String()),
	}
	resp := &kafkaprotocol.HeartbeatResponse{}
	r, err := conn.SendRequest(&req, kafkaprotocol.ApiKeyHeartbeat, 0, resp)
	require.NoError(t, err)
	resp = r.(*kafkaprotocol.HeartbeatResponse)
	require.Equal(t, expectedErrCode, int(resp.ErrorCode))
}

func sendLeaveGroupExpectErrCode(t *testing.T, conn *apiclient.KafkaApiConnection, groupID string,
	expectedErrCode int) {
	req := kafkaprotocol.LeaveGroupRequest{
		GroupId:  common.StrPtr(groupID),
		MemberId: common.StrPtr(uuid.New().String()),
	}
	resp := &kafkaprotocol.LeaveGroupResponse{}
	r, err := conn.SendRequest(&req, kafkaprotocol.ApiKeyLeaveGroup, 0, resp)
	require.NoError(t, err)
	resp = r.(*kafkaprotocol.LeaveGroupResponse)
	require.Equal(t, expectedErrCode, int(resp.ErrorCode))
}

func testTransactionalIdDescribePermission(t *testing.T, topicName string, conn *apiclient.KafkaApiConnection, agent *Agent, authTimeout time.Duration) {
	txID := fmt.Sprintf("tx-%s", uuid.New().String())
	sendFindCoordinatorExpectErrorCode(t, conn, kafkaprotocol.ErrorCodeTransactionalIDAuthorizationFailed, agent, txID, 1)
	time.Sleep(authTimeout) // timeout cached auth
	// Now add an acl to allow access
	createAcl(t, agent, acls.AclEntry{
		Principal:           "User:admin",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationDescribe,
		ResourceType:        acls.ResourceTypeTransactionalID,
		ResourceName:        txID,
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
		Host:                "*",
	})
	// should now succeed
	sendFindCoordinatorExpectErrorCode(t, conn, kafkaprotocol.ErrorCodeNone, agent, txID, 1)
}

func testTransactionalIdWritePermission(t *testing.T, topicName string, conn *apiclient.KafkaApiConnection, agent *Agent, authTimeout time.Duration) {
	txID := fmt.Sprintf("tx-%s", uuid.New().String())
	// Add an acl to prevent topic write being denied
	createAcl(t, agent, acls.AclEntry{
		Principal:           "User:admin",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationWrite,
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        topicName,
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
		Host:                "*",
	})
	sendProduceExpectErrCode(t, conn, topicName, common.StrPtr(txID), kafkaprotocol.ErrorCodeTransactionalIDAuthorizationFailed)
	time.Sleep(authTimeout) // timeout cached auth
	// Now add an acl to allow access
	createAcl(t, agent, acls.AclEntry{
		Principal:           "User:admin",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationWrite,
		ResourceType:        acls.ResourceTypeTransactionalID,
		ResourceName:        txID,
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
		Host:                "*",
	})
	// should now succeed
	sendProduceExpectErrCode(t, conn, topicName, common.StrPtr(txID), kafkaprotocol.ErrorCodeNone)
}

func createAdminUser(t *testing.T, agent *Agent) {
	cl, err := agent.controlClientCache.GetClient()
	require.NoError(t, err)
	storedKey, serverKey, salt := auth.CreateUserScramCreds("admin", auth.AuthenticationSaslScramSha512)
	err = cl.PutUserCredentials("User:admin", storedKey, serverKey, salt, 4096)
	require.NoError(t, err)
}

func createAcl(t *testing.T, agent *Agent, entry acls.AclEntry) {
	cl, err := agent.controlClientCache.GetClient()
	require.NoError(t, err)
	err = cl.CreateAcls([]acls.AclEntry{entry})
	require.NoError(t, err)
}

func authenticateConnection(t *testing.T, conn *apiclient.KafkaApiConnection) {
	handshakeReq := kafkaprotocol.SaslHandshakeRequest{
		Mechanism: common.StrPtr("PLAIN"),
	}
	handshakeResp := &kafkaprotocol.SaslHandshakeResponse{}
	r1, err := conn.SendRequest(&handshakeReq, kafkaprotocol.APIKeySaslHandshake, 1, handshakeResp)
	require.NoError(t, err)
	handshakeResp = r1.(*kafkaprotocol.SaslHandshakeResponse)
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(handshakeResp.ErrorCode))
	authBuilder := &strings.Builder{}
	authBuilder.WriteByte(0)
	authBuilder.WriteString("User:admin")
	authBuilder.WriteByte(0)
	authBuilder.WriteString("admin")
	authRequest := kafkaprotocol.SaslAuthenticateRequest{
		AuthBytes: []byte(authBuilder.String()),
	}
	authResp := &kafkaprotocol.SaslAuthenticateResponse{}
	r2, err := conn.SendRequest(&authRequest, kafkaprotocol.APIKeySaslAuthenticate, 1, authResp)
	authResp = r2.(*kafkaprotocol.SaslAuthenticateResponse)
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(authResp.ErrorCode))
}
