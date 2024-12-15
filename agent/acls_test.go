package agent

import (
	"fmt"
	"github.com/spirit-labs/tektite/acls"
	"github.com/spirit-labs/tektite/apiclient"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	"github.com/spirit-labs/tektite/transport"
	"github.com/stretchr/testify/require"
	"math/rand"
	"sort"
	"testing"
	"time"
)

func TestAcls(t *testing.T) {

	cfg := NewConf()
	cfg.PusherConf.WriteTimeout = 1 * time.Millisecond // for fast commit of user creds
	agent, objStore, tearDown := setupAgent(t, nil, cfg)

	cl, err := apiclient.NewKafkaApiClientWithClientID("")
	require.NoError(t, err)
	conn, err := cl.NewConnection(agent.cfg.KafkaListenerConfig.Address)
	require.NoError(t, err)

	numAcls := 10
	var creations []kafkaprotocol.CreateAclsRequestAclCreation
	for i := 0; i < numAcls; i++ {
		creations = append(creations, kafkaprotocol.CreateAclsRequestAclCreation{
			ResourceType:        int8(acls.ResourceTypeTopic),
			ResourceName:        common.StrPtr(fmt.Sprintf("topic-%05d", i)),
			ResourcePatternType: int8(acls.ResourcePatternTypeLiteral),
			Principal:           common.StrPtr(fmt.Sprintf("User:user-%d", i)),
			Host:                common.StrPtr(generateFakeIPAddress()),
			Operation:           int8(acls.OperationRead),
			PermissionType:      int8(acls.PermissionAllow),
		})
	}

	createReq := kafkaprotocol.CreateAclsRequest{
		Creations: creations,
	}
	createResp := &kafkaprotocol.CreateAclsResponse{}
	r, err := conn.SendRequest(&createReq, kafkaprotocol.ApiKeyCreateAcls, 3, createResp)
	require.NoError(t, err)
	createResp = r.(*kafkaprotocol.CreateAclsResponse)
	require.Equal(t, numAcls, len(createResp.Results))
	for _, res := range createResp.Results {
		require.Equal(t, kafkaprotocol.ErrorCodeNone, int(res.ErrorCode))
	}

	verifyDescribe(t, conn, numAcls, creations)

	err = conn.Close()
	require.NoError(t, err)
	tearDown(t)

	// restart agent
	inMemMemberships := NewInMemClusterMemberships()
	inMemMemberships.Start()
	localTransports := transport.NewLocalTransports()
	agent, tearDown = setupAgentWithArgs(t, cfg, objStore, inMemMemberships, localTransports)

	// describe again - should be loaded
	conn, err = cl.NewConnection(agent.cfg.KafkaListenerConfig.Address)
	require.NoError(t, err)

	verifyDescribe(t, conn, numAcls, creations)

	// Now delete them all

	deleteReq := kafkaprotocol.DeleteAclsRequest{
		Filters: []kafkaprotocol.DeleteAclsRequestDeleteAclsFilter{
			{
				ResourceTypeFilter: int8(acls.ResourceTypeAny),
				ResourceNameFilter: nil,
				PatternTypeFilter:  int8(acls.ResourcePatternTypeAny),
				PrincipalFilter:    nil,
				HostFilter:         nil,
				Operation:          int8(acls.OperationAny),
				PermissionType:     int8(acls.PermissionAny),
			},
		},
	}
	deleteResp := &kafkaprotocol.DeleteAclsResponse{}
	r, err = conn.SendRequest(&deleteReq, kafkaprotocol.ApiKeyDeleteAcls, 3, deleteResp)
	require.NoError(t, err)
	deleteResp = r.(*kafkaprotocol.DeleteAclsResponse)
	require.Equal(t, 1, len(deleteResp.FilterResults))
	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(deleteResp.FilterResults[0].ErrorCode))

	verifyDescribeNoAcls(t, conn)

	// restart again
	err = conn.Close()
	require.NoError(t, err)
	tearDown(t)

	// restart agent
	inMemMemberships = NewInMemClusterMemberships()
	inMemMemberships.Start()
	localTransports = transport.NewLocalTransports()
	agent, tearDown = setupAgentWithArgs(t, cfg, objStore, inMemMemberships, localTransports)
	defer tearDown(t)

	// describe again - should be none
	conn, err = cl.NewConnection(agent.cfg.KafkaListenerConfig.Address)
	require.NoError(t, err)
	defer func() {
		err := conn.Close()
		require.NoError(t, err)
	}()
	verifyDescribeNoAcls(t, conn)
}

func verifyDescribe(t *testing.T, conn *apiclient.KafkaApiConnection, numAcls int, creations []kafkaprotocol.CreateAclsRequestAclCreation) {
	describeReq := kafkaprotocol.DescribeAclsRequest{
		ResourceTypeFilter: int8(acls.ResourceTypeAny),
		ResourceNameFilter: nil,
		PatternTypeFilter:  int8(acls.ResourcePatternTypeAny),
		PrincipalFilter:    nil,
		HostFilter:         nil,
		Operation:          int8(acls.OperationAny),
		PermissionType:     int8(acls.PermissionAny),
	}
	describeResp := &kafkaprotocol.DescribeAclsResponse{}
	r, err := conn.SendRequest(&describeReq, kafkaprotocol.ApiKeyDescribeAcls, 3, describeResp)
	require.NoError(t, err)
	describeResp = r.(*kafkaprotocol.DescribeAclsResponse)

	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(describeResp.ErrorCode))
	require.Equal(t, numAcls, len(describeResp.Resources))
	sort.SliceStable(describeResp.Resources, func(i, j int) bool {
		return common.SafeDerefStringPtr(describeResp.Resources[i].ResourceName) < common.SafeDerefStringPtr(describeResp.Resources[j].ResourceName)
	})
	for i, res := range describeResp.Resources {
		require.Equal(t, creations[i].ResourceType, res.ResourceType)
		require.Equal(t, creations[i].ResourceName, res.ResourceName)
		require.Equal(t, creations[i].ResourcePatternType, res.PatternType)
		require.Equal(t, 1, len(res.Acls))
		acl := res.Acls[0]
		require.Equal(t, creations[i].Principal, acl.Principal)
		require.Equal(t, creations[i].Host, acl.Host)
		require.Equal(t, creations[i].Operation, acl.Operation)
		require.Equal(t, creations[i].PermissionType, acl.PermissionType)
	}
}

func verifyDescribeNoAcls(t *testing.T, conn *apiclient.KafkaApiConnection) {
	describeReq := kafkaprotocol.DescribeAclsRequest{
		ResourceTypeFilter: int8(acls.ResourceTypeAny),
		ResourceNameFilter: nil,
		PatternTypeFilter:  int8(acls.ResourcePatternTypeAny),
		PrincipalFilter:    nil,
		HostFilter:         nil,
		Operation:          int8(acls.OperationAny),
		PermissionType:     int8(acls.PermissionAny),
	}
	describeResp := &kafkaprotocol.DescribeAclsResponse{}
	r, err := conn.SendRequest(&describeReq, kafkaprotocol.ApiKeyDescribeAcls, 3, describeResp)
	require.NoError(t, err)
	describeResp = r.(*kafkaprotocol.DescribeAclsResponse)

	require.Equal(t, kafkaprotocol.ErrorCodeNone, int(describeResp.ErrorCode))
	// should be none
	require.Equal(t, 0, len(describeResp.Resources))
}

func generateFakeIPAddress() string {
	return fmt.Sprintf("%d.%d.%d.%d", rand.Intn(256), rand.Intn(256), rand.Intn(256), rand.Intn(256))
}
