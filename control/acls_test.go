package control

import (
	"encoding/binary"
	"fmt"
	"github.com/spirit-labs/tektite/acls"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/parthash"
	"github.com/spirit-labs/tektite/transport"
	"github.com/stretchr/testify/require"
	"math/rand"
	"net"
	"sort"
	"strings"
	"testing"
)

func init() {
	common.EnableTestPorts()
}

func TestAuthorise(t *testing.T) {
	objStore := dev.NewInMemStore(0)
	controller, _, tearDown := setupControllerForAclTest(t, objStore)
	defer tearDown(t)
	updateMembership(t, 1, 1, []*Controller{controller}, 0)
	cl, err := controller.Client()
	require.NoError(t, err)
	defer func() {
		err := cl.Close()
		require.NoError(t, err)
	}()
	for _, holder := range aclTestCases {
		// delete all acls
		deleteAllAcls(t, cl, controller)
		t.Run(holder.testName, func(t *testing.T) {
			holder.testCase(t, cl, controller)
		})
	}
}

func testAuthoriseSpecificOperation(t *testing.T, cl Client, controller *Controller) {
	// For each valid operation type we test that the match must be exact to be authorised
	specificOperations := []acls.Operation{acls.OperationCreate, acls.OperationRead, acls.OperationWrite, acls.OperationDelete, acls.OperationDescribe}
	for i := 0; i < len(specificOperations); i++ {
		for j := 0; j < len(specificOperations); j++ {
			if i != j {
				deleteAllAcls(t, cl, controller)
				testAuthoriseOperation(t, cl, specificOperations[i], specificOperations[j])
			}
		}
	}
}

func testAuthoriseOperation(t *testing.T, cl Client, operation acls.Operation, otherOp acls.Operation) {
	entry1 := acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAllow,
		Operation:           operation,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "test-topic",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	entry2 := acls.AclEntry{
		Principal:           "User:bob",
		Permission:          acls.PermissionAllow,
		Operation:           otherOp,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "test-topic",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	err := cl.CreateAcls([]acls.AclEntry{entry1, entry2})
	require.NoError(t, err)
	authed, err := cl.Authorise("User:alice", acls.ResourceTypeTopic, "test-topic", operation)
	require.NoError(t, err)
	require.True(t, authed)
	authed, err = cl.Authorise("User:bob", acls.ResourceTypeTopic, "test-topic", operation)
	require.NoError(t, err)
	require.False(t, authed)
	authed, err = cl.Authorise("User:dave", acls.ResourceTypeTopic, "test-topic", operation)
	require.NoError(t, err)
	require.False(t, authed)
}

func testAuthoriseAclWithOperationAll(t *testing.T, cl Client, _ *Controller) {
	entry1 := acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "test-topic",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	err := cl.CreateAcls([]acls.AclEntry{entry1})
	require.NoError(t, err)
	specificOperations := []acls.Operation{acls.OperationCreate, acls.OperationRead, acls.OperationWrite, acls.OperationDelete, acls.OperationDescribe}
	for _, op := range specificOperations {
		authed, err := cl.Authorise("User:alice", acls.ResourceTypeTopic, "test-topic", op)
		require.NoError(t, err)
		require.True(t, authed)
	}
}

func testAuthoriseAclWithOperationAllAndSingleDeny(t *testing.T, cl Client, _ *Controller) {
	entry1 := acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "test-topic",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	entry2 := acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionDeny,
		Operation:           acls.OperationCreate,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "test-topic",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	err := cl.CreateAcls([]acls.AclEntry{entry1, entry2})
	require.NoError(t, err)
	specificOperations := []acls.Operation{acls.OperationRead, acls.OperationWrite, acls.OperationDelete, acls.OperationDescribe}
	for _, op := range specificOperations {
		authed, err := cl.Authorise("User:alice", acls.ResourceTypeTopic, "test-topic", op)
		require.NoError(t, err)
		require.True(t, authed)
	}
	// create should be denied
	authed, err := cl.Authorise("User:alice", acls.ResourceTypeTopic, "test-topic", acls.OperationCreate)
	require.NoError(t, err)
	require.False(t, authed)
}

func testAuthoriseIndividualAndDenyAll(t *testing.T, cl Client, _ *Controller) {
	specificOperations := []acls.Operation{acls.OperationCreate, acls.OperationRead, acls.OperationWrite, acls.OperationDelete, acls.OperationDescribe}
	var entries []acls.AclEntry
	for _, op := range specificOperations {
		entry := acls.AclEntry{
			Principal:           "User:alice",
			Permission:          acls.PermissionAllow,
			Operation:           op,
			Host:                "*",
			ResourceType:        acls.ResourceTypeTopic,
			ResourceName:        "test-topic",
			ResourcePatternType: acls.ResourcePatternTypeLiteral,
		}
		entries = append(entries, entry)
	}
	entry2 := acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionDeny,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "test-topic",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	entries = append(entries, entry2)
	err := cl.CreateAcls(entries)
	require.NoError(t, err)
	// should all be denied
	for _, op := range specificOperations {
		authed, err := cl.Authorise("User:alice", acls.ResourceTypeTopic, "test-topic", op)
		require.NoError(t, err)
		require.False(t, authed)
	}
}

func testAuthorisePrincipalName(t *testing.T, cl Client, _ *Controller) {
	entry1 := acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "test-topic",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	entry2 := acls.AclEntry{
		Principal:           "User:bob",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "test-topic",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	entry3 := acls.AclEntry{
		Principal:           "User:dave",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "test-topic2",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	err := cl.CreateAcls([]acls.AclEntry{entry1, entry2, entry3})
	require.NoError(t, err)
	authed, err := cl.Authorise("User:alice", acls.ResourceTypeTopic, "test-topic", acls.OperationRead)
	require.NoError(t, err)
	require.True(t, authed)
	authed, err = cl.Authorise("User:bob", acls.ResourceTypeTopic, "test-topic", acls.OperationRead)
	require.NoError(t, err)
	require.True(t, authed)
	authed, err = cl.Authorise("User:dave", acls.ResourceTypeTopic, "test-topic", acls.OperationRead)
	require.NoError(t, err)
	require.False(t, authed)
	authed, err = cl.Authorise("User:joe", acls.ResourceTypeTopic, "test-topic", acls.OperationRead)
	require.NoError(t, err)
	require.False(t, authed)
	authed, err = cl.Authorise("User:alice2", acls.ResourceTypeTopic, "test-topic", acls.OperationRead)
	require.NoError(t, err)
	require.False(t, authed)
	authed, err = cl.Authorise("User:alice ", acls.ResourceTypeTopic, "test-topic", acls.OperationRead)
	require.NoError(t, err)
	require.False(t, authed)
	authed, err = cl.Authorise("User: alice", acls.ResourceTypeTopic, "test-topic", acls.OperationRead)
	require.NoError(t, err)
	require.False(t, authed)
	authed, err = cl.Authorise("User:ALICE", acls.ResourceTypeTopic, "test-topic", acls.OperationRead)
	require.NoError(t, err)
	require.False(t, authed)
	authed, err = cl.Authorise("User:AlIcE", acls.ResourceTypeTopic, "test-topic", acls.OperationRead)
	require.NoError(t, err)
	require.False(t, authed)
}

func testAuthoriseHost(t *testing.T, cl Client, _ *Controller) {
	cc := cl.(*client)
	stc := cc.conn.(*transport.SocketTransportConnection)
	nc := stc.NetConn()
	clientHost, _, err := net.SplitHostPort(nc.LocalAddr().String())
	require.NoError(t, err)

	entry1 := acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                clientHost,
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "test-topic",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	entry2 := acls.AclEntry{
		Principal:           "User:bob",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "34.45.23.45", // Some random IP
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "test-topic",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	entry3 := acls.AclEntry{
		Principal:           "User:joe",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*", // Wildcard
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "test-topic",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	err = cl.CreateAcls([]acls.AclEntry{entry1, entry2, entry3})
	require.NoError(t, err)
	authed, err := cl.Authorise("User:alice", acls.ResourceTypeTopic, "test-topic", acls.OperationRead)
	require.NoError(t, err)
	require.True(t, authed)
	authed, err = cl.Authorise("User:bob", acls.ResourceTypeTopic, "test-topic", acls.OperationRead)
	require.NoError(t, err)
	require.False(t, authed)
	authed, err = cl.Authorise("User:joe", acls.ResourceTypeTopic, "test-topic", acls.OperationRead)
	require.NoError(t, err)
	require.True(t, authed)
}

func testDenyHost(t *testing.T, cl Client, _ *Controller) {
	cc := cl.(*client)
	stc := cc.conn.(*transport.SocketTransportConnection)
	nc := stc.NetConn()
	clientHost, _, err := net.SplitHostPort(nc.LocalAddr().String())
	require.NoError(t, err)

	entry1 := acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "test-topic",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	entry2 := acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionDeny,
		Operation:           acls.OperationAll,
		Host:                clientHost,
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "test-topic",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	entry3 := acls.AclEntry{
		Principal:           "User:bob",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                clientHost,
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "test-topic",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	entry4 := acls.AclEntry{
		Principal:           "User:bob",
		Permission:          acls.PermissionDeny,
		Operation:           acls.OperationAll,
		Host:                clientHost,
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "test-topic",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	err = cl.CreateAcls([]acls.AclEntry{entry1, entry2, entry3, entry4})
	require.NoError(t, err)
	authed, err := cl.Authorise("User:alice", acls.ResourceTypeTopic, "test-topic", acls.OperationRead)
	require.NoError(t, err)
	require.False(t, authed)
	authed, err = cl.Authorise("User:bob", acls.ResourceTypeTopic, "test-topic", acls.OperationRead)
	require.NoError(t, err)
	require.False(t, authed)
}

func testAuthoriseResourceType(t *testing.T, cl Client, _ *Controller) {
	entry1 := acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "*",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	entry2 := acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeGroup,
		ResourceName:        "*",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	err := cl.CreateAcls([]acls.AclEntry{entry1, entry2})
	require.NoError(t, err)
	authed, err := cl.Authorise("User:alice", acls.ResourceTypeTopic, "test-topic", acls.OperationRead)
	require.NoError(t, err)
	require.True(t, authed)
	authed, err = cl.Authorise("User:alice", acls.ResourceTypeGroup, "some-group", acls.OperationRead)
	require.NoError(t, err)
	require.True(t, authed)
	authed, err = cl.Authorise("User:alice", acls.ResourceTypeCluster, "some-cluster", acls.OperationRead)
	require.NoError(t, err)
	require.False(t, authed)
}

func testAuthoriseLiteralResource(t *testing.T, cl Client, _ *Controller) {
	entry1 := acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "some-topic",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	entry2 := acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "some-other-topic",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	err := cl.CreateAcls([]acls.AclEntry{entry1, entry2})
	require.NoError(t, err)
	authed, err := cl.Authorise("User:alice", acls.ResourceTypeTopic, "some-topic", acls.OperationRead)
	require.NoError(t, err)
	require.True(t, authed)
	authed, err = cl.Authorise("User:alice", acls.ResourceTypeTopic, "some-other-topic", acls.OperationRead)
	require.NoError(t, err)
	require.True(t, authed)
	authed, err = cl.Authorise("User:alice", acls.ResourceTypeGroup, "foo-topic", acls.OperationRead)
	require.NoError(t, err)
	require.False(t, authed)
}

func testAuthorisePrefixed(t *testing.T, cl Client, _ *Controller) {
	entry1 := acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "foo.",
		ResourcePatternType: acls.ResourcePatternTypePrefixed,
	}
	entry2 := acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionDeny,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "foo.topic4",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	err := cl.CreateAcls([]acls.AclEntry{entry1, entry2})
	require.NoError(t, err)
	authed, err := cl.Authorise("User:alice", acls.ResourceTypeTopic, "foo.topic1", acls.OperationRead)
	require.NoError(t, err)
	require.True(t, authed)
	authed, err = cl.Authorise("User:alice", acls.ResourceTypeTopic, "foo.topic2", acls.OperationRead)
	require.NoError(t, err)
	require.True(t, authed)
	authed, err = cl.Authorise("User:alice", acls.ResourceTypeTopic, "foo.topic3", acls.OperationRead)
	require.NoError(t, err)
	require.True(t, authed)
	authed, err = cl.Authorise("User:alice", acls.ResourceTypeTopic, "foo.topic4", acls.OperationRead)
	require.NoError(t, err)
	require.False(t, authed)
	authed, err = cl.Authorise("User:alice", acls.ResourceTypeTopic, "bar.topic1", acls.OperationRead)
	require.NoError(t, err)
	require.False(t, authed)
}

func testAuthoriseMultiple(t *testing.T, cl Client, _ *Controller) {
	numEntries := 10
	var entries []acls.AclEntry
	for i := 0; i < numEntries; i++ {
		entry := acls.AclEntry{
			Principal:           "User:alice",
			Permission:          acls.PermissionAllow,
			Operation:           acls.OperationAll,
			Host:                "*",
			ResourceType:        acls.ResourceTypeTopic,
			ResourceName:        fmt.Sprintf("allow-topic-%d", i),
			ResourcePatternType: acls.ResourcePatternTypeLiteral,
		}
		entries = append(entries, entry)
	}
	err := cl.CreateAcls(entries)
	require.NoError(t, err)
	for i := 0; i < numEntries; i++ {
		topicName := fmt.Sprintf("allow-topic-%d", i)
		authed, err := cl.Authorise("User:alice", acls.ResourceTypeTopic, topicName, acls.OperationRead)
		require.NoError(t, err)
		require.True(t, authed)
	}
}

func testCreateSameAclMoreThanOnce(t *testing.T, cl Client, _ *Controller) {
	numCreates := 10
	numDups := 10
	var entries []acls.AclEntry
	var unique []acls.AclEntry
	for i := 0; i < numCreates; i++ {
		for j := 0; j < numDups; j++ {
			entry := acls.AclEntry{
				Principal:           "User:alice",
				Permission:          acls.PermissionAllow,
				Operation:           acls.OperationAll,
				Host:                "*",
				ResourceType:        acls.ResourceTypeTopic,
				ResourceName:        fmt.Sprintf("some-topic-%d", i),
				ResourcePatternType: acls.ResourcePatternTypeLiteral,
			}
			entries = append(entries, entry)
			if j == 0 {
				unique = append(unique, entry)
			}
		}
	}
	// shuffle them
	rand.Shuffle(len(entries), func(i, j int) {
		entries[i], entries[j] = entries[j], entries[i]
	})

	err := cl.CreateAcls(entries)
	require.NoError(t, err)
	existing, err := cl.ListAcls(acls.ResourceTypeTopic, "", acls.ResourcePatternTypeAny,
		"User:alice", "*", acls.OperationAny, acls.PermissionAny)
	require.NoError(t, err)
	// Dups should be removed
	require.Equal(t, numCreates, len(existing))
	sort.SliceStable(existing, func(i, j int) bool {
		return existing[i].ResourceName < existing[j].ResourceName
	})
	require.Equal(t, unique, existing)

	// Now add some different ones to the already created ones and try and create them all again
	for i := 0; i < numCreates; i++ {
		for j := 0; j < numDups; j++ {
			entry := acls.AclEntry{
				Principal:           "User:alice",
				Permission:          acls.PermissionAllow,
				Operation:           acls.OperationAll,
				Host:                "*",
				ResourceType:        acls.ResourceTypeTopic,
				ResourceName:        fmt.Sprintf("xome-topic-%d", i),
				ResourcePatternType: acls.ResourcePatternTypeLiteral,
			}
			entries = append(entries, entry)
			if j == 0 {
				unique = append(unique, entry)
			}
		}
	}
	rand.Shuffle(len(entries), func(i, j int) {
		entries[i], entries[j] = entries[j], entries[i]
	})
	err = cl.CreateAcls(entries)
	require.NoError(t, err)
	existing, err = cl.ListAcls(acls.ResourceTypeTopic, "", acls.ResourcePatternTypeAny,
		"User:alice", "*", acls.OperationAny, acls.PermissionAny)
	sort.SliceStable(existing, func(i, j int) bool {
		return existing[i].ResourceName < existing[j].ResourceName
	})
	require.NoError(t, err)
	require.Equal(t, len(unique), len(existing))
	require.Equal(t, unique, existing)
}

func testValidateStoredAclEntry(t *testing.T, _ Client, _ *Controller) {
	entry := acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAny,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "some-topic",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	err := validateCreatedAcl(entry)
	require.Error(t, err)
	require.True(t, strings.HasPrefix(err.Error(), "ACL entry has invalid permission"))
	entry = acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionUnknown,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "some-topic",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	err = validateCreatedAcl(entry)
	require.Error(t, err)
	require.True(t, strings.HasPrefix(err.Error(), "ACL entry has invalid permission"))
	entry = acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAny,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "some-topic",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	err = validateCreatedAcl(entry)
	require.Error(t, err)
	require.True(t, strings.HasPrefix(err.Error(), "ACL entry has invalid operation"))
	entry = acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationUnknown,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "some-topic",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	err = validateCreatedAcl(entry)
	require.Error(t, err)
	require.True(t, strings.HasPrefix(err.Error(), "ACL entry has invalid operation"))
	entry = acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationRead,
		Host:                "foo.com",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "some-topic",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	err = validateCreatedAcl(entry)
	require.Error(t, err)
	require.True(t, strings.HasPrefix(err.Error(), "ACL entry has invalid host"))
	entry = acls.AclEntry{
		Principal:           "george",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationRead,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "some-topic",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	err = validateCreatedAcl(entry)
	require.Error(t, err)
	require.True(t, strings.HasPrefix(err.Error(), "ACL principals must start with 'User:'"))
	entry = acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationRead,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "some-topic",
		ResourcePatternType: acls.ResourcePatternTypeMatch,
	}
	err = validateCreatedAcl(entry)
	require.Error(t, err)
	require.True(t, strings.HasPrefix(err.Error(), "ACL resource pattern type must be literal or prefixed"))
	entry = acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationRead,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "some-topic",
		ResourcePatternType: acls.ResourcePatternTypeAny,
	}
	err = validateCreatedAcl(entry)
	require.Error(t, err)
	require.True(t, strings.HasPrefix(err.Error(), "ACL resource pattern type must be literal or prefixed"))
	entry = acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationRead,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "   ",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	err = validateCreatedAcl(entry)
	require.Error(t, err)
	require.True(t, strings.HasPrefix(err.Error(), "ACL entry resource name cannot be blank"))
	entry = acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationRead,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	err = validateCreatedAcl(entry)
	require.Error(t, err)
	require.True(t, strings.HasPrefix(err.Error(), "ACL entry resource name cannot be blank"))
}

func testListAllAcls(t *testing.T, cl Client, _ *Controller) {
	entry1 := acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "some-topic",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	entry2 := acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeGroup,
		ResourceName:        "some-group",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	entry3 := acls.AclEntry{
		Principal:           "User:bob",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeCluster,
		ResourceName:        "some-cluster",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	entry4 := acls.AclEntry{
		Principal:           "User:joe",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeGroup,
		ResourceName:        "some-group2",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	entries := []acls.AclEntry{entry1, entry2, entry3, entry4}
	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].ResourceName < entries[j].ResourceName
	})
	err := cl.CreateAcls(entries)
	require.NoError(t, err)

	listed, err := cl.ListAcls(acls.ResourceTypeAny, "", acls.ResourcePatternTypeAny,
		"", "", acls.OperationAny, acls.PermissionAny)
	require.NoError(t, err)
	require.Equal(t, len(entries), len(listed))
	sort.SliceStable(listed, func(i, j int) bool {
		return listed[i].ResourceName < listed[j].ResourceName
	})
	require.Equal(t, entries, listed)
}

func testListAllTopicAcls(t *testing.T, cl Client, _ *Controller) {
	entry1 := acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "some-topic",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	entry2 := acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "some-topic2",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	entry3 := acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "foo.topic1",
		ResourcePatternType: acls.ResourcePatternTypePrefixed,
	}
	entry4 := acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "foo.topic2",
		ResourcePatternType: acls.ResourcePatternTypePrefixed,
	}
	entry5 := acls.AclEntry{
		Principal:           "User:bob",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "some-topic4",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	entry6 := acls.AclEntry{
		Principal:           "User:bob",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeCluster,
		ResourceName:        "some-cluster",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	entries := []acls.AclEntry{entry1, entry2, entry3, entry4, entry5, entry6}
	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].ResourceName < entries[j].ResourceName
	})
	err := cl.CreateAcls(entries)
	require.NoError(t, err)

	listed, err := cl.ListAcls(acls.ResourceTypeTopic, "", acls.ResourcePatternTypeAny,
		"", "", acls.OperationAny, acls.PermissionAny)
	require.NoError(t, err)
	expected := []acls.AclEntry{entry1, entry2, entry3, entry4, entry5}
	sort.SliceStable(expected, func(i, j int) bool {
		return expected[i].ResourceName < expected[j].ResourceName
	})
	require.Equal(t, len(expected), len(listed))
	sort.SliceStable(listed, func(i, j int) bool {
		return listed[i].ResourceName < listed[j].ResourceName
	})
	require.Equal(t, expected, listed)
}

func testListAclsForTopic(t *testing.T, cl Client, _ *Controller) {
	entry1 := acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "some-topic",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	entry2 := acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "some-topic2",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	entry3 := acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "foo.",
		ResourcePatternType: acls.ResourcePatternTypePrefixed,
	}
	entry4 := acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "bar.",
		ResourcePatternType: acls.ResourcePatternTypePrefixed,
	}
	entry5 := acls.AclEntry{
		Principal:           "User:bob",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "some-topic",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	entry6 := acls.AclEntry{
		Principal:           "User:bob",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeCluster,
		ResourceName:        "some-cluster",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	entry7 := acls.AclEntry{
		Principal:           "User:bob",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "*",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	entry8 := acls.AclEntry{
		Principal:           "User:bob",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "foo.topic1",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	entries := []acls.AclEntry{entry1, entry2, entry3, entry4, entry5, entry6, entry7, entry8}
	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].ResourceName < entries[j].ResourceName
	})
	err := cl.CreateAcls(entries)
	require.NoError(t, err)

	// list specific literal topic
	listed, err := cl.ListAcls(acls.ResourceTypeTopic, "some-topic", acls.ResourcePatternTypeLiteral,
		"", "", acls.OperationAny, acls.PermissionAny)
	require.NoError(t, err)
	expected := []acls.AclEntry{entry1, entry5}
	require.Equal(t, len(expected), len(listed))
	sort.SliceStable(listed, func(i, j int) bool {
		if listed[i].Principal == listed[j].Principal {
			return listed[i].ResourceName < listed[j].ResourceName
		} else {
			return listed[i].Principal < listed[j].Principal
		}
	})
	require.Equal(t, expected, listed)

	// Cannot list prefixed using literal
	listed, err = cl.ListAcls(acls.ResourceTypeTopic, "foo.topic1", acls.ResourcePatternTypeLiteral,
		"", "", acls.OperationAny, acls.PermissionAny)
	sort.SliceStable(listed, func(i, j int) bool {
		return listed[i].ResourceName < listed[j].ResourceName
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(listed))
	// Just the literal one should be returned
	require.Equal(t, []acls.AclEntry{entry8}, listed)

	// Must specify pattern type prefixed
	listed, err = cl.ListAcls(acls.ResourceTypeTopic, "foo.", acls.ResourcePatternTypePrefixed,
		"", "", acls.OperationAny, acls.PermissionAny)
	require.NoError(t, err)
	require.Equal(t, 1, len(listed))
	require.Equal(t, entry3, listed[0])

	// List wildcard using literal
	listed, err = cl.ListAcls(acls.ResourceTypeTopic, "*", acls.ResourcePatternTypeLiteral,
		"", "", acls.OperationAny, acls.PermissionAny)
	require.NoError(t, err)
	require.Equal(t, 1, len(listed))
	require.Equal(t, entry7, listed[0])

	// List all using any
	listed, err = cl.ListAcls(acls.ResourceTypeTopic, "", acls.ResourcePatternTypeAny,
		"", "", acls.OperationAny, acls.PermissionAny)
	require.NoError(t, err)
	expected = []acls.AclEntry{entry1, entry2, entry3, entry4, entry5, entry7, entry8}
	require.Equal(t, len(expected), len(listed))
	sort.SliceStable(expected, func(i, j int) bool {
		if expected[i].Principal == expected[j].Principal {
			return expected[i].ResourceName < expected[j].ResourceName
		} else {
			return expected[i].Principal < expected[j].Principal
		}
	})
	sort.SliceStable(listed, func(i, j int) bool {
		if listed[i].Principal == listed[j].Principal {
			return listed[i].ResourceName < listed[j].ResourceName
		} else {
			return listed[i].Principal < listed[j].Principal
		}
	})
	require.Equal(t, expected, listed)

	// List specific resource names using any
	listed, err = cl.ListAcls(acls.ResourceTypeTopic, "foo.topic1", acls.ResourcePatternTypeAny,
		"", "", acls.OperationAny, acls.PermissionAny)
	require.NoError(t, err)
	require.Equal(t, 1, len(listed))
	require.Equal(t, []acls.AclEntry{entry8}, listed)

	// List using match
	listed, err = cl.ListAcls(acls.ResourceTypeTopic, "foo.topic1", acls.ResourcePatternTypeMatch,
		"", "", acls.OperationAny, acls.PermissionAny)
	require.NoError(t, err)
	expected = []acls.AclEntry{entry3, entry7, entry8}
	require.Equal(t, len(expected), len(listed))
	sort.SliceStable(listed, func(i, j int) bool {
		if listed[i].Principal == listed[j].Principal {
			return listed[i].ResourceName < listed[j].ResourceName
		} else {
			return listed[i].Principal < listed[j].Principal
		}
	})
	require.Equal(t, expected, listed)
}

/*
	Delete specific literal
	Delete specific prefixed
	Delete specific wildcard prefixed
	Delete using any and - make sure matches resource name
	Delete using match - make sure it deletes all acls that refer to a specific resource
*/

func testDeleteLiteralAclsSpecifically(t *testing.T, cl Client, _ *Controller) {
	entry1 := acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "foo",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	entry2 := acls.AclEntry{
		Principal:           "User:bob",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "*",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	entry3 := acls.AclEntry{
		Principal:           "User:joe",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "foo",
		ResourcePatternType: acls.ResourcePatternTypePrefixed,
	}
	err := cl.CreateAcls([]acls.AclEntry{entry1, entry2, entry3})
	require.NoError(t, err)

	// delete non-existent
	err = cl.DeleteAcls(acls.ResourceTypeTopic, "unknown", acls.ResourcePatternTypeLiteral, "", "", acls.OperationAny, acls.PermissionAny)
	require.NoError(t, err)
	entries := getAllAclsSortedByPrincipal(t, cl)
	require.Equal(t, []acls.AclEntry{entry1, entry2, entry3}, entries)

	// delete the wildcard only
	err = cl.DeleteAcls(acls.ResourceTypeTopic, "*", acls.ResourcePatternTypeLiteral, "", "", acls.OperationAny, acls.PermissionAny)
	entries = getAllAclsSortedByPrincipal(t, cl)
	require.Equal(t, []acls.AclEntry{entry1, entry3}, entries)
	err = cl.CreateAcls([]acls.AclEntry{entry2})
	require.NoError(t, err)

	// delete the non wildcard
	err = cl.DeleteAcls(acls.ResourceTypeTopic, "foo", acls.ResourcePatternTypeLiteral, "", "", acls.OperationAny, acls.PermissionAny)
	entries = getAllAclsSortedByPrincipal(t, cl)
	require.Equal(t, []acls.AclEntry{entry2, entry3}, entries)
}

func testDeletePrefixedAclsSpecifically(t *testing.T, cl Client, _ *Controller) {
	entry1 := acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "foo",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	entry2 := acls.AclEntry{
		Principal:           "User:bob",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "foo",
		ResourcePatternType: acls.ResourcePatternTypePrefixed,
	}
	entry3 := acls.AclEntry{
		Principal:           "User:joe",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "bar",
		ResourcePatternType: acls.ResourcePatternTypePrefixed,
	}
	err := cl.CreateAcls([]acls.AclEntry{entry1, entry2, entry3})
	require.NoError(t, err)

	// delete non-existent
	err = cl.DeleteAcls(acls.ResourceTypeTopic, "unknown", acls.ResourcePatternTypePrefixed, "", "", acls.OperationAny, acls.PermissionAny)
	require.NoError(t, err)
	entries := getAllAclsSortedByPrincipal(t, cl)
	require.Equal(t, []acls.AclEntry{entry1, entry2, entry3}, entries)

	// delete the foo prefixed acl
	err = cl.DeleteAcls(acls.ResourceTypeTopic, "foo", acls.ResourcePatternTypePrefixed, "", "", acls.OperationAny, acls.PermissionAny)
	entries = getAllAclsSortedByPrincipal(t, cl)
	require.Equal(t, []acls.AclEntry{entry1, entry3}, entries)
	err = cl.CreateAcls([]acls.AclEntry{entry2})
	require.NoError(t, err)

	// delete the bar prefixed acls
	err = cl.DeleteAcls(acls.ResourceTypeTopic, "bar", acls.ResourcePatternTypePrefixed, "", "", acls.OperationAny, acls.PermissionAny)
	entries = getAllAclsSortedByPrincipal(t, cl)
	require.Equal(t, []acls.AclEntry{entry1, entry2}, entries)
}

func testDeleteAllAclsOfResourceType(t *testing.T, cl Client, _ *Controller) {
	entry1 := acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "foo",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	entry2 := acls.AclEntry{
		Principal:           "User:bob",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "quux",
		ResourcePatternType: acls.ResourcePatternTypePrefixed,
	}
	entry3 := acls.AclEntry{
		Principal:           "User:joe",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeCluster,
		ResourceName:        "bar",
		ResourcePatternType: acls.ResourcePatternTypePrefixed,
	}
	entry4 := acls.AclEntry{
		Principal:           "User:kevin",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeGroup,
		ResourceName:        "wibble",
		ResourcePatternType: acls.ResourcePatternTypePrefixed,
	}
	err := cl.CreateAcls([]acls.AclEntry{entry1, entry2, entry3, entry4})
	require.NoError(t, err)

	err = cl.DeleteAcls(acls.ResourceTypeTopic, "", acls.ResourcePatternTypeAny, "", "", acls.OperationAny, acls.PermissionAny)
	require.NoError(t, err)
	entries := getAllAclsSortedByPrincipal(t, cl)
	require.Equal(t, []acls.AclEntry{entry3, entry4}, entries)
	err = cl.CreateAcls([]acls.AclEntry{entry1, entry2})
	require.NoError(t, err)

	err = cl.DeleteAcls(acls.ResourceTypeGroup, "", acls.ResourcePatternTypeAny, "", "", acls.OperationAny, acls.PermissionAny)
	require.NoError(t, err)
	entries = getAllAclsSortedByPrincipal(t, cl)
	require.Equal(t, []acls.AclEntry{entry1, entry2, entry3}, entries)
	err = cl.CreateAcls([]acls.AclEntry{entry4})
	require.NoError(t, err)

	err = cl.DeleteAcls(acls.ResourceTypeCluster, "", acls.ResourcePatternTypeAny, "", "", acls.OperationAny, acls.PermissionAny)
	require.NoError(t, err)
	entries = getAllAclsSortedByPrincipal(t, cl)
	require.Equal(t, []acls.AclEntry{entry1, entry2, entry4}, entries)
}

func testDeleteAllAcls(t *testing.T, cl Client, _ *Controller) {
	entry1 := acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "foo",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	entry2 := acls.AclEntry{
		Principal:           "User:bob",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "quux",
		ResourcePatternType: acls.ResourcePatternTypePrefixed,
	}
	entry3 := acls.AclEntry{
		Principal:           "User:joe",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeCluster,
		ResourceName:        "bar",
		ResourcePatternType: acls.ResourcePatternTypePrefixed,
	}
	entry4 := acls.AclEntry{
		Principal:           "User:kevin",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeGroup,
		ResourceName:        "wibble",
		ResourcePatternType: acls.ResourcePatternTypePrefixed,
	}
	err := cl.CreateAcls([]acls.AclEntry{entry1, entry2, entry3, entry4})
	require.NoError(t, err)

	err = cl.DeleteAcls(acls.ResourceTypeAny, "", acls.ResourcePatternTypeAny, "", "", acls.OperationAny, acls.PermissionAny)
	require.NoError(t, err)
	entries := getAllAclsSortedByPrincipal(t, cl)
	require.Equal(t, 0, len(entries))
}

func testDeleteAllAclsWithName(t *testing.T, cl Client, _ *Controller) {
	entry1 := acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "foo",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	entry2 := acls.AclEntry{
		Principal:           "User:bob",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "wibble",
		ResourcePatternType: acls.ResourcePatternTypePrefixed,
	}
	entry3 := acls.AclEntry{
		Principal:           "User:joe",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeCluster,
		ResourceName:        "foo",
		ResourcePatternType: acls.ResourcePatternTypePrefixed,
	}
	entry4 := acls.AclEntry{
		Principal:           "User:kevin",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeGroup,
		ResourceName:        "wibble",
		ResourcePatternType: acls.ResourcePatternTypePrefixed,
	}
	err := cl.CreateAcls([]acls.AclEntry{entry1, entry2, entry3, entry4})
	require.NoError(t, err)

	err = cl.DeleteAcls(acls.ResourceTypeAny, "foo", acls.ResourcePatternTypeAny, "", "", acls.OperationAny, acls.PermissionAny)
	require.NoError(t, err)
	entries := getAllAclsSortedByPrincipal(t, cl)
	require.Equal(t, []acls.AclEntry{entry2, entry4}, entries)
	err = cl.CreateAcls([]acls.AclEntry{entry1, entry3})
	require.NoError(t, err)

	err = cl.DeleteAcls(acls.ResourceTypeAny, "wibble", acls.ResourcePatternTypeAny, "", "", acls.OperationAny, acls.PermissionAny)
	require.NoError(t, err)
	entries = getAllAclsSortedByPrincipal(t, cl)
	require.Equal(t, []acls.AclEntry{entry1, entry3}, entries)
}

func testDeleteMatchingAcls(t *testing.T, cl Client, _ *Controller) {
	entry1 := acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "foo",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	entry2 := acls.AclEntry{
		Principal:           "User:bob",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "foo",
		ResourcePatternType: acls.ResourcePatternTypePrefixed,
	}
	entry3 := acls.AclEntry{
		Principal:           "User:joe",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeCluster,
		ResourceName:        "bar",
		ResourcePatternType: acls.ResourcePatternTypePrefixed,
	}
	entry4 := acls.AclEntry{
		Principal:           "User:alice",
		Permission:          acls.PermissionAllow,
		Operation:           acls.OperationAll,
		Host:                "*",
		ResourceType:        acls.ResourceTypeTopic,
		ResourceName:        "*",
		ResourcePatternType: acls.ResourcePatternTypeLiteral,
	}
	err := cl.CreateAcls([]acls.AclEntry{entry1, entry2, entry3, entry4})
	require.NoError(t, err)

	// only delete wildcard
	err = cl.DeleteAcls(acls.ResourceTypeAny, "unknown", acls.ResourcePatternTypeMatch, "", "", acls.OperationAny, acls.PermissionAny)
	require.NoError(t, err)
	entries := getAllAclsSortedByPrincipal(t, cl)
	require.Equal(t, []acls.AclEntry{entry1, entry2, entry3}, entries)
	err = cl.CreateAcls([]acls.AclEntry{entry4})
	require.NoError(t, err)

	// delete acls which match topic which has prefix
	err = cl.DeleteAcls(acls.ResourceTypeAny, "foo.topic1", acls.ResourcePatternTypeMatch, "", "", acls.OperationAny, acls.PermissionAny)
	require.NoError(t, err)
	entries = getAllAclsSortedByPrincipal(t, cl)
	require.Equal(t, []acls.AclEntry{entry1, entry3}, entries)
	err = cl.CreateAcls([]acls.AclEntry{entry2, entry4})
	require.NoError(t, err)

	err = cl.DeleteAcls(acls.ResourceTypeAny, "bar.topic1", acls.ResourcePatternTypeMatch, "", "", acls.OperationAny, acls.PermissionAny)
	require.NoError(t, err)
	entries = getAllAclsSortedByPrincipal(t, cl)
	require.Equal(t, []acls.AclEntry{entry1, entry2}, entries)
	err = cl.CreateAcls([]acls.AclEntry{entry3, entry4})
	require.NoError(t, err)

	err = cl.DeleteAcls(acls.ResourceTypeAny, "foo", acls.ResourcePatternTypeMatch, "", "", acls.OperationAny, acls.PermissionAny)
	require.NoError(t, err)
	entries = getAllAclsSortedByPrincipal(t, cl)
	require.Equal(t, []acls.AclEntry{entry3}, entries)
}

func getAllAclsSortedByPrincipal(t *testing.T, cl Client) []acls.AclEntry {
	entries, err := cl.ListAcls(acls.ResourceTypeAny, "", acls.ResourcePatternTypeAny, "", "", acls.OperationAny, acls.PermissionAny)
	require.NoError(t, err)
	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].Principal < entries[j].Principal
	})
	return entries
}

type tcHolder struct {
	testName string
	testCase func(t *testing.T, cl Client, controller *Controller)
}

var aclTestCases = []tcHolder{
	{testName: "testAuthoriseSpecificOperation", testCase: testAuthoriseSpecificOperation},
	{testName: "testAuthoriseAclWithOperationAny", testCase: testAuthoriseAclWithOperationAll},
	{testName: "testAuthoriseAclWithOperationAnyAndSingleDeny", testCase: testAuthoriseAclWithOperationAllAndSingleDeny},
	{testName: "testAuthoriseIndividualAndDenyAll", testCase: testAuthoriseIndividualAndDenyAll},
	{testName: "testAuthorisePrincipalName", testCase: testAuthorisePrincipalName},
	{testName: "testAuthoriseHost", testCase: testAuthoriseHost},
	{testName: "testDenyHost", testCase: testDenyHost},
	{testName: "testAuthoriseResourceType", testCase: testAuthoriseResourceType},
	{testName: "testAuthoriseLiteralResource", testCase: testAuthoriseLiteralResource},
	{testName: "testAuthorisePrefixed", testCase: testAuthorisePrefixed},
	{testName: "testAuthoriseMultiple", testCase: testAuthoriseMultiple},
	{testName: "testCreateSameAclMoreThanOnce", testCase: testCreateSameAclMoreThanOnce},
	{testName: "testValidateStoredAclEntry", testCase: testValidateStoredAclEntry},
	{testName: "testListAllAcls", testCase: testListAllAcls},
	{testName: "testListAllTopicAcls", testCase: testListAllTopicAcls},
	{testName: "testListAclsForTopic", testCase: testListAclsForTopic},
	{testName: "testDeleteLiteralAclsSpecifically", testCase: testDeleteLiteralAclsSpecifically},
	{testName: "testDeletePrefixedAclsSpecifically", testCase: testDeletePrefixedAclsSpecifically},
	{testName: "testDeleteAllAclsOfResourceType", testCase: testDeleteAllAclsOfResourceType},
	{testName: "testDeleteAllAcls", testCase: testDeleteAllAcls},
	{testName: "testDeleteAllAclsWithName", testCase: testDeleteAllAclsWithName},
	{testName: "testDeleteMatchingAcls", testCase: testDeleteMatchingAcls},
}

func setupControllerForAclTest(t *testing.T, objStore objstore.Client) (*Controller, *fakePusherSink, func(t *testing.T)) {
	// For ACL test we need to use socket transport not local transport as we need client host to be a real ip address
	sockClient, err := transport.NewSocketClient(nil)
	require.NoError(t, err)
	address, err := common.AddressWithPort("localhost")
	require.NoError(t, err)
	transportServer := transport.NewSocketTransportServer(address, conf.TlsConf{})
	require.NoError(t, err)
	err = transportServer.Start()
	require.NoError(t, err)
	cfg := NewConf()
	// Set to a high number as we don't have compaction running and don't want to block L0 adds
	cfg.LsmConf.L0MaxTablesBeforeBlocking = 10000
	connCaches := transport.NewConnCaches(10, sockClient.CreateConnection)
	ctrl := NewController(cfg, objStore, connCaches, sockClient.CreateConnection, transportServer)
	err = ctrl.Start()
	require.NoError(t, err)
	transportServer.RegisterHandler(transport.HandlerIDMetaLocalCacheTopicAdded,
		func(ctx *transport.ConnectionContext, request []byte, responseBuff []byte,
			responseWriter transport.ResponseWriter) error {
			return responseWriter(responseBuff, nil)
		})
	transportServer.RegisterHandler(transport.HandlerIDMetaLocalCacheTopicDeleted,
		func(ctx *transport.ConnectionContext, request []byte, responseBuff []byte,
			responseWriter transport.ResponseWriter) error {
			return responseWriter(responseBuff, nil)
		})
	fp := addFakePusherSinkToController(t, ctrl, objStore)
	return ctrl, fp, func(t *testing.T) {
		err := transportServer.Stop()
		require.NoError(t, err)
		err = ctrl.Stop()
		require.NoError(t, err)
	}
}

func deleteAllAcls(t *testing.T, cl Client, controller *Controller) {
	log.Infof("deleting all")
	err := cl.DeleteAcls(acls.ResourceTypeAny, "", acls.ResourcePatternTypeAny, "", "",
		acls.OperationAny, acls.PermissionAny)
	require.NoError(t, err)
	require.Equal(t, 0, controller.aclManager.AclCount())
}

func TestAclsStoredCorrectly(t *testing.T) {

	objStore := dev.NewInMemStore(0)

	controller, fp, tearDown := setupControllerWithPusherSink(t, objStore)
	defer tearDown(t)

	updateMembership(t, 1, 1, []*Controller{controller}, 0)

	cl, err := controller.Client()
	require.NoError(t, err)

	numAcls := 10
	numUsers := 5
	var entries []acls.AclEntry
	for i := 0; i < numAcls; i++ {
		entry := acls.AclEntry{
			Principal:           fmt.Sprintf("User:user-%d", i%numUsers),
			Permission:          acls.Permission(i%2 + 2),
			Operation:           acls.Operation(i%11 + 2),
			Host:                generateFakeIPAddress(),
			ResourceType:        acls.ResourceType(i%5 + 2),
			ResourceName:        fmt.Sprintf("resource-%05d", i),
			ResourcePatternType: acls.ResourcePatternType(i%2 + 3),
		}
		entries = append(entries, entry)
	}

	err = cl.CreateAcls(entries)
	require.NoError(t, err)
	listed, err := cl.ListAcls(acls.ResourceTypeAny, "", acls.ResourcePatternTypeAny, "",
		"", acls.OperationAny, acls.PermissionAny)
	require.NoError(t, err)
	sort.SliceStable(listed, func(i, j int) bool {
		return listed[i].ResourceName < listed[j].ResourceName
	})
	require.Equal(t, entries, listed)

	// check KVs created correctly
	kvs := fp.getAllKvs()
	require.Equal(t, numAcls, len(kvs))
	partHash, err := parthash.CreateHash([]byte("acls"))
	require.NoError(t, err)
	for i, entry := range entries {
		expectedKey := common.ByteSliceCopy(partHash)
		expectedKey = encoding.KeyEncodeInt(expectedKey, int64(i))
		expectedKey = encoding.EncodeVersion(expectedKey, 0)
		expectedValue := binary.BigEndian.AppendUint16(nil, aclDataVersion)
		expectedValue = entry.Serialize(expectedValue)
		expectedValue = common.AppendValueMetadata(expectedValue)
		require.Equal(t, expectedKey, kvs[i].Key)
		require.Equal(t, expectedValue, kvs[i].Value)
	}

	createAndRegisterTableWithKVs(t, kvs, objStore, "tektite-data", controller.lsmHolder)

	// Now restart
	err = cl.Close()
	require.NoError(t, err)
	tearDown(t)
	controller, fp, tearDown = setupControllerWithPusherSink(t, objStore)
	defer tearDown(t)
	updateMembership(t, 1, 1, []*Controller{controller}, 0)
	cl, err = controller.Client()
	require.NoError(t, err)

	// should still be there
	listed, err = cl.ListAcls(acls.ResourceTypeAny, "", acls.ResourcePatternTypeAny, "",
		"", acls.OperationAny, acls.PermissionAny)
	require.NoError(t, err)
	sort.SliceStable(listed, func(i, j int) bool {
		return listed[i].ResourceName < listed[j].ResourceName
	})
	require.Equal(t, entries, listed)

	// Now delete every other one
	for i, entry := range entries {
		if i%2 == 0 {
			err = cl.DeleteAcls(acls.ResourceTypeAny, entry.ResourceName, acls.ResourcePatternTypeAny, "",
				"", acls.OperationAny, acls.PermissionAny)
			require.NoError(t, err)
		}
	}

	kvs = fp.getAllKvs()

	require.Equal(t, numAcls/2, len(kvs))
	for i, kv := range kvs {
		expectedKey := common.ByteSliceCopy(partHash)
		expectedKey = encoding.KeyEncodeInt(expectedKey, int64(i*2))
		expectedKey = encoding.EncodeVersion(expectedKey, 0)
		require.Equal(t, expectedKey, kv.Key)
		require.Equal(t, 0, len(kv.Value))
	}
}

func generateFakeIPAddress() string {
	return fmt.Sprintf("%d.%d.%d.%d", rand.Intn(256), rand.Intn(256), rand.Intn(256), rand.Intn(256))
}
