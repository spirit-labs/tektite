package acls

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestSerializeDeserializeAclEntry(t *testing.T) {
	aclE := AclEntry{
		Principal:           "joe bloggs",
		ResourceType:        ResourceTypeTopic,
		ResourceName:        "armadillo-topic",
		ResourcePatternType: ResourcePatternTypeLiteral,
		Host:                "some-host",
		Operation:           OperationCreate,
		Permission:          PermissionAllow,
	}
	var buff []byte
	buff = append(buff, 1, 2, 3)
	buff = aclE.Serialize(buff)
	var aclE2 AclEntry
	off := aclE2.Deserialize(buff, 3)
	require.Equal(t, aclE, aclE2)
	require.Equal(t, off, len(buff))
}

func TestAuthCache(t *testing.T) {
	principal := "User:bob"
	cc := &testControlClient{
		allow: true,
	}
	timeout := 100 * time.Millisecond
	authCache := NewUserAuthCache(principal, func() (ControlClient, error) {
		return cc, nil
	}, timeout)

	allow, err := authCache.Authorize(ResourceTypeGroup, "test-group-1", OperationWrite)
	require.NoError(t, err)
	require.True(t, allow)

	require.Equal(t, 1, len(cc.requests))
	req := cc.requests[0]
	require.Equal(t, principal, req.principal)
	require.Equal(t, ResourceTypeGroup, req.resourceType)
	require.Equal(t, "test-group-1", req.resourceName)
	require.Equal(t, OperationWrite, req.operation)

	// Auth should be cached
	for i := 0; i < 10; i++ {
		allow, err := authCache.Authorize(ResourceTypeGroup, "test-group-1", OperationWrite)
		require.NoError(t, err)
		require.True(t, allow)
		require.Equal(t, 1, len(cc.requests))
	}

	// Wait for expiry
	cc.allow = false
	time.Sleep(timeout)

	allow, err = authCache.Authorize(ResourceTypeGroup, "test-group-1", OperationWrite)
	require.NoError(t, err)
	require.False(t, allow)

	require.Equal(t, 2, len(cc.requests))
	require.Equal(t, cc.requests[0], cc.requests[1])

	// Make sure deny is cached too
	for i := 0; i < 10; i++ {
		allow, err := authCache.Authorize(ResourceTypeGroup, "test-group-1", OperationWrite)
		require.NoError(t, err)
		require.False(t, allow)
		require.Equal(t, 2, len(cc.requests))
	}

	// Wait for expiry again
	cc.allow = true
	time.Sleep(timeout)

	allow, err = authCache.Authorize(ResourceTypeGroup, "test-group-1", OperationWrite)
	require.NoError(t, err)
	require.True(t, allow)

	require.Equal(t, 3, len(cc.requests))
	require.Equal(t, cc.requests[0], cc.requests[1])
	require.Equal(t, cc.requests[1], cc.requests[2])
}

type testControlClient struct {
	requests []authRequest
	allow    bool
}

func (t *testControlClient) Authorise(principal string, resourceType ResourceType, resourceName string, operation Operation) (bool, error) {
	t.requests = append(t.requests, authRequest{
		principal:    principal,
		resourceType: resourceType,
		resourceName: resourceName,
		operation:    operation,
	})
	return t.allow, nil
}

type authRequest struct {
	principal    string
	resourceType ResourceType
	resourceName string
	operation    Operation
}
