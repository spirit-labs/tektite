package auth

import (
	"fmt"
	"github.com/spirit-labs/tektite/acls"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestAuthCache(t *testing.T) {
	principal := "User:bob"
	cc := &testControlClient{
		allow: true,
	}
	timeout := 100 * time.Millisecond
	authCache := NewUserAuthCache(principal, func() (ControlClient, error) {
		return cc, nil
	}, timeout)

	allow, err := authCache.Authorize(acls.ResourceTypeGroup, "test-group-1", acls.OperationWrite)
	require.NoError(t, err)
	require.True(t, allow)

	require.Equal(t, 1, len(cc.requests))
	req := cc.requests[0]
	require.Equal(t, principal, req.principal)
	require.Equal(t, acls.ResourceTypeGroup, req.resourceType)
	require.Equal(t, "test-group-1", req.resourceName)
	require.Equal(t, acls.OperationWrite, req.operation)

	// Auth should be cached
	for i := 0; i < 10; i++ {
		allow, err := authCache.Authorize(acls.ResourceTypeGroup, "test-group-1", acls.OperationWrite)
		require.NoError(t, err)
		require.True(t, allow)
		require.Equal(t, 1, len(cc.requests))
	}

	// Wait for expiry
	cc.allow = false
	time.Sleep(timeout)

	allow, err = authCache.Authorize(acls.ResourceTypeGroup, "test-group-1", acls.OperationWrite)
	require.NoError(t, err)
	require.False(t, allow)

	require.Equal(t, 2, len(cc.requests))
	require.Equal(t, cc.requests[0], cc.requests[1])

	// Make sure deny is cached too
	for i := 0; i < 10; i++ {
		allow, err := authCache.Authorize(acls.ResourceTypeGroup, "test-group-1", acls.OperationWrite)
		require.NoError(t, err)
		require.False(t, allow)
		require.Equal(t, 2, len(cc.requests))
	}

	// Wait for expiry again
	cc.allow = true
	time.Sleep(timeout)

	allow, err = authCache.Authorize(acls.ResourceTypeGroup, "test-group-1", acls.OperationWrite)
	require.NoError(t, err)
	require.True(t, allow)

	require.Equal(t, 3, len(cc.requests))
	require.Equal(t, cc.requests[0], cc.requests[1])
	require.Equal(t, cc.requests[1], cc.requests[2])
}

func TestCheckExpired(t *testing.T) {
	principal := "User:bob"
	cc := &testControlClient{
		allow: true,
	}
	timeout := 100 * time.Millisecond
	authCache := NewUserAuthCache(principal, func() (ControlClient, error) {
		return cc, nil
	}, timeout)

	for rt := acls.ResourceTypeTopic; rt <= acls.ResourceTypeDelegationToken; rt++ {
		for i := 0; i < 10; i++ {
			resourceName := fmt.Sprintf("resource-%d", i)
			for op := acls.OperationRead; op <= acls.OperationIdempotentWrite; op++ {
				allow, err := authCache.Authorize(rt, resourceName, op)
				require.NoError(t, err)
				require.True(t, allow)
			}
		}
	}
	require.Equal(t, int(1+acls.ResourceTypeDelegationToken-acls.ResourceTypeTopic), len(authCache.authorisations))
	time.Sleep(timeout)
	authCache.CheckExpired()
	require.Equal(t, 0, len(authCache.authorisations))
}

type testControlClient struct {
	requests []authRequest
	allow    bool
}

func (t *testControlClient) Authorise(principal string, resourceType acls.ResourceType, resourceName string, operation acls.Operation) (bool, error) {
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
	resourceType acls.ResourceType
	resourceName string
	operation    acls.Operation
}
