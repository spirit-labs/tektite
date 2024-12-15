package acls

import (
	"encoding/binary"
	"github.com/spirit-labs/tektite/asl/arista"
	"sync"
	"time"
)

type Authorizer interface {
	Authorize(principal string, resourceType ResourceType, resourceName string, operation Operation) bool
}

type ResourceType int8

const (
	ResourceTypeUnknown         = ResourceType(0)
	ResourceTypeAny             = ResourceType(1)
	ResourceTypeTopic           = ResourceType(2)
	ResourceTypeGroup           = ResourceType(3)
	ResourceTypeCluster         = ResourceType(4)
	ResourceTypeTransactionalID = ResourceType(5)
	ResourceTypeDelegationToken = ResourceType(6)
)

type Operation int8

const (
	OperationUnknown         = Operation(0)
	OperationAny             = Operation(1) // Only used in filters when --listing
	OperationAll             = Operation(2) // Only used in stored ACLs represents access to all operations
	OperationRead            = Operation(3)
	OperationWrite           = Operation(4)
	OperationCreate          = Operation(5)
	OperationDelete          = Operation(6)
	OperationAlter           = Operation(7)
	OperationDescribe        = Operation(8)
	OperationClusterAction   = Operation(9)
	OperationDescribeConfigs = Operation(10)
	OperationAlterConfigs    = Operation(11)
	OperationIdempotentWrite = Operation(12)
)

type Permission int8

const (
	PermissionUnknown = Permission(0)
	PermissionAny     = Permission(1)
	PermissionDeny    = Permission(2)
	PermissionAllow   =  Permission(3)
)

type ResourcePatternType int8

const (
	ResourcePatternTypeUnknown  = ResourcePatternType(0)
	ResourcePatternTypeAny      = ResourcePatternType(1) // ACL will match any resource name
	ResourcePatternTypeMatch    = ResourcePatternType(2) // Only used when listing/deleting - ACLS will match against literal, prefix or wildcard
	ResourcePatternTypeLiteral  = ResourcePatternType(3) // This is the default - acl is exact match on name
	ResourcePatternTypePrefixed = ResourcePatternType(4) // ACL resource name is a prefix - will match any resource which has this prefix
)

/*
AclEntry

From Kafka docs:

<<Kafka ACLs are defined in the general format of “Principal P is [Allowed/Denied] Operation O From Host H On Resources
matching ResourcePattern RP”.>>
*/
type AclEntry struct {
	Principal           string              // Principal <P>, (can be '*' too)
	Permission          Permission          // Is [allowed/denied]
	Operation           Operation           // <operation> (e.g. read/write/create/all)
	Host                string              // from <host>, (can be '*' too)
	ResourceType        ResourceType        // on <resource type> (e.g. group, cluster, topic, etc)
	ResourceName        string              // on <resource name> (can be an exact literal name (e.g. "topic1"), but can also be a prefix to, say, match all topics that start with that prefix
	ResourcePatternType ResourcePatternType // with <pattern type> - (can be literal, or prefix)
}

func (a *AclEntry) Serialize(buff []byte) []byte {
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(a.Principal)))
	buff = append(buff, a.Principal...)
	buff = append(buff, byte(a.ResourceType))
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(a.ResourceName)))
	buff = append(buff, a.ResourceName...)
	buff = append(buff, byte(a.ResourcePatternType))
	buff = binary.BigEndian.AppendUint32(buff, uint32(len(a.Host)))
	buff = append(buff, a.Host...)
	buff = append(buff, byte(a.Operation))
	return append(buff, byte(a.Permission))
}

func (a *AclEntry) Deserialize(buff []byte, offset int) int {
	l := int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	a.Principal = string(buff[offset : offset+l])
	offset += l
	a.ResourceType = ResourceType(buff[offset])
	offset++
	l = int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	a.ResourceName = string(buff[offset : offset+l])
	offset += l
	a.ResourcePatternType = ResourcePatternType(buff[offset])
	offset++
	l = int(binary.BigEndian.Uint32(buff[offset:]))
	offset += 4
	a.Host = string(buff[offset : offset+l])
	offset += l
	a.Operation = Operation(buff[offset])
	offset++
	a.Permission = Permission(buff[offset])
	offset++
	return offset
}

type ControlClient interface {
	Authorise(principal string, resourceType ResourceType, resourceName string, operation Operation) (bool, error)
}

type ControlClientFactory func() (ControlClient, error)

type UserAuthCache struct {
	lock                 sync.RWMutex
	principal            string
	authTimeout          time.Duration
	authorisations       map[int8]map[string][]ResourceAuthorization
	controlClientFactory ControlClientFactory
}

func NewUserAuthCache(principal string, controlClientFactory ControlClientFactory, authTimeout time.Duration) *UserAuthCache {
	return &UserAuthCache{
		principal:            principal,
		authorisations:       make(map[int8]map[string][]ResourceAuthorization),
		controlClientFactory: controlClientFactory,
		authTimeout:          authTimeout,
	}
}

type ResourceAuthorization struct {
	operation  Operation
	authorised bool
	authTime   uint64
}

func (u *UserAuthCache) Authorize(resourceType ResourceType, resourceName string, operation Operation) (bool, error) {
	now := arista.NanoTime()
	authorised, cached := u.authoriseFromCache(resourceType, resourceName, operation, now)
	if cached {
		return authorised, nil
	}
	u.lock.Lock()
	defer u.lock.Unlock()
	// authorise from cache again to avoid race between dropping rlock and getting wlock
	authorised, cached = u.authoriseFromCache0(resourceType, resourceName, operation, now)
	if cached {
		return authorised, nil
	}
	conn, err := u.controlClientFactory()
	if err != nil {
		return false, err
	}
	authorised, err = conn.Authorise(u.principal, resourceType, resourceName, operation)
	if err != nil {
		return false, err
	}
	resourceTypeMap, ok := u.authorisations[int8(resourceType)]
	if !ok {
		resourceTypeMap = map[string][]ResourceAuthorization{}
		u.authorisations[int8(resourceType)] = resourceTypeMap
	}
	authorisations := resourceTypeMap[resourceName]
	var authsPruned []ResourceAuthorization
	for _, auth := range authorisations {
		// Remove the auth if same operation as could be an expired one, and we don't want duplicates
		if auth.operation != operation {
			authsPruned = append(authsPruned, auth)
		}
	}
	authsPruned = append(authsPruned, ResourceAuthorization{
		operation:  operation,
		authorised: authorised,
		authTime:   arista.NanoTime(),
	})
	resourceTypeMap[resourceName] = authsPruned
	return authorised, nil
}

func (u *UserAuthCache) authoriseFromCache(resourceType ResourceType, resourceName string, operation Operation, now uint64) (authorised bool, cached bool) {
	u.lock.RLock()
	defer u.lock.RUnlock()
	return u.authoriseFromCache0(resourceType, resourceName, operation, now)
}

func (u *UserAuthCache) authoriseFromCache0(resourceType ResourceType, resourceName string, operation Operation, now uint64) (authorised bool, cached bool) {
	resourceTypeMap, ok := u.authorisations[int8(resourceType)]
	if ok {
		authorizations, ok := resourceTypeMap[resourceName]
		if ok {
			for _, auth := range authorizations {
				if auth.operation == operation && now-auth.authTime < uint64(u.authTimeout.Nanoseconds()) {
					return auth.authorised, true
				}
			}
		}
	}
	return false, false
}
