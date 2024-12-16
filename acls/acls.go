package acls

import (
	"encoding/binary"
)

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

const ClusterResourceName = "kafka-cluster"

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
	PermissionAllow   = Permission(3)
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
