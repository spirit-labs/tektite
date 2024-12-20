package acls

import (
	"github.com/stretchr/testify/require"
	"testing"
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
