// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"

type DescribeAclsRequest struct {
    // The resource type.
    ResourceTypeFilter int8
    // The resource name, or null to match any resource name.
    ResourceNameFilter *string
    // The resource pattern to match.
    PatternTypeFilter int8
    // The principal to match, or null to match any principal.
    PrincipalFilter *string
    // The host to match, or null to match any host.
    HostFilter *string
    // The operation to match.
    Operation int8
    // The permission type to match.
    PermissionType int8
}

func (m *DescribeAclsRequest) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.ResourceTypeFilter: The resource type.
        m.ResourceTypeFilter = int8(buff[offset])
        offset++
    }
    {
        // reading m.ResourceNameFilter: The resource name, or null to match any resource name.
        if version >= 2 {
            // flexible and nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l0 := int(u - 1)
            if l0 > 0 {
                s := string(buff[offset: offset + l0])
                m.ResourceNameFilter = &s
                offset += l0
            } else {
                m.ResourceNameFilter = nil
            }
        } else {
            // non flexible and nullable
            var l0 int
            l0 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
            offset += 2
            if l0 > 0 {
                s := string(buff[offset: offset + l0])
                m.ResourceNameFilter = &s
                offset += l0
            } else {
                m.ResourceNameFilter = nil
            }
        }
    }
    if version >= 1 {
        {
            // reading m.PatternTypeFilter: The resource pattern to match.
            m.PatternTypeFilter = int8(buff[offset])
            offset++
        }
    }
    {
        // reading m.PrincipalFilter: The principal to match, or null to match any principal.
        if version >= 2 {
            // flexible and nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l1 := int(u - 1)
            if l1 > 0 {
                s := string(buff[offset: offset + l1])
                m.PrincipalFilter = &s
                offset += l1
            } else {
                m.PrincipalFilter = nil
            }
        } else {
            // non flexible and nullable
            var l1 int
            l1 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
            offset += 2
            if l1 > 0 {
                s := string(buff[offset: offset + l1])
                m.PrincipalFilter = &s
                offset += l1
            } else {
                m.PrincipalFilter = nil
            }
        }
    }
    {
        // reading m.HostFilter: The host to match, or null to match any host.
        if version >= 2 {
            // flexible and nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l2 := int(u - 1)
            if l2 > 0 {
                s := string(buff[offset: offset + l2])
                m.HostFilter = &s
                offset += l2
            } else {
                m.HostFilter = nil
            }
        } else {
            // non flexible and nullable
            var l2 int
            l2 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
            offset += 2
            if l2 > 0 {
                s := string(buff[offset: offset + l2])
                m.HostFilter = &s
                offset += l2
            } else {
                m.HostFilter = nil
            }
        }
    }
    {
        // reading m.Operation: The operation to match.
        m.Operation = int8(buff[offset])
        offset++
    }
    {
        // reading m.PermissionType: The permission type to match.
        m.PermissionType = int8(buff[offset])
        offset++
    }
    if version >= 2 {
        // reading tagged fields
        nt, n := binary.Uvarint(buff[offset:])
        offset += n
        for i := 0; i < int(nt); i++ {
            t, n := binary.Uvarint(buff[offset:])
            offset += n
            ts, n := binary.Uvarint(buff[offset:])
            offset += n
            switch t {
                default:
                    offset += int(ts)
            }
        }
    }
    return offset, nil
}

func (m *DescribeAclsRequest) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.ResourceTypeFilter: The resource type.
    buff = append(buff, byte(m.ResourceTypeFilter))
    // writing m.ResourceNameFilter: The resource name, or null to match any resource name.
    if version >= 2 {
        // flexible and nullable
        if m.ResourceNameFilter == nil {
            // null
            buff = append(buff, 0)
        } else {
            // not null
            buff = binary.AppendUvarint(buff, uint64(len(*m.ResourceNameFilter) + 1))
        }
    } else {
        // non flexible and nullable
        if m.ResourceNameFilter == nil {
            // null
            buff = binary.BigEndian.AppendUint16(buff, 65535)
        } else {
            // not null
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.ResourceNameFilter)))
        }
    }
    if m.ResourceNameFilter != nil {
        buff = append(buff, *m.ResourceNameFilter...)
    }
    if version >= 1 {
        // writing m.PatternTypeFilter: The resource pattern to match.
        buff = append(buff, byte(m.PatternTypeFilter))
    }
    // writing m.PrincipalFilter: The principal to match, or null to match any principal.
    if version >= 2 {
        // flexible and nullable
        if m.PrincipalFilter == nil {
            // null
            buff = append(buff, 0)
        } else {
            // not null
            buff = binary.AppendUvarint(buff, uint64(len(*m.PrincipalFilter) + 1))
        }
    } else {
        // non flexible and nullable
        if m.PrincipalFilter == nil {
            // null
            buff = binary.BigEndian.AppendUint16(buff, 65535)
        } else {
            // not null
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.PrincipalFilter)))
        }
    }
    if m.PrincipalFilter != nil {
        buff = append(buff, *m.PrincipalFilter...)
    }
    // writing m.HostFilter: The host to match, or null to match any host.
    if version >= 2 {
        // flexible and nullable
        if m.HostFilter == nil {
            // null
            buff = append(buff, 0)
        } else {
            // not null
            buff = binary.AppendUvarint(buff, uint64(len(*m.HostFilter) + 1))
        }
    } else {
        // non flexible and nullable
        if m.HostFilter == nil {
            // null
            buff = binary.BigEndian.AppendUint16(buff, 65535)
        } else {
            // not null
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.HostFilter)))
        }
    }
    if m.HostFilter != nil {
        buff = append(buff, *m.HostFilter...)
    }
    // writing m.Operation: The operation to match.
    buff = append(buff, byte(m.Operation))
    // writing m.PermissionType: The permission type to match.
    buff = append(buff, byte(m.PermissionType))
    if version >= 2 {
        numTaggedFields7 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields7))
    }
    return buff
}

func (m *DescribeAclsRequest) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.ResourceTypeFilter: The resource type.
    size += 1
    // size for m.ResourceNameFilter: The resource name, or null to match any resource name.
    if version >= 2 {
        // flexible and nullable
        if m.ResourceNameFilter == nil {
            // null
            size += 1
        } else {
            // not null
            size += sizeofUvarint(len(*m.ResourceNameFilter) + 1)
        }
    } else {
        // non flexible and nullable
        size += 2
    }
    if m.ResourceNameFilter != nil {
        size += len(*m.ResourceNameFilter)
    }
    if version >= 1 {
        // size for m.PatternTypeFilter: The resource pattern to match.
        size += 1
    }
    // size for m.PrincipalFilter: The principal to match, or null to match any principal.
    if version >= 2 {
        // flexible and nullable
        if m.PrincipalFilter == nil {
            // null
            size += 1
        } else {
            // not null
            size += sizeofUvarint(len(*m.PrincipalFilter) + 1)
        }
    } else {
        // non flexible and nullable
        size += 2
    }
    if m.PrincipalFilter != nil {
        size += len(*m.PrincipalFilter)
    }
    // size for m.HostFilter: The host to match, or null to match any host.
    if version >= 2 {
        // flexible and nullable
        if m.HostFilter == nil {
            // null
            size += 1
        } else {
            // not null
            size += sizeofUvarint(len(*m.HostFilter) + 1)
        }
    } else {
        // non flexible and nullable
        size += 2
    }
    if m.HostFilter != nil {
        size += len(*m.HostFilter)
    }
    // size for m.Operation: The operation to match.
    size += 1
    // size for m.PermissionType: The permission type to match.
    size += 1
    numTaggedFields1:= 0
    numTaggedFields1 += 0
    if version >= 2 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields1)
    }
    return size, tagSizes
}

func (m *DescribeAclsRequest) HeaderVersions(version int16) (int16, int16) {
    if version >= 2 {
        return 2, 1
    } else {
        return 1, 0
    }
}

func (m *DescribeAclsRequest) SupportedApiVersions() (int16, int16) {
    return 3, 3
}
