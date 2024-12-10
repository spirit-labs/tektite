// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"
import "unsafe"

type DeleteAclsRequestDeleteAclsFilter struct {
    // The resource type.
    ResourceTypeFilter int8
    // The resource name.
    ResourceNameFilter *string
    // The pattern type.
    PatternTypeFilter int8
    // The principal filter, or null to accept all principals.
    PrincipalFilter *string
    // The host filter, or null to accept all hosts.
    HostFilter *string
    // The ACL operation.
    Operation int8
    // The permission type.
    PermissionType int8
}

type DeleteAclsRequest struct {
    // The filters to use when deleting ACLs.
    Filters []DeleteAclsRequestDeleteAclsFilter
}

func (m *DeleteAclsRequest) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.Filters: The filters to use when deleting ACLs.
        var l0 int
        if version >= 2 {
            // flexible and not nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l0 = int(u - 1)
        } else {
            // non flexible and non nullable
            l0 = int(binary.BigEndian.Uint32(buff[offset:]))
            offset += 4
        }
        if l0 >= 0 {
            // length will be -1 if field is null
            filters := make([]DeleteAclsRequestDeleteAclsFilter, l0)
            for i0 := 0; i0 < l0; i0++ {
                // reading non tagged fields
                {
                    // reading filters[i0].ResourceTypeFilter: The resource type.
                    filters[i0].ResourceTypeFilter = int8(buff[offset])
                    offset++
                }
                {
                    // reading filters[i0].ResourceNameFilter: The resource name.
                    if version >= 2 {
                        // flexible and nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l1 := int(u - 1)
                        if l1 > 0 {
                            s := string(buff[offset: offset + l1])
                            filters[i0].ResourceNameFilter = &s
                            offset += l1
                        } else {
                            filters[i0].ResourceNameFilter = nil
                        }
                    } else {
                        // non flexible and nullable
                        var l1 int
                        l1 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
                        offset += 2
                        if l1 > 0 {
                            s := string(buff[offset: offset + l1])
                            filters[i0].ResourceNameFilter = &s
                            offset += l1
                        } else {
                            filters[i0].ResourceNameFilter = nil
                        }
                    }
                }
                if version >= 1 {
                    {
                        // reading filters[i0].PatternTypeFilter: The pattern type.
                        filters[i0].PatternTypeFilter = int8(buff[offset])
                        offset++
                    }
                }
                {
                    // reading filters[i0].PrincipalFilter: The principal filter, or null to accept all principals.
                    if version >= 2 {
                        // flexible and nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l2 := int(u - 1)
                        if l2 > 0 {
                            s := string(buff[offset: offset + l2])
                            filters[i0].PrincipalFilter = &s
                            offset += l2
                        } else {
                            filters[i0].PrincipalFilter = nil
                        }
                    } else {
                        // non flexible and nullable
                        var l2 int
                        l2 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
                        offset += 2
                        if l2 > 0 {
                            s := string(buff[offset: offset + l2])
                            filters[i0].PrincipalFilter = &s
                            offset += l2
                        } else {
                            filters[i0].PrincipalFilter = nil
                        }
                    }
                }
                {
                    // reading filters[i0].HostFilter: The host filter, or null to accept all hosts.
                    if version >= 2 {
                        // flexible and nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l3 := int(u - 1)
                        if l3 > 0 {
                            s := string(buff[offset: offset + l3])
                            filters[i0].HostFilter = &s
                            offset += l3
                        } else {
                            filters[i0].HostFilter = nil
                        }
                    } else {
                        // non flexible and nullable
                        var l3 int
                        l3 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
                        offset += 2
                        if l3 > 0 {
                            s := string(buff[offset: offset + l3])
                            filters[i0].HostFilter = &s
                            offset += l3
                        } else {
                            filters[i0].HostFilter = nil
                        }
                    }
                }
                {
                    // reading filters[i0].Operation: The ACL operation.
                    filters[i0].Operation = int8(buff[offset])
                    offset++
                }
                {
                    // reading filters[i0].PermissionType: The permission type.
                    filters[i0].PermissionType = int8(buff[offset])
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
            }
        m.Filters = filters
        }
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

func (m *DeleteAclsRequest) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.Filters: The filters to use when deleting ACLs.
    if version >= 2 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.Filters) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.Filters)))
    }
    for _, filters := range m.Filters {
        // writing non tagged fields
        // writing filters.ResourceTypeFilter: The resource type.
        buff = append(buff, byte(filters.ResourceTypeFilter))
        // writing filters.ResourceNameFilter: The resource name.
        if version >= 2 {
            // flexible and nullable
            if filters.ResourceNameFilter == nil {
                // null
                buff = append(buff, 0)
            } else {
                // not null
                buff = binary.AppendUvarint(buff, uint64(len(*filters.ResourceNameFilter) + 1))
            }
        } else {
            // non flexible and nullable
            if filters.ResourceNameFilter == nil {
                // null
                buff = binary.BigEndian.AppendUint16(buff, 65535)
            } else {
                // not null
                buff = binary.BigEndian.AppendUint16(buff, uint16(len(*filters.ResourceNameFilter)))
            }
        }
        if filters.ResourceNameFilter != nil {
            buff = append(buff, *filters.ResourceNameFilter...)
        }
        if version >= 1 {
            // writing filters.PatternTypeFilter: The pattern type.
            buff = append(buff, byte(filters.PatternTypeFilter))
        }
        // writing filters.PrincipalFilter: The principal filter, or null to accept all principals.
        if version >= 2 {
            // flexible and nullable
            if filters.PrincipalFilter == nil {
                // null
                buff = append(buff, 0)
            } else {
                // not null
                buff = binary.AppendUvarint(buff, uint64(len(*filters.PrincipalFilter) + 1))
            }
        } else {
            // non flexible and nullable
            if filters.PrincipalFilter == nil {
                // null
                buff = binary.BigEndian.AppendUint16(buff, 65535)
            } else {
                // not null
                buff = binary.BigEndian.AppendUint16(buff, uint16(len(*filters.PrincipalFilter)))
            }
        }
        if filters.PrincipalFilter != nil {
            buff = append(buff, *filters.PrincipalFilter...)
        }
        // writing filters.HostFilter: The host filter, or null to accept all hosts.
        if version >= 2 {
            // flexible and nullable
            if filters.HostFilter == nil {
                // null
                buff = append(buff, 0)
            } else {
                // not null
                buff = binary.AppendUvarint(buff, uint64(len(*filters.HostFilter) + 1))
            }
        } else {
            // non flexible and nullable
            if filters.HostFilter == nil {
                // null
                buff = binary.BigEndian.AppendUint16(buff, 65535)
            } else {
                // not null
                buff = binary.BigEndian.AppendUint16(buff, uint16(len(*filters.HostFilter)))
            }
        }
        if filters.HostFilter != nil {
            buff = append(buff, *filters.HostFilter...)
        }
        // writing filters.Operation: The ACL operation.
        buff = append(buff, byte(filters.Operation))
        // writing filters.PermissionType: The permission type.
        buff = append(buff, byte(filters.PermissionType))
        if version >= 2 {
            numTaggedFields8 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields8))
        }
    }
    if version >= 2 {
        numTaggedFields9 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields9))
    }
    return buff
}

func (m *DeleteAclsRequest) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.Filters: The filters to use when deleting ACLs.
    if version >= 2 {
        // flexible and not nullable
        size += sizeofUvarint(len(m.Filters) + 1)
    } else {
        // non flexible and non nullable
        size += 4
    }
    for _, filters := range m.Filters {
        size += 0 * int(unsafe.Sizeof(filters)) // hack to make sure loop variable is always used
        // calculating size for non tagged fields
        numTaggedFields1:= 0
        numTaggedFields1 += 0
        // size for filters.ResourceTypeFilter: The resource type.
        size += 1
        // size for filters.ResourceNameFilter: The resource name.
        if version >= 2 {
            // flexible and nullable
            if filters.ResourceNameFilter == nil {
                // null
                size += 1
            } else {
                // not null
                size += sizeofUvarint(len(*filters.ResourceNameFilter) + 1)
            }
        } else {
            // non flexible and nullable
            size += 2
        }
        if filters.ResourceNameFilter != nil {
            size += len(*filters.ResourceNameFilter)
        }
        if version >= 1 {
            // size for filters.PatternTypeFilter: The pattern type.
            size += 1
        }
        // size for filters.PrincipalFilter: The principal filter, or null to accept all principals.
        if version >= 2 {
            // flexible and nullable
            if filters.PrincipalFilter == nil {
                // null
                size += 1
            } else {
                // not null
                size += sizeofUvarint(len(*filters.PrincipalFilter) + 1)
            }
        } else {
            // non flexible and nullable
            size += 2
        }
        if filters.PrincipalFilter != nil {
            size += len(*filters.PrincipalFilter)
        }
        // size for filters.HostFilter: The host filter, or null to accept all hosts.
        if version >= 2 {
            // flexible and nullable
            if filters.HostFilter == nil {
                // null
                size += 1
            } else {
                // not null
                size += sizeofUvarint(len(*filters.HostFilter) + 1)
            }
        } else {
            // non flexible and nullable
            size += 2
        }
        if filters.HostFilter != nil {
            size += len(*filters.HostFilter)
        }
        // size for filters.Operation: The ACL operation.
        size += 1
        // size for filters.PermissionType: The permission type.
        size += 1
        numTaggedFields2:= 0
        numTaggedFields2 += 0
        if version >= 2 {
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields2)
        }
    }
    numTaggedFields3:= 0
    numTaggedFields3 += 0
    if version >= 2 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields3)
    }
    return size, tagSizes
}

func (m *DeleteAclsRequest) HeaderVersions(version int16) (int16, int16) {
    if version >= 2 {
        return 2, 1
    } else {
        return 1, 0
    }
}

func (m *DeleteAclsRequest) SupportedApiVersions() (int16, int16) {
    return 3, 3
}
