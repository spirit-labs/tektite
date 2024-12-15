// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"
import "unsafe"

type DescribeAclsResponseAclDescription struct {
    // The ACL principal.
    Principal *string
    // The ACL host.
    Host *string
    // The ACL operation.
    Operation int8
    // The ACL permission type.
    PermissionType int8
}

type DescribeAclsResponseDescribeAclsResource struct {
    // The resource type.
    ResourceType int8
    // The resource name.
    ResourceName *string
    // The resource pattern type.
    PatternType int8
    // The ACLs.
    Acls []DescribeAclsResponseAclDescription
}

type DescribeAclsResponse struct {
    // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ThrottleTimeMs int32
    // The error code, or 0 if there was no error.
    ErrorCode int16
    // The error message, or null if there was no error.
    ErrorMessage *string
    // Each Resource that is referenced in an ACL.
    Resources []DescribeAclsResponseDescribeAclsResource
}

func (m *DescribeAclsResponse) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        m.ThrottleTimeMs = int32(binary.BigEndian.Uint32(buff[offset:]))
        offset += 4
    }
    {
        // reading m.ErrorCode: The error code, or 0 if there was no error.
        m.ErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
        offset += 2
    }
    {
        // reading m.ErrorMessage: The error message, or null if there was no error.
        if version >= 2 {
            // flexible and nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l0 := int(u - 1)
            if l0 > 0 {
                s := string(buff[offset: offset + l0])
                m.ErrorMessage = &s
                offset += l0
            } else {
                m.ErrorMessage = nil
            }
        } else {
            // non flexible and nullable
            var l0 int
            l0 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
            offset += 2
            if l0 > 0 {
                s := string(buff[offset: offset + l0])
                m.ErrorMessage = &s
                offset += l0
            } else {
                m.ErrorMessage = nil
            }
        }
    }
    {
        // reading m.Resources: Each Resource that is referenced in an ACL.
        var l1 int
        if version >= 2 {
            // flexible and not nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l1 = int(u - 1)
        } else {
            // non flexible and non nullable
            l1 = int(binary.BigEndian.Uint32(buff[offset:]))
            offset += 4
        }
        if l1 >= 0 {
            // length will be -1 if field is null
            resources := make([]DescribeAclsResponseDescribeAclsResource, l1)
            for i0 := 0; i0 < l1; i0++ {
                // reading non tagged fields
                {
                    // reading resources[i0].ResourceType: The resource type.
                    resources[i0].ResourceType = int8(buff[offset])
                    offset++
                }
                {
                    // reading resources[i0].ResourceName: The resource name.
                    if version >= 2 {
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l2 := int(u - 1)
                        s := string(buff[offset: offset + l2])
                        resources[i0].ResourceName = &s
                        offset += l2
                    } else {
                        // non flexible and non nullable
                        var l2 int
                        l2 = int(binary.BigEndian.Uint16(buff[offset:]))
                        offset += 2
                        s := string(buff[offset: offset + l2])
                        resources[i0].ResourceName = &s
                        offset += l2
                    }
                }
                if version >= 1 {
                    {
                        // reading resources[i0].PatternType: The resource pattern type.
                        resources[i0].PatternType = int8(buff[offset])
                        offset++
                    }
                }
                {
                    // reading resources[i0].Acls: The ACLs.
                    var l3 int
                    if version >= 2 {
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l3 = int(u - 1)
                    } else {
                        // non flexible and non nullable
                        l3 = int(binary.BigEndian.Uint32(buff[offset:]))
                        offset += 4
                    }
                    if l3 >= 0 {
                        // length will be -1 if field is null
                        acls := make([]DescribeAclsResponseAclDescription, l3)
                        for i1 := 0; i1 < l3; i1++ {
                            // reading non tagged fields
                            {
                                // reading acls[i1].Principal: The ACL principal.
                                if version >= 2 {
                                    // flexible and not nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l4 := int(u - 1)
                                    s := string(buff[offset: offset + l4])
                                    acls[i1].Principal = &s
                                    offset += l4
                                } else {
                                    // non flexible and non nullable
                                    var l4 int
                                    l4 = int(binary.BigEndian.Uint16(buff[offset:]))
                                    offset += 2
                                    s := string(buff[offset: offset + l4])
                                    acls[i1].Principal = &s
                                    offset += l4
                                }
                            }
                            {
                                // reading acls[i1].Host: The ACL host.
                                if version >= 2 {
                                    // flexible and not nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l5 := int(u - 1)
                                    s := string(buff[offset: offset + l5])
                                    acls[i1].Host = &s
                                    offset += l5
                                } else {
                                    // non flexible and non nullable
                                    var l5 int
                                    l5 = int(binary.BigEndian.Uint16(buff[offset:]))
                                    offset += 2
                                    s := string(buff[offset: offset + l5])
                                    acls[i1].Host = &s
                                    offset += l5
                                }
                            }
                            {
                                // reading acls[i1].Operation: The ACL operation.
                                acls[i1].Operation = int8(buff[offset])
                                offset++
                            }
                            {
                                // reading acls[i1].PermissionType: The ACL permission type.
                                acls[i1].PermissionType = int8(buff[offset])
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
                    resources[i0].Acls = acls
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
            }
        m.Resources = resources
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

func (m *DescribeAclsResponse) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    buff = binary.BigEndian.AppendUint32(buff, uint32(m.ThrottleTimeMs))
    // writing m.ErrorCode: The error code, or 0 if there was no error.
    buff = binary.BigEndian.AppendUint16(buff, uint16(m.ErrorCode))
    // writing m.ErrorMessage: The error message, or null if there was no error.
    if version >= 2 {
        // flexible and nullable
        if m.ErrorMessage == nil {
            // null
            buff = append(buff, 0)
        } else {
            // not null
            buff = binary.AppendUvarint(buff, uint64(len(*m.ErrorMessage) + 1))
        }
    } else {
        // non flexible and nullable
        if m.ErrorMessage == nil {
            // null
            buff = binary.BigEndian.AppendUint16(buff, 65535)
        } else {
            // not null
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.ErrorMessage)))
        }
    }
    if m.ErrorMessage != nil {
        buff = append(buff, *m.ErrorMessage...)
    }
    // writing m.Resources: Each Resource that is referenced in an ACL.
    if version >= 2 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.Resources) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.Resources)))
    }
    for _, resources := range m.Resources {
        // writing non tagged fields
        // writing resources.ResourceType: The resource type.
        buff = append(buff, byte(resources.ResourceType))
        // writing resources.ResourceName: The resource name.
        if version >= 2 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*resources.ResourceName) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*resources.ResourceName)))
        }
        if resources.ResourceName != nil {
            buff = append(buff, *resources.ResourceName...)
        }
        if version >= 1 {
            // writing resources.PatternType: The resource pattern type.
            buff = append(buff, byte(resources.PatternType))
        }
        // writing resources.Acls: The ACLs.
        if version >= 2 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(resources.Acls) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint32(buff, uint32(len(resources.Acls)))
        }
        for _, acls := range resources.Acls {
            // writing non tagged fields
            // writing acls.Principal: The ACL principal.
            if version >= 2 {
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(*acls.Principal) + 1))
            } else {
                // non flexible and non nullable
                buff = binary.BigEndian.AppendUint16(buff, uint16(len(*acls.Principal)))
            }
            if acls.Principal != nil {
                buff = append(buff, *acls.Principal...)
            }
            // writing acls.Host: The ACL host.
            if version >= 2 {
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(*acls.Host) + 1))
            } else {
                // non flexible and non nullable
                buff = binary.BigEndian.AppendUint16(buff, uint16(len(*acls.Host)))
            }
            if acls.Host != nil {
                buff = append(buff, *acls.Host...)
            }
            // writing acls.Operation: The ACL operation.
            buff = append(buff, byte(acls.Operation))
            // writing acls.PermissionType: The ACL permission type.
            buff = append(buff, byte(acls.PermissionType))
            if version >= 2 {
                numTaggedFields12 := 0
                // write number of tagged fields
                buff = binary.AppendUvarint(buff, uint64(numTaggedFields12))
            }
        }
        if version >= 2 {
            numTaggedFields13 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields13))
        }
    }
    if version >= 2 {
        numTaggedFields14 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields14))
    }
    return buff
}

func (m *DescribeAclsResponse) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    size += 4
    // size for m.ErrorCode: The error code, or 0 if there was no error.
    size += 2
    // size for m.ErrorMessage: The error message, or null if there was no error.
    if version >= 2 {
        // flexible and nullable
        if m.ErrorMessage == nil {
            // null
            size += 1
        } else {
            // not null
            size += sizeofUvarint(len(*m.ErrorMessage) + 1)
        }
    } else {
        // non flexible and nullable
        size += 2
    }
    if m.ErrorMessage != nil {
        size += len(*m.ErrorMessage)
    }
    // size for m.Resources: Each Resource that is referenced in an ACL.
    if version >= 2 {
        // flexible and not nullable
        size += sizeofUvarint(len(m.Resources) + 1)
    } else {
        // non flexible and non nullable
        size += 4
    }
    for _, resources := range m.Resources {
        size += 0 * int(unsafe.Sizeof(resources)) // hack to make sure loop variable is always used
        // calculating size for non tagged fields
        numTaggedFields1:= 0
        numTaggedFields1 += 0
        // size for resources.ResourceType: The resource type.
        size += 1
        // size for resources.ResourceName: The resource name.
        if version >= 2 {
            // flexible and not nullable
            size += sizeofUvarint(len(*resources.ResourceName) + 1)
        } else {
            // non flexible and non nullable
            size += 2
        }
        if resources.ResourceName != nil {
            size += len(*resources.ResourceName)
        }
        if version >= 1 {
            // size for resources.PatternType: The resource pattern type.
            size += 1
        }
        // size for resources.Acls: The ACLs.
        if version >= 2 {
            // flexible and not nullable
            size += sizeofUvarint(len(resources.Acls) + 1)
        } else {
            // non flexible and non nullable
            size += 4
        }
        for _, acls := range resources.Acls {
            size += 0 * int(unsafe.Sizeof(acls)) // hack to make sure loop variable is always used
            // calculating size for non tagged fields
            numTaggedFields2:= 0
            numTaggedFields2 += 0
            // size for acls.Principal: The ACL principal.
            if version >= 2 {
                // flexible and not nullable
                size += sizeofUvarint(len(*acls.Principal) + 1)
            } else {
                // non flexible and non nullable
                size += 2
            }
            if acls.Principal != nil {
                size += len(*acls.Principal)
            }
            // size for acls.Host: The ACL host.
            if version >= 2 {
                // flexible and not nullable
                size += sizeofUvarint(len(*acls.Host) + 1)
            } else {
                // non flexible and non nullable
                size += 2
            }
            if acls.Host != nil {
                size += len(*acls.Host)
            }
            // size for acls.Operation: The ACL operation.
            size += 1
            // size for acls.PermissionType: The ACL permission type.
            size += 1
            numTaggedFields3:= 0
            numTaggedFields3 += 0
            if version >= 2 {
                // writing size of num tagged fields field
                size += sizeofUvarint(numTaggedFields3)
            }
        }
        numTaggedFields4:= 0
        numTaggedFields4 += 0
        if version >= 2 {
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields4)
        }
    }
    numTaggedFields5:= 0
    numTaggedFields5 += 0
    if version >= 2 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields5)
    }
    return size, tagSizes
}


