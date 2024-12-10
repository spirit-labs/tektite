// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"
import "unsafe"

type DeleteAclsResponseDeleteAclsMatchingAcl struct {
    // The deletion error code, or 0 if the deletion succeeded.
    ErrorCode int16
    // The deletion error message, or null if the deletion succeeded.
    ErrorMessage *string
    // The ACL resource type.
    ResourceType int8
    // The ACL resource name.
    ResourceName *string
    // The ACL resource pattern type.
    PatternType int8
    // The ACL principal.
    Principal *string
    // The ACL host.
    Host *string
    // The ACL operation.
    Operation int8
    // The ACL permission type.
    PermissionType int8
}

type DeleteAclsResponseDeleteAclsFilterResult struct {
    // The error code, or 0 if the filter succeeded.
    ErrorCode int16
    // The error message, or null if the filter succeeded.
    ErrorMessage *string
    // The ACLs which matched this filter.
    MatchingAcls []DeleteAclsResponseDeleteAclsMatchingAcl
}

type DeleteAclsResponse struct {
    // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ThrottleTimeMs int32
    // The results for each filter.
    FilterResults []DeleteAclsResponseDeleteAclsFilterResult
}

func (m *DeleteAclsResponse) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        m.ThrottleTimeMs = int32(binary.BigEndian.Uint32(buff[offset:]))
        offset += 4
    }
    {
        // reading m.FilterResults: The results for each filter.
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
            filterResults := make([]DeleteAclsResponseDeleteAclsFilterResult, l0)
            for i0 := 0; i0 < l0; i0++ {
                // reading non tagged fields
                {
                    // reading filterResults[i0].ErrorCode: The error code, or 0 if the filter succeeded.
                    filterResults[i0].ErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
                    offset += 2
                }
                {
                    // reading filterResults[i0].ErrorMessage: The error message, or null if the filter succeeded.
                    if version >= 2 {
                        // flexible and nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l1 := int(u - 1)
                        if l1 > 0 {
                            s := string(buff[offset: offset + l1])
                            filterResults[i0].ErrorMessage = &s
                            offset += l1
                        } else {
                            filterResults[i0].ErrorMessage = nil
                        }
                    } else {
                        // non flexible and nullable
                        var l1 int
                        l1 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
                        offset += 2
                        if l1 > 0 {
                            s := string(buff[offset: offset + l1])
                            filterResults[i0].ErrorMessage = &s
                            offset += l1
                        } else {
                            filterResults[i0].ErrorMessage = nil
                        }
                    }
                }
                {
                    // reading filterResults[i0].MatchingAcls: The ACLs which matched this filter.
                    var l2 int
                    if version >= 2 {
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l2 = int(u - 1)
                    } else {
                        // non flexible and non nullable
                        l2 = int(binary.BigEndian.Uint32(buff[offset:]))
                        offset += 4
                    }
                    if l2 >= 0 {
                        // length will be -1 if field is null
                        matchingAcls := make([]DeleteAclsResponseDeleteAclsMatchingAcl, l2)
                        for i1 := 0; i1 < l2; i1++ {
                            // reading non tagged fields
                            {
                                // reading matchingAcls[i1].ErrorCode: The deletion error code, or 0 if the deletion succeeded.
                                matchingAcls[i1].ErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
                                offset += 2
                            }
                            {
                                // reading matchingAcls[i1].ErrorMessage: The deletion error message, or null if the deletion succeeded.
                                if version >= 2 {
                                    // flexible and nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l3 := int(u - 1)
                                    if l3 > 0 {
                                        s := string(buff[offset: offset + l3])
                                        matchingAcls[i1].ErrorMessage = &s
                                        offset += l3
                                    } else {
                                        matchingAcls[i1].ErrorMessage = nil
                                    }
                                } else {
                                    // non flexible and nullable
                                    var l3 int
                                    l3 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
                                    offset += 2
                                    if l3 > 0 {
                                        s := string(buff[offset: offset + l3])
                                        matchingAcls[i1].ErrorMessage = &s
                                        offset += l3
                                    } else {
                                        matchingAcls[i1].ErrorMessage = nil
                                    }
                                }
                            }
                            {
                                // reading matchingAcls[i1].ResourceType: The ACL resource type.
                                matchingAcls[i1].ResourceType = int8(buff[offset])
                                offset++
                            }
                            {
                                // reading matchingAcls[i1].ResourceName: The ACL resource name.
                                if version >= 2 {
                                    // flexible and not nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l4 := int(u - 1)
                                    s := string(buff[offset: offset + l4])
                                    matchingAcls[i1].ResourceName = &s
                                    offset += l4
                                } else {
                                    // non flexible and non nullable
                                    var l4 int
                                    l4 = int(binary.BigEndian.Uint16(buff[offset:]))
                                    offset += 2
                                    s := string(buff[offset: offset + l4])
                                    matchingAcls[i1].ResourceName = &s
                                    offset += l4
                                }
                            }
                            if version >= 1 {
                                {
                                    // reading matchingAcls[i1].PatternType: The ACL resource pattern type.
                                    matchingAcls[i1].PatternType = int8(buff[offset])
                                    offset++
                                }
                            }
                            {
                                // reading matchingAcls[i1].Principal: The ACL principal.
                                if version >= 2 {
                                    // flexible and not nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l5 := int(u - 1)
                                    s := string(buff[offset: offset + l5])
                                    matchingAcls[i1].Principal = &s
                                    offset += l5
                                } else {
                                    // non flexible and non nullable
                                    var l5 int
                                    l5 = int(binary.BigEndian.Uint16(buff[offset:]))
                                    offset += 2
                                    s := string(buff[offset: offset + l5])
                                    matchingAcls[i1].Principal = &s
                                    offset += l5
                                }
                            }
                            {
                                // reading matchingAcls[i1].Host: The ACL host.
                                if version >= 2 {
                                    // flexible and not nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l6 := int(u - 1)
                                    s := string(buff[offset: offset + l6])
                                    matchingAcls[i1].Host = &s
                                    offset += l6
                                } else {
                                    // non flexible and non nullable
                                    var l6 int
                                    l6 = int(binary.BigEndian.Uint16(buff[offset:]))
                                    offset += 2
                                    s := string(buff[offset: offset + l6])
                                    matchingAcls[i1].Host = &s
                                    offset += l6
                                }
                            }
                            {
                                // reading matchingAcls[i1].Operation: The ACL operation.
                                matchingAcls[i1].Operation = int8(buff[offset])
                                offset++
                            }
                            {
                                // reading matchingAcls[i1].PermissionType: The ACL permission type.
                                matchingAcls[i1].PermissionType = int8(buff[offset])
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
                    filterResults[i0].MatchingAcls = matchingAcls
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
        m.FilterResults = filterResults
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

func (m *DeleteAclsResponse) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    buff = binary.BigEndian.AppendUint32(buff, uint32(m.ThrottleTimeMs))
    // writing m.FilterResults: The results for each filter.
    if version >= 2 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.FilterResults) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.FilterResults)))
    }
    for _, filterResults := range m.FilterResults {
        // writing non tagged fields
        // writing filterResults.ErrorCode: The error code, or 0 if the filter succeeded.
        buff = binary.BigEndian.AppendUint16(buff, uint16(filterResults.ErrorCode))
        // writing filterResults.ErrorMessage: The error message, or null if the filter succeeded.
        if version >= 2 {
            // flexible and nullable
            if filterResults.ErrorMessage == nil {
                // null
                buff = append(buff, 0)
            } else {
                // not null
                buff = binary.AppendUvarint(buff, uint64(len(*filterResults.ErrorMessage) + 1))
            }
        } else {
            // non flexible and nullable
            if filterResults.ErrorMessage == nil {
                // null
                buff = binary.BigEndian.AppendUint16(buff, 65535)
            } else {
                // not null
                buff = binary.BigEndian.AppendUint16(buff, uint16(len(*filterResults.ErrorMessage)))
            }
        }
        if filterResults.ErrorMessage != nil {
            buff = append(buff, *filterResults.ErrorMessage...)
        }
        // writing filterResults.MatchingAcls: The ACLs which matched this filter.
        if version >= 2 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(filterResults.MatchingAcls) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint32(buff, uint32(len(filterResults.MatchingAcls)))
        }
        for _, matchingAcls := range filterResults.MatchingAcls {
            // writing non tagged fields
            // writing matchingAcls.ErrorCode: The deletion error code, or 0 if the deletion succeeded.
            buff = binary.BigEndian.AppendUint16(buff, uint16(matchingAcls.ErrorCode))
            // writing matchingAcls.ErrorMessage: The deletion error message, or null if the deletion succeeded.
            if version >= 2 {
                // flexible and nullable
                if matchingAcls.ErrorMessage == nil {
                    // null
                    buff = append(buff, 0)
                } else {
                    // not null
                    buff = binary.AppendUvarint(buff, uint64(len(*matchingAcls.ErrorMessage) + 1))
                }
            } else {
                // non flexible and nullable
                if matchingAcls.ErrorMessage == nil {
                    // null
                    buff = binary.BigEndian.AppendUint16(buff, 65535)
                } else {
                    // not null
                    buff = binary.BigEndian.AppendUint16(buff, uint16(len(*matchingAcls.ErrorMessage)))
                }
            }
            if matchingAcls.ErrorMessage != nil {
                buff = append(buff, *matchingAcls.ErrorMessage...)
            }
            // writing matchingAcls.ResourceType: The ACL resource type.
            buff = append(buff, byte(matchingAcls.ResourceType))
            // writing matchingAcls.ResourceName: The ACL resource name.
            if version >= 2 {
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(*matchingAcls.ResourceName) + 1))
            } else {
                // non flexible and non nullable
                buff = binary.BigEndian.AppendUint16(buff, uint16(len(*matchingAcls.ResourceName)))
            }
            if matchingAcls.ResourceName != nil {
                buff = append(buff, *matchingAcls.ResourceName...)
            }
            if version >= 1 {
                // writing matchingAcls.PatternType: The ACL resource pattern type.
                buff = append(buff, byte(matchingAcls.PatternType))
            }
            // writing matchingAcls.Principal: The ACL principal.
            if version >= 2 {
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(*matchingAcls.Principal) + 1))
            } else {
                // non flexible and non nullable
                buff = binary.BigEndian.AppendUint16(buff, uint16(len(*matchingAcls.Principal)))
            }
            if matchingAcls.Principal != nil {
                buff = append(buff, *matchingAcls.Principal...)
            }
            // writing matchingAcls.Host: The ACL host.
            if version >= 2 {
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(*matchingAcls.Host) + 1))
            } else {
                // non flexible and non nullable
                buff = binary.BigEndian.AppendUint16(buff, uint16(len(*matchingAcls.Host)))
            }
            if matchingAcls.Host != nil {
                buff = append(buff, *matchingAcls.Host...)
            }
            // writing matchingAcls.Operation: The ACL operation.
            buff = append(buff, byte(matchingAcls.Operation))
            // writing matchingAcls.PermissionType: The ACL permission type.
            buff = append(buff, byte(matchingAcls.PermissionType))
            if version >= 2 {
                numTaggedFields14 := 0
                // write number of tagged fields
                buff = binary.AppendUvarint(buff, uint64(numTaggedFields14))
            }
        }
        if version >= 2 {
            numTaggedFields15 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields15))
        }
    }
    if version >= 2 {
        numTaggedFields16 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields16))
    }
    return buff
}

func (m *DeleteAclsResponse) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    size += 4
    // size for m.FilterResults: The results for each filter.
    if version >= 2 {
        // flexible and not nullable
        size += sizeofUvarint(len(m.FilterResults) + 1)
    } else {
        // non flexible and non nullable
        size += 4
    }
    for _, filterResults := range m.FilterResults {
        size += 0 * int(unsafe.Sizeof(filterResults)) // hack to make sure loop variable is always used
        // calculating size for non tagged fields
        numTaggedFields1:= 0
        numTaggedFields1 += 0
        // size for filterResults.ErrorCode: The error code, or 0 if the filter succeeded.
        size += 2
        // size for filterResults.ErrorMessage: The error message, or null if the filter succeeded.
        if version >= 2 {
            // flexible and nullable
            if filterResults.ErrorMessage == nil {
                // null
                size += 1
            } else {
                // not null
                size += sizeofUvarint(len(*filterResults.ErrorMessage) + 1)
            }
        } else {
            // non flexible and nullable
            size += 2
        }
        if filterResults.ErrorMessage != nil {
            size += len(*filterResults.ErrorMessage)
        }
        // size for filterResults.MatchingAcls: The ACLs which matched this filter.
        if version >= 2 {
            // flexible and not nullable
            size += sizeofUvarint(len(filterResults.MatchingAcls) + 1)
        } else {
            // non flexible and non nullable
            size += 4
        }
        for _, matchingAcls := range filterResults.MatchingAcls {
            size += 0 * int(unsafe.Sizeof(matchingAcls)) // hack to make sure loop variable is always used
            // calculating size for non tagged fields
            numTaggedFields2:= 0
            numTaggedFields2 += 0
            // size for matchingAcls.ErrorCode: The deletion error code, or 0 if the deletion succeeded.
            size += 2
            // size for matchingAcls.ErrorMessage: The deletion error message, or null if the deletion succeeded.
            if version >= 2 {
                // flexible and nullable
                if matchingAcls.ErrorMessage == nil {
                    // null
                    size += 1
                } else {
                    // not null
                    size += sizeofUvarint(len(*matchingAcls.ErrorMessage) + 1)
                }
            } else {
                // non flexible and nullable
                size += 2
            }
            if matchingAcls.ErrorMessage != nil {
                size += len(*matchingAcls.ErrorMessage)
            }
            // size for matchingAcls.ResourceType: The ACL resource type.
            size += 1
            // size for matchingAcls.ResourceName: The ACL resource name.
            if version >= 2 {
                // flexible and not nullable
                size += sizeofUvarint(len(*matchingAcls.ResourceName) + 1)
            } else {
                // non flexible and non nullable
                size += 2
            }
            if matchingAcls.ResourceName != nil {
                size += len(*matchingAcls.ResourceName)
            }
            if version >= 1 {
                // size for matchingAcls.PatternType: The ACL resource pattern type.
                size += 1
            }
            // size for matchingAcls.Principal: The ACL principal.
            if version >= 2 {
                // flexible and not nullable
                size += sizeofUvarint(len(*matchingAcls.Principal) + 1)
            } else {
                // non flexible and non nullable
                size += 2
            }
            if matchingAcls.Principal != nil {
                size += len(*matchingAcls.Principal)
            }
            // size for matchingAcls.Host: The ACL host.
            if version >= 2 {
                // flexible and not nullable
                size += sizeofUvarint(len(*matchingAcls.Host) + 1)
            } else {
                // non flexible and non nullable
                size += 2
            }
            if matchingAcls.Host != nil {
                size += len(*matchingAcls.Host)
            }
            // size for matchingAcls.Operation: The ACL operation.
            size += 1
            // size for matchingAcls.PermissionType: The ACL permission type.
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


