// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"
import "github.com/spirit-labs/tektite/common"
import "unsafe"

type DescribeGroupsResponseDescribedGroupMember struct {
    // The member ID assigned by the group coordinator.
    MemberId *string
    // The unique identifier of the consumer instance provided by end user.
    GroupInstanceId *string
    // The client ID used in the member's latest join group request.
    ClientId *string
    // The client host.
    ClientHost *string
    // The metadata corresponding to the current group protocol in use.
    MemberMetadata []byte
    // The current assignment provided by the group leader.
    MemberAssignment []byte
}

type DescribeGroupsResponseDescribedGroup struct {
    // The describe error, or 0 if there was no error.
    ErrorCode int16
    // The group ID string.
    GroupId *string
    // The group state string, or the empty string.
    GroupState *string
    // The group protocol type, or the empty string.
    ProtocolType *string
    // The group protocol data, or the empty string.
    ProtocolData *string
    // The group members.
    Members []DescribeGroupsResponseDescribedGroupMember
    // 32-bit bitfield to represent authorized operations for this group.
    AuthorizedOperations int32
}

type DescribeGroupsResponse struct {
    // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ThrottleTimeMs int32
    // Each described group.
    Groups []DescribeGroupsResponseDescribedGroup
}

func (m *DescribeGroupsResponse) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    if version >= 1 {
        {
            // reading m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
            m.ThrottleTimeMs = int32(binary.BigEndian.Uint32(buff[offset:]))
            offset += 4
        }
    }
    {
        // reading m.Groups: Each described group.
        var l0 int
        if version >= 5 {
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
            groups := make([]DescribeGroupsResponseDescribedGroup, l0)
            for i0 := 0; i0 < l0; i0++ {
                // reading non tagged fields
                {
                    // reading groups[i0].ErrorCode: The describe error, or 0 if there was no error.
                    groups[i0].ErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
                    offset += 2
                }
                {
                    // reading groups[i0].GroupId: The group ID string.
                    if version >= 5 {
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l1 := int(u - 1)
                        s := string(buff[offset: offset + l1])
                        groups[i0].GroupId = &s
                        offset += l1
                    } else {
                        // non flexible and non nullable
                        var l1 int
                        l1 = int(binary.BigEndian.Uint16(buff[offset:]))
                        offset += 2
                        s := string(buff[offset: offset + l1])
                        groups[i0].GroupId = &s
                        offset += l1
                    }
                }
                {
                    // reading groups[i0].GroupState: The group state string, or the empty string.
                    if version >= 5 {
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l2 := int(u - 1)
                        s := string(buff[offset: offset + l2])
                        groups[i0].GroupState = &s
                        offset += l2
                    } else {
                        // non flexible and non nullable
                        var l2 int
                        l2 = int(binary.BigEndian.Uint16(buff[offset:]))
                        offset += 2
                        s := string(buff[offset: offset + l2])
                        groups[i0].GroupState = &s
                        offset += l2
                    }
                }
                {
                    // reading groups[i0].ProtocolType: The group protocol type, or the empty string.
                    if version >= 5 {
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l3 := int(u - 1)
                        s := string(buff[offset: offset + l3])
                        groups[i0].ProtocolType = &s
                        offset += l3
                    } else {
                        // non flexible and non nullable
                        var l3 int
                        l3 = int(binary.BigEndian.Uint16(buff[offset:]))
                        offset += 2
                        s := string(buff[offset: offset + l3])
                        groups[i0].ProtocolType = &s
                        offset += l3
                    }
                }
                {
                    // reading groups[i0].ProtocolData: The group protocol data, or the empty string.
                    if version >= 5 {
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l4 := int(u - 1)
                        s := string(buff[offset: offset + l4])
                        groups[i0].ProtocolData = &s
                        offset += l4
                    } else {
                        // non flexible and non nullable
                        var l4 int
                        l4 = int(binary.BigEndian.Uint16(buff[offset:]))
                        offset += 2
                        s := string(buff[offset: offset + l4])
                        groups[i0].ProtocolData = &s
                        offset += l4
                    }
                }
                {
                    // reading groups[i0].Members: The group members.
                    var l5 int
                    if version >= 5 {
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l5 = int(u - 1)
                    } else {
                        // non flexible and non nullable
                        l5 = int(binary.BigEndian.Uint32(buff[offset:]))
                        offset += 4
                    }
                    if l5 >= 0 {
                        // length will be -1 if field is null
                        members := make([]DescribeGroupsResponseDescribedGroupMember, l5)
                        for i1 := 0; i1 < l5; i1++ {
                            // reading non tagged fields
                            {
                                // reading members[i1].MemberId: The member ID assigned by the group coordinator.
                                if version >= 5 {
                                    // flexible and not nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l6 := int(u - 1)
                                    s := string(buff[offset: offset + l6])
                                    members[i1].MemberId = &s
                                    offset += l6
                                } else {
                                    // non flexible and non nullable
                                    var l6 int
                                    l6 = int(binary.BigEndian.Uint16(buff[offset:]))
                                    offset += 2
                                    s := string(buff[offset: offset + l6])
                                    members[i1].MemberId = &s
                                    offset += l6
                                }
                            }
                            if version >= 4 {
                                {
                                    // reading members[i1].GroupInstanceId: The unique identifier of the consumer instance provided by end user.
                                    if version >= 5 {
                                        // flexible and nullable
                                        u, n := binary.Uvarint(buff[offset:])
                                        offset += n
                                        l7 := int(u - 1)
                                        if l7 > 0 {
                                            s := string(buff[offset: offset + l7])
                                            members[i1].GroupInstanceId = &s
                                            offset += l7
                                        } else {
                                            members[i1].GroupInstanceId = nil
                                        }
                                    } else {
                                        // non flexible and nullable
                                        var l7 int
                                        l7 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
                                        offset += 2
                                        if l7 > 0 {
                                            s := string(buff[offset: offset + l7])
                                            members[i1].GroupInstanceId = &s
                                            offset += l7
                                        } else {
                                            members[i1].GroupInstanceId = nil
                                        }
                                    }
                                }
                            }
                            {
                                // reading members[i1].ClientId: The client ID used in the member's latest join group request.
                                if version >= 5 {
                                    // flexible and not nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l8 := int(u - 1)
                                    s := string(buff[offset: offset + l8])
                                    members[i1].ClientId = &s
                                    offset += l8
                                } else {
                                    // non flexible and non nullable
                                    var l8 int
                                    l8 = int(binary.BigEndian.Uint16(buff[offset:]))
                                    offset += 2
                                    s := string(buff[offset: offset + l8])
                                    members[i1].ClientId = &s
                                    offset += l8
                                }
                            }
                            {
                                // reading members[i1].ClientHost: The client host.
                                if version >= 5 {
                                    // flexible and not nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l9 := int(u - 1)
                                    s := string(buff[offset: offset + l9])
                                    members[i1].ClientHost = &s
                                    offset += l9
                                } else {
                                    // non flexible and non nullable
                                    var l9 int
                                    l9 = int(binary.BigEndian.Uint16(buff[offset:]))
                                    offset += 2
                                    s := string(buff[offset: offset + l9])
                                    members[i1].ClientHost = &s
                                    offset += l9
                                }
                            }
                            {
                                // reading members[i1].MemberMetadata: The metadata corresponding to the current group protocol in use.
                                if version >= 5 {
                                    // flexible and not nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l10 := int(u - 1)
                                    members[i1].MemberMetadata = common.ByteSliceCopy(buff[offset: offset + l10])
                                    offset += l10
                                } else {
                                    // non flexible and non nullable
                                    var l10 int
                                    l10 = int(binary.BigEndian.Uint32(buff[offset:]))
                                    offset += 4
                                    members[i1].MemberMetadata = common.ByteSliceCopy(buff[offset: offset + l10])
                                    offset += l10
                                }
                            }
                            {
                                // reading members[i1].MemberAssignment: The current assignment provided by the group leader.
                                if version >= 5 {
                                    // flexible and not nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l11 := int(u - 1)
                                    members[i1].MemberAssignment = common.ByteSliceCopy(buff[offset: offset + l11])
                                    offset += l11
                                } else {
                                    // non flexible and non nullable
                                    var l11 int
                                    l11 = int(binary.BigEndian.Uint32(buff[offset:]))
                                    offset += 4
                                    members[i1].MemberAssignment = common.ByteSliceCopy(buff[offset: offset + l11])
                                    offset += l11
                                }
                            }
                            if version >= 5 {
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
                    groups[i0].Members = members
                    }
                }
                if version >= 3 {
                    {
                        // reading groups[i0].AuthorizedOperations: 32-bit bitfield to represent authorized operations for this group.
                        groups[i0].AuthorizedOperations = int32(binary.BigEndian.Uint32(buff[offset:]))
                        offset += 4
                    }
                }
                if version >= 5 {
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
        m.Groups = groups
        }
    }
    if version >= 5 {
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

func (m *DescribeGroupsResponse) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    if version >= 1 {
        // writing m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        buff = binary.BigEndian.AppendUint32(buff, uint32(m.ThrottleTimeMs))
    }
    // writing m.Groups: Each described group.
    if version >= 5 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.Groups) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.Groups)))
    }
    for _, groups := range m.Groups {
        // writing non tagged fields
        // writing groups.ErrorCode: The describe error, or 0 if there was no error.
        buff = binary.BigEndian.AppendUint16(buff, uint16(groups.ErrorCode))
        // writing groups.GroupId: The group ID string.
        if version >= 5 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*groups.GroupId) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*groups.GroupId)))
        }
        if groups.GroupId != nil {
            buff = append(buff, *groups.GroupId...)
        }
        // writing groups.GroupState: The group state string, or the empty string.
        if version >= 5 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*groups.GroupState) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*groups.GroupState)))
        }
        if groups.GroupState != nil {
            buff = append(buff, *groups.GroupState...)
        }
        // writing groups.ProtocolType: The group protocol type, or the empty string.
        if version >= 5 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*groups.ProtocolType) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*groups.ProtocolType)))
        }
        if groups.ProtocolType != nil {
            buff = append(buff, *groups.ProtocolType...)
        }
        // writing groups.ProtocolData: The group protocol data, or the empty string.
        if version >= 5 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*groups.ProtocolData) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*groups.ProtocolData)))
        }
        if groups.ProtocolData != nil {
            buff = append(buff, *groups.ProtocolData...)
        }
        // writing groups.Members: The group members.
        if version >= 5 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(groups.Members) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint32(buff, uint32(len(groups.Members)))
        }
        for _, members := range groups.Members {
            // writing non tagged fields
            // writing members.MemberId: The member ID assigned by the group coordinator.
            if version >= 5 {
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(*members.MemberId) + 1))
            } else {
                // non flexible and non nullable
                buff = binary.BigEndian.AppendUint16(buff, uint16(len(*members.MemberId)))
            }
            if members.MemberId != nil {
                buff = append(buff, *members.MemberId...)
            }
            if version >= 4 {
                // writing members.GroupInstanceId: The unique identifier of the consumer instance provided by end user.
                if version >= 5 {
                    // flexible and nullable
                    if members.GroupInstanceId == nil {
                        // null
                        buff = append(buff, 0)
                    } else {
                        // not null
                        buff = binary.AppendUvarint(buff, uint64(len(*members.GroupInstanceId) + 1))
                    }
                } else {
                    // non flexible and nullable
                    if members.GroupInstanceId == nil {
                        // null
                        buff = binary.BigEndian.AppendUint16(buff, 65535)
                    } else {
                        // not null
                        buff = binary.BigEndian.AppendUint16(buff, uint16(len(*members.GroupInstanceId)))
                    }
                }
                if members.GroupInstanceId != nil {
                    buff = append(buff, *members.GroupInstanceId...)
                }
            }
            // writing members.ClientId: The client ID used in the member's latest join group request.
            if version >= 5 {
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(*members.ClientId) + 1))
            } else {
                // non flexible and non nullable
                buff = binary.BigEndian.AppendUint16(buff, uint16(len(*members.ClientId)))
            }
            if members.ClientId != nil {
                buff = append(buff, *members.ClientId...)
            }
            // writing members.ClientHost: The client host.
            if version >= 5 {
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(*members.ClientHost) + 1))
            } else {
                // non flexible and non nullable
                buff = binary.BigEndian.AppendUint16(buff, uint16(len(*members.ClientHost)))
            }
            if members.ClientHost != nil {
                buff = append(buff, *members.ClientHost...)
            }
            // writing members.MemberMetadata: The metadata corresponding to the current group protocol in use.
            if version >= 5 {
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(members.MemberMetadata) + 1))
            } else {
                // non flexible and non nullable
                buff = binary.BigEndian.AppendUint32(buff, uint32(len(members.MemberMetadata)))
            }
            if members.MemberMetadata != nil {
                buff = append(buff, members.MemberMetadata...)
            }
            // writing members.MemberAssignment: The current assignment provided by the group leader.
            if version >= 5 {
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(members.MemberAssignment) + 1))
            } else {
                // non flexible and non nullable
                buff = binary.BigEndian.AppendUint32(buff, uint32(len(members.MemberAssignment)))
            }
            if members.MemberAssignment != nil {
                buff = append(buff, members.MemberAssignment...)
            }
            if version >= 5 {
                numTaggedFields14 := 0
                // write number of tagged fields
                buff = binary.AppendUvarint(buff, uint64(numTaggedFields14))
            }
        }
        if version >= 3 {
            // writing groups.AuthorizedOperations: 32-bit bitfield to represent authorized operations for this group.
            buff = binary.BigEndian.AppendUint32(buff, uint32(groups.AuthorizedOperations))
        }
        if version >= 5 {
            numTaggedFields16 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields16))
        }
    }
    if version >= 5 {
        numTaggedFields17 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields17))
    }
    return buff
}

func (m *DescribeGroupsResponse) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    if version >= 1 {
        // size for m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        size += 4
    }
    // size for m.Groups: Each described group.
    if version >= 5 {
        // flexible and not nullable
        size += sizeofUvarint(len(m.Groups) + 1)
    } else {
        // non flexible and non nullable
        size += 4
    }
    for _, groups := range m.Groups {
        size += 0 * int(unsafe.Sizeof(groups)) // hack to make sure loop variable is always used
        // calculating size for non tagged fields
        numTaggedFields1:= 0
        numTaggedFields1 += 0
        // size for groups.ErrorCode: The describe error, or 0 if there was no error.
        size += 2
        // size for groups.GroupId: The group ID string.
        if version >= 5 {
            // flexible and not nullable
            size += sizeofUvarint(len(*groups.GroupId) + 1)
        } else {
            // non flexible and non nullable
            size += 2
        }
        if groups.GroupId != nil {
            size += len(*groups.GroupId)
        }
        // size for groups.GroupState: The group state string, or the empty string.
        if version >= 5 {
            // flexible and not nullable
            size += sizeofUvarint(len(*groups.GroupState) + 1)
        } else {
            // non flexible and non nullable
            size += 2
        }
        if groups.GroupState != nil {
            size += len(*groups.GroupState)
        }
        // size for groups.ProtocolType: The group protocol type, or the empty string.
        if version >= 5 {
            // flexible and not nullable
            size += sizeofUvarint(len(*groups.ProtocolType) + 1)
        } else {
            // non flexible and non nullable
            size += 2
        }
        if groups.ProtocolType != nil {
            size += len(*groups.ProtocolType)
        }
        // size for groups.ProtocolData: The group protocol data, or the empty string.
        if version >= 5 {
            // flexible and not nullable
            size += sizeofUvarint(len(*groups.ProtocolData) + 1)
        } else {
            // non flexible and non nullable
            size += 2
        }
        if groups.ProtocolData != nil {
            size += len(*groups.ProtocolData)
        }
        // size for groups.Members: The group members.
        if version >= 5 {
            // flexible and not nullable
            size += sizeofUvarint(len(groups.Members) + 1)
        } else {
            // non flexible and non nullable
            size += 4
        }
        for _, members := range groups.Members {
            size += 0 * int(unsafe.Sizeof(members)) // hack to make sure loop variable is always used
            // calculating size for non tagged fields
            numTaggedFields2:= 0
            numTaggedFields2 += 0
            // size for members.MemberId: The member ID assigned by the group coordinator.
            if version >= 5 {
                // flexible and not nullable
                size += sizeofUvarint(len(*members.MemberId) + 1)
            } else {
                // non flexible and non nullable
                size += 2
            }
            if members.MemberId != nil {
                size += len(*members.MemberId)
            }
            if version >= 4 {
                // size for members.GroupInstanceId: The unique identifier of the consumer instance provided by end user.
                if version >= 5 {
                    // flexible and nullable
                    if members.GroupInstanceId == nil {
                        // null
                        size += 1
                    } else {
                        // not null
                        size += sizeofUvarint(len(*members.GroupInstanceId) + 1)
                    }
                } else {
                    // non flexible and nullable
                    size += 2
                }
                if members.GroupInstanceId != nil {
                    size += len(*members.GroupInstanceId)
                }
            }
            // size for members.ClientId: The client ID used in the member's latest join group request.
            if version >= 5 {
                // flexible and not nullable
                size += sizeofUvarint(len(*members.ClientId) + 1)
            } else {
                // non flexible and non nullable
                size += 2
            }
            if members.ClientId != nil {
                size += len(*members.ClientId)
            }
            // size for members.ClientHost: The client host.
            if version >= 5 {
                // flexible and not nullable
                size += sizeofUvarint(len(*members.ClientHost) + 1)
            } else {
                // non flexible and non nullable
                size += 2
            }
            if members.ClientHost != nil {
                size += len(*members.ClientHost)
            }
            // size for members.MemberMetadata: The metadata corresponding to the current group protocol in use.
            if version >= 5 {
                // flexible and not nullable
                size += sizeofUvarint(len(members.MemberMetadata) + 1)
            } else {
                // non flexible and non nullable
                size += 4
            }
            if members.MemberMetadata != nil {
                size += len(members.MemberMetadata)
            }
            // size for members.MemberAssignment: The current assignment provided by the group leader.
            if version >= 5 {
                // flexible and not nullable
                size += sizeofUvarint(len(members.MemberAssignment) + 1)
            } else {
                // non flexible and non nullable
                size += 4
            }
            if members.MemberAssignment != nil {
                size += len(members.MemberAssignment)
            }
            numTaggedFields3:= 0
            numTaggedFields3 += 0
            if version >= 5 {
                // writing size of num tagged fields field
                size += sizeofUvarint(numTaggedFields3)
            }
        }
        if version >= 3 {
            // size for groups.AuthorizedOperations: 32-bit bitfield to represent authorized operations for this group.
            size += 4
        }
        numTaggedFields4:= 0
        numTaggedFields4 += 0
        if version >= 5 {
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields4)
        }
    }
    numTaggedFields5:= 0
    numTaggedFields5 += 0
    if version >= 5 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields5)
    }
    return size, tagSizes
}


