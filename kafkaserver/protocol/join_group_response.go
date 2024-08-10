// Package protocol - This is a generated file, please do not edit

package protocol

import "encoding/binary"
import "github.com/spirit-labs/tektite/common"
import "unsafe"

type JoinGroupResponseJoinGroupResponseMember struct {
    // The group member ID.
    MemberId *string
    // The unique identifier of the consumer instance provided by end user.
    GroupInstanceId *string
    // The group member metadata.
    Metadata []byte
}

type JoinGroupResponse struct {
    // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ThrottleTimeMs int32
    // The error code, or 0 if there was no error.
    ErrorCode int16
    // The generation ID of the group.
    GenerationId int32
    // The group protocol name.
    ProtocolType *string
    // The group protocol selected by the coordinator.
    ProtocolName *string
    // The leader of the group.
    Leader *string
    // True if the leader must skip running the assignment.
    SkipAssignment bool
    // The member ID assigned by the group coordinator.
    MemberId *string
    Members []JoinGroupResponseJoinGroupResponseMember
}

func (m *JoinGroupResponse) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    if version >= 2 {
        {
            // reading m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
            m.ThrottleTimeMs = int32(binary.BigEndian.Uint32(buff[offset:]))
            offset += 4
        }
    }
    {
        // reading m.ErrorCode: The error code, or 0 if there was no error.
        m.ErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
        offset += 2
    }
    {
        // reading m.GenerationId: The generation ID of the group.
        m.GenerationId = int32(binary.BigEndian.Uint32(buff[offset:]))
        offset += 4
    }
    if version >= 7 {
        {
            // reading m.ProtocolType: The group protocol name.
            // flexible and nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l0 := int(u - 1)
            if l0 > 0 {
                s := string(buff[offset: offset + l0])
                m.ProtocolType = &s
                offset += l0
            } else {
                m.ProtocolType = nil
            }
        }
    }
    {
        // reading m.ProtocolName: The group protocol selected by the coordinator.
        if version >= 6 {
            if version >= 7 {
                // flexible and nullable
                u, n := binary.Uvarint(buff[offset:])
                offset += n
                l1 := int(u - 1)
                if l1 > 0 {
                    s := string(buff[offset: offset + l1])
                    m.ProtocolName = &s
                    offset += l1
                } else {
                    m.ProtocolName = nil
                }
            } else {
                // flexible and not nullable
                u, n := binary.Uvarint(buff[offset:])
                offset += n
                l1 := int(u - 1)
                s := string(buff[offset: offset + l1])
                m.ProtocolName = &s
                offset += l1
            }
        } else {
            if version >= 7 {
                // non flexible and nullable
                var l1 int
                l1 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
                offset += 2
                if l1 > 0 {
                    s := string(buff[offset: offset + l1])
                    m.ProtocolName = &s
                    offset += l1
                } else {
                    m.ProtocolName = nil
                }
            } else {
                // non flexible and non nullable
                var l1 int
                l1 = int(binary.BigEndian.Uint16(buff[offset:]))
                offset += 2
                s := string(buff[offset: offset + l1])
                m.ProtocolName = &s
                offset += l1
            }
        }
    }
    {
        // reading m.Leader: The leader of the group.
        if version >= 6 {
            // flexible and not nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l2 := int(u - 1)
            s := string(buff[offset: offset + l2])
            m.Leader = &s
            offset += l2
        } else {
            // non flexible and non nullable
            var l2 int
            l2 = int(binary.BigEndian.Uint16(buff[offset:]))
            offset += 2
            s := string(buff[offset: offset + l2])
            m.Leader = &s
            offset += l2
        }
    }
    if version >= 9 {
        {
            // reading m.SkipAssignment: True if the leader must skip running the assignment.
            m.SkipAssignment = buff[offset] == 1
            offset++
        }
    }
    {
        // reading m.MemberId: The member ID assigned by the group coordinator.
        if version >= 6 {
            // flexible and not nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l3 := int(u - 1)
            s := string(buff[offset: offset + l3])
            m.MemberId = &s
            offset += l3
        } else {
            // non flexible and non nullable
            var l3 int
            l3 = int(binary.BigEndian.Uint16(buff[offset:]))
            offset += 2
            s := string(buff[offset: offset + l3])
            m.MemberId = &s
            offset += l3
        }
    }
    {
        // reading m.Members: 
        var l4 int
        if version >= 6 {
            // flexible and not nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l4 = int(u - 1)
        } else {
            // non flexible and non nullable
            l4 = int(binary.BigEndian.Uint32(buff[offset:]))
            offset += 4
        }
        if l4 >= 0 {
            // length will be -1 if field is null
            members := make([]JoinGroupResponseJoinGroupResponseMember, l4)
            for i0 := 0; i0 < l4; i0++ {
                // reading non tagged fields
                {
                    // reading members[i0].MemberId: The group member ID.
                    if version >= 6 {
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l5 := int(u - 1)
                        s := string(buff[offset: offset + l5])
                        members[i0].MemberId = &s
                        offset += l5
                    } else {
                        // non flexible and non nullable
                        var l5 int
                        l5 = int(binary.BigEndian.Uint16(buff[offset:]))
                        offset += 2
                        s := string(buff[offset: offset + l5])
                        members[i0].MemberId = &s
                        offset += l5
                    }
                }
                if version >= 5 {
                    {
                        // reading members[i0].GroupInstanceId: The unique identifier of the consumer instance provided by end user.
                        if version >= 6 {
                            // flexible and nullable
                            u, n := binary.Uvarint(buff[offset:])
                            offset += n
                            l6 := int(u - 1)
                            if l6 > 0 {
                                s := string(buff[offset: offset + l6])
                                members[i0].GroupInstanceId = &s
                                offset += l6
                            } else {
                                members[i0].GroupInstanceId = nil
                            }
                        } else {
                            // non flexible and nullable
                            var l6 int
                            l6 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
                            offset += 2
                            if l6 > 0 {
                                s := string(buff[offset: offset + l6])
                                members[i0].GroupInstanceId = &s
                                offset += l6
                            } else {
                                members[i0].GroupInstanceId = nil
                            }
                        }
                    }
                }
                {
                    // reading members[i0].Metadata: The group member metadata.
                    if version >= 6 {
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l7 := int(u - 1)
                        members[i0].Metadata = common.ByteSliceCopy(buff[offset: offset + l7])
                        offset += l7
                    } else {
                        // non flexible and non nullable
                        var l7 int
                        l7 = int(binary.BigEndian.Uint32(buff[offset:]))
                        offset += 4
                        members[i0].Metadata = common.ByteSliceCopy(buff[offset: offset + l7])
                        offset += l7
                    }
                }
                if version >= 6 {
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
        m.Members = members
        }
    }
    if version >= 6 {
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

func (m *JoinGroupResponse) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    if version >= 2 {
        // writing m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        buff = binary.BigEndian.AppendUint32(buff, uint32(m.ThrottleTimeMs))
    }
    // writing m.ErrorCode: The error code, or 0 if there was no error.
    buff = binary.BigEndian.AppendUint16(buff, uint16(m.ErrorCode))
    // writing m.GenerationId: The generation ID of the group.
    buff = binary.BigEndian.AppendUint32(buff, uint32(m.GenerationId))
    if version >= 7 {
        // writing m.ProtocolType: The group protocol name.
        // flexible and nullable
        if m.ProtocolType == nil {
            // null
            buff = append(buff, 0)
        } else {
            // not null
            buff = binary.AppendUvarint(buff, uint64(len(*m.ProtocolType) + 1))
        }
        if m.ProtocolType != nil {
            buff = append(buff, *m.ProtocolType...)
        }
    }
    // writing m.ProtocolName: The group protocol selected by the coordinator.
    if version >= 6 {
        if version >= 7 {
            // flexible and nullable
            if m.ProtocolName == nil {
                // null
                buff = append(buff, 0)
            } else {
                // not null
                buff = binary.AppendUvarint(buff, uint64(len(*m.ProtocolName) + 1))
            }
        } else {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*m.ProtocolName) + 1))
        }
    } else {
        if version >= 7 {
            // non flexible and nullable
            if m.ProtocolName == nil {
                // null
                buff = binary.BigEndian.AppendUint16(buff, 65535)
            } else {
                // not null
                buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.ProtocolName)))
            }
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.ProtocolName)))
        }
    }
    if m.ProtocolName != nil {
        buff = append(buff, *m.ProtocolName...)
    }
    // writing m.Leader: The leader of the group.
    if version >= 6 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(*m.Leader) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.Leader)))
    }
    if m.Leader != nil {
        buff = append(buff, *m.Leader...)
    }
    if version >= 9 {
        // writing m.SkipAssignment: True if the leader must skip running the assignment.
        if m.SkipAssignment {
            buff = append(buff, 1)
        } else {
            buff = append(buff, 0)
        }
    }
    // writing m.MemberId: The member ID assigned by the group coordinator.
    if version >= 6 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(*m.MemberId) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.MemberId)))
    }
    if m.MemberId != nil {
        buff = append(buff, *m.MemberId...)
    }
    // writing m.Members: 
    if version >= 6 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.Members) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.Members)))
    }
    for _, members := range m.Members {
        // writing non tagged fields
        // writing members.MemberId: The group member ID.
        if version >= 6 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*members.MemberId) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*members.MemberId)))
        }
        if members.MemberId != nil {
            buff = append(buff, *members.MemberId...)
        }
        if version >= 5 {
            // writing members.GroupInstanceId: The unique identifier of the consumer instance provided by end user.
            if version >= 6 {
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
        // writing members.Metadata: The group member metadata.
        if version >= 6 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(members.Metadata) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint32(buff, uint32(len(members.Metadata)))
        }
        if members.Metadata != nil {
            buff = append(buff, members.Metadata...)
        }
        if version >= 6 {
            numTaggedFields12 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields12))
        }
    }
    if version >= 6 {
        numTaggedFields13 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields13))
    }
    return buff
}

func (m *JoinGroupResponse) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    if version >= 2 {
        // size for m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        size += 4
    }
    // size for m.ErrorCode: The error code, or 0 if there was no error.
    size += 2
    // size for m.GenerationId: The generation ID of the group.
    size += 4
    if version >= 7 {
        // size for m.ProtocolType: The group protocol name.
        // flexible and nullable
        if m.ProtocolType == nil {
            // null
            size += 1
        } else {
            // not null
            size += sizeofUvarint(len(*m.ProtocolType) + 1)
        }
        if m.ProtocolType != nil {
            size += len(*m.ProtocolType)
        }
    }
    // size for m.ProtocolName: The group protocol selected by the coordinator.
    if version >= 6 {
        if version >= 7 {
            // flexible and nullable
            if m.ProtocolName == nil {
                // null
                size += 1
            } else {
                // not null
                size += sizeofUvarint(len(*m.ProtocolName) + 1)
            }
        } else {
            // flexible and not nullable
            size += sizeofUvarint(len(*m.ProtocolName) + 1)
        }
    } else {
        if version >= 7 {
            // non flexible and nullable
            size += 2
        } else {
            // non flexible and non nullable
            size += 2
        }
    }
    if m.ProtocolName != nil {
        size += len(*m.ProtocolName)
    }
    // size for m.Leader: The leader of the group.
    if version >= 6 {
        // flexible and not nullable
        size += sizeofUvarint(len(*m.Leader) + 1)
    } else {
        // non flexible and non nullable
        size += 2
    }
    if m.Leader != nil {
        size += len(*m.Leader)
    }
    if version >= 9 {
        // size for m.SkipAssignment: True if the leader must skip running the assignment.
        size += 1
    }
    // size for m.MemberId: The member ID assigned by the group coordinator.
    if version >= 6 {
        // flexible and not nullable
        size += sizeofUvarint(len(*m.MemberId) + 1)
    } else {
        // non flexible and non nullable
        size += 2
    }
    if m.MemberId != nil {
        size += len(*m.MemberId)
    }
    // size for m.Members: 
    if version >= 6 {
        // flexible and not nullable
        size += sizeofUvarint(len(m.Members) + 1)
    } else {
        // non flexible and non nullable
        size += 4
    }
    for _, members := range m.Members {
        size += 0 * int(unsafe.Sizeof(members)) // hack to make sure loop variable is always used
        // calculating size for non tagged fields
        numTaggedFields1:= 0
        numTaggedFields1 += 0
        // size for members.MemberId: The group member ID.
        if version >= 6 {
            // flexible and not nullable
            size += sizeofUvarint(len(*members.MemberId) + 1)
        } else {
            // non flexible and non nullable
            size += 2
        }
        if members.MemberId != nil {
            size += len(*members.MemberId)
        }
        if version >= 5 {
            // size for members.GroupInstanceId: The unique identifier of the consumer instance provided by end user.
            if version >= 6 {
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
        // size for members.Metadata: The group member metadata.
        if version >= 6 {
            // flexible and not nullable
            size += sizeofUvarint(len(members.Metadata) + 1)
        } else {
            // non flexible and non nullable
            size += 4
        }
        if members.Metadata != nil {
            size += len(members.Metadata)
        }
        numTaggedFields2:= 0
        numTaggedFields2 += 0
        if version >= 6 {
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields2)
        }
    }
    numTaggedFields3:= 0
    numTaggedFields3 += 0
    if version >= 6 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields3)
    }
    return size, tagSizes
}


