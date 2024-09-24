// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"
import "unsafe"

type LeaveGroupRequestMemberIdentity struct {
    // The member ID to remove from the group.
    MemberId *string
    // The group instance ID to remove from the group.
    GroupInstanceId *string
    // The reason why the member left the group.
    Reason *string
}

type LeaveGroupRequest struct {
    // The ID of the group to leave.
    GroupId *string
    // The member ID to remove from the group.
    MemberId *string
    // List of leaving member identities.
    Members []LeaveGroupRequestMemberIdentity
}

func (m *LeaveGroupRequest) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.GroupId: The ID of the group to leave.
        if version >= 4 {
            // flexible and not nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l0 := int(u - 1)
            s := string(buff[offset: offset + l0])
            m.GroupId = &s
            offset += l0
        } else {
            // non flexible and non nullable
            var l0 int
            l0 = int(binary.BigEndian.Uint16(buff[offset:]))
            offset += 2
            s := string(buff[offset: offset + l0])
            m.GroupId = &s
            offset += l0
        }
    }
    if version <= 2 {
        {
            // reading m.MemberId: The member ID to remove from the group.
            // non flexible and non nullable
            var l1 int
            l1 = int(binary.BigEndian.Uint16(buff[offset:]))
            offset += 2
            s := string(buff[offset: offset + l1])
            m.MemberId = &s
            offset += l1
        }
    }
    if version >= 3 {
        {
            // reading m.Members: List of leaving member identities.
            var l2 int
            if version >= 4 {
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
                members := make([]LeaveGroupRequestMemberIdentity, l2)
                for i0 := 0; i0 < l2; i0++ {
                    // reading non tagged fields
                    {
                        // reading members[i0].MemberId: The member ID to remove from the group.
                        if version >= 4 {
                            // flexible and not nullable
                            u, n := binary.Uvarint(buff[offset:])
                            offset += n
                            l3 := int(u - 1)
                            s := string(buff[offset: offset + l3])
                            members[i0].MemberId = &s
                            offset += l3
                        } else {
                            // non flexible and non nullable
                            var l3 int
                            l3 = int(binary.BigEndian.Uint16(buff[offset:]))
                            offset += 2
                            s := string(buff[offset: offset + l3])
                            members[i0].MemberId = &s
                            offset += l3
                        }
                    }
                    {
                        // reading members[i0].GroupInstanceId: The group instance ID to remove from the group.
                        if version >= 4 {
                            // flexible and nullable
                            u, n := binary.Uvarint(buff[offset:])
                            offset += n
                            l4 := int(u - 1)
                            if l4 > 0 {
                                s := string(buff[offset: offset + l4])
                                members[i0].GroupInstanceId = &s
                                offset += l4
                            } else {
                                members[i0].GroupInstanceId = nil
                            }
                        } else {
                            // non flexible and nullable
                            var l4 int
                            l4 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
                            offset += 2
                            if l4 > 0 {
                                s := string(buff[offset: offset + l4])
                                members[i0].GroupInstanceId = &s
                                offset += l4
                            } else {
                                members[i0].GroupInstanceId = nil
                            }
                        }
                    }
                    if version >= 5 {
                        {
                            // reading members[i0].Reason: The reason why the member left the group.
                            // flexible and nullable
                            u, n := binary.Uvarint(buff[offset:])
                            offset += n
                            l5 := int(u - 1)
                            if l5 > 0 {
                                s := string(buff[offset: offset + l5])
                                members[i0].Reason = &s
                                offset += l5
                            } else {
                                members[i0].Reason = nil
                            }
                        }
                    }
                    if version >= 4 {
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
    }
    if version >= 4 {
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

func (m *LeaveGroupRequest) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.GroupId: The ID of the group to leave.
    if version >= 4 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(*m.GroupId) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.GroupId)))
    }
    if m.GroupId != nil {
        buff = append(buff, *m.GroupId...)
    }
    if version <= 2 {
        // writing m.MemberId: The member ID to remove from the group.
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.MemberId)))
        if m.MemberId != nil {
            buff = append(buff, *m.MemberId...)
        }
    }
    if version >= 3 {
        // writing m.Members: List of leaving member identities.
        if version >= 4 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(m.Members) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.Members)))
        }
        for _, members := range m.Members {
            // writing non tagged fields
            // writing members.MemberId: The member ID to remove from the group.
            if version >= 4 {
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(*members.MemberId) + 1))
            } else {
                // non flexible and non nullable
                buff = binary.BigEndian.AppendUint16(buff, uint16(len(*members.MemberId)))
            }
            if members.MemberId != nil {
                buff = append(buff, *members.MemberId...)
            }
            // writing members.GroupInstanceId: The group instance ID to remove from the group.
            if version >= 4 {
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
            if version >= 5 {
                // writing members.Reason: The reason why the member left the group.
                // flexible and nullable
                if members.Reason == nil {
                    // null
                    buff = append(buff, 0)
                } else {
                    // not null
                    buff = binary.AppendUvarint(buff, uint64(len(*members.Reason) + 1))
                }
                if members.Reason != nil {
                    buff = append(buff, *members.Reason...)
                }
            }
            if version >= 4 {
                numTaggedFields6 := 0
                // write number of tagged fields
                buff = binary.AppendUvarint(buff, uint64(numTaggedFields6))
            }
        }
    }
    if version >= 4 {
        numTaggedFields7 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields7))
    }
    return buff
}

func (m *LeaveGroupRequest) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.GroupId: The ID of the group to leave.
    if version >= 4 {
        // flexible and not nullable
        size += sizeofUvarint(len(*m.GroupId) + 1)
    } else {
        // non flexible and non nullable
        size += 2
    }
    if m.GroupId != nil {
        size += len(*m.GroupId)
    }
    if version <= 2 {
        // size for m.MemberId: The member ID to remove from the group.
        // non flexible and non nullable
        size += 2
        if m.MemberId != nil {
            size += len(*m.MemberId)
        }
    }
    if version >= 3 {
        // size for m.Members: List of leaving member identities.
        if version >= 4 {
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
            // size for members.MemberId: The member ID to remove from the group.
            if version >= 4 {
                // flexible and not nullable
                size += sizeofUvarint(len(*members.MemberId) + 1)
            } else {
                // non flexible and non nullable
                size += 2
            }
            if members.MemberId != nil {
                size += len(*members.MemberId)
            }
            // size for members.GroupInstanceId: The group instance ID to remove from the group.
            if version >= 4 {
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
            if version >= 5 {
                // size for members.Reason: The reason why the member left the group.
                // flexible and nullable
                if members.Reason == nil {
                    // null
                    size += 1
                } else {
                    // not null
                    size += sizeofUvarint(len(*members.Reason) + 1)
                }
                if members.Reason != nil {
                    size += len(*members.Reason)
                }
            }
            numTaggedFields2:= 0
            numTaggedFields2 += 0
            if version >= 4 {
                // writing size of num tagged fields field
                size += sizeofUvarint(numTaggedFields2)
            }
        }
    }
    numTaggedFields3:= 0
    numTaggedFields3 += 0
    if version >= 4 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields3)
    }
    return size, tagSizes
}

func (m *LeaveGroupRequest) HeaderVersions(version int16) (int16, int16) {
    if version >= 4 {
        return 2, 1
    } else {
        return 1, 0
    }
}

func (m *LeaveGroupRequest) SupportedApiVersions() (int16, int16) {
    return 0, 0
}
