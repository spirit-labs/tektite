// Package protocol - This is a generated file, please do not edit

package protocol

import "encoding/binary"
import "unsafe"

type LeaveGroupResponseMemberResponse struct {
    // The member ID to remove from the group.
    MemberId *string
    // The group instance ID to remove from the group.
    GroupInstanceId *string
    // The error code, or 0 if there was no error.
    ErrorCode int16
}

type LeaveGroupResponse struct {
    // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ThrottleTimeMs int32
    // The error code, or 0 if there was no error.
    ErrorCode int16
    // List of leaving member responses.
    Members []LeaveGroupResponseMemberResponse
}

func (m *LeaveGroupResponse) Read(version int16, buff []byte) (int, error) {
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
        // reading m.ErrorCode: The error code, or 0 if there was no error.
        m.ErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
        offset += 2
    }
    if version >= 3 {
        {
            // reading m.Members: List of leaving member responses.
            var l0 int
            if version >= 4 {
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
                members := make([]LeaveGroupResponseMemberResponse, l0)
                for i0 := 0; i0 < l0; i0++ {
                    // reading non tagged fields
                    {
                        // reading members[i0].MemberId: The member ID to remove from the group.
                        if version >= 4 {
                            // flexible and not nullable
                            u, n := binary.Uvarint(buff[offset:])
                            offset += n
                            l1 := int(u - 1)
                            s := string(buff[offset: offset + l1])
                            members[i0].MemberId = &s
                            offset += l1
                        } else {
                            // non flexible and non nullable
                            var l1 int
                            l1 = int(binary.BigEndian.Uint16(buff[offset:]))
                            offset += 2
                            s := string(buff[offset: offset + l1])
                            members[i0].MemberId = &s
                            offset += l1
                        }
                    }
                    {
                        // reading members[i0].GroupInstanceId: The group instance ID to remove from the group.
                        if version >= 4 {
                            // flexible and nullable
                            u, n := binary.Uvarint(buff[offset:])
                            offset += n
                            l2 := int(u - 1)
                            if l2 > 0 {
                                s := string(buff[offset: offset + l2])
                                members[i0].GroupInstanceId = &s
                                offset += l2
                            } else {
                                members[i0].GroupInstanceId = nil
                            }
                        } else {
                            // non flexible and nullable
                            var l2 int
                            l2 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
                            offset += 2
                            if l2 > 0 {
                                s := string(buff[offset: offset + l2])
                                members[i0].GroupInstanceId = &s
                                offset += l2
                            } else {
                                members[i0].GroupInstanceId = nil
                            }
                        }
                    }
                    {
                        // reading members[i0].ErrorCode: The error code, or 0 if there was no error.
                        members[i0].ErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
                        offset += 2
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

func (m *LeaveGroupResponse) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    if version >= 1 {
        // writing m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        buff = binary.BigEndian.AppendUint32(buff, uint32(m.ThrottleTimeMs))
    }
    // writing m.ErrorCode: The error code, or 0 if there was no error.
    buff = binary.BigEndian.AppendUint16(buff, uint16(m.ErrorCode))
    if version >= 3 {
        // writing m.Members: List of leaving member responses.
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
            // writing members.ErrorCode: The error code, or 0 if there was no error.
            buff = binary.BigEndian.AppendUint16(buff, uint16(members.ErrorCode))
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

func (m *LeaveGroupResponse) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    if version >= 1 {
        // size for m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        size += 4
    }
    // size for m.ErrorCode: The error code, or 0 if there was no error.
    size += 2
    if version >= 3 {
        // size for m.Members: List of leaving member responses.
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
            // size for members.ErrorCode: The error code, or 0 if there was no error.
            size += 2
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

