// Package protocol - This is a generated file, please do not edit

package protocol

import "encoding/binary"
import "github.com/spirit-labs/tektite/common"
import "unsafe"

type SyncGroupRequestSyncGroupRequestAssignment struct {
    // The ID of the member to assign.
    MemberId *string
    // The member assignment.
    Assignment []byte
}

type SyncGroupRequest struct {
    // The unique group identifier.
    GroupId *string
    // The generation of the group.
    GenerationId int32
    // The member ID assigned by the group.
    MemberId *string
    // The unique identifier of the consumer instance provided by end user.
    GroupInstanceId *string
    // The group protocol type.
    ProtocolType *string
    // The group protocol name.
    ProtocolName *string
    // Each assignment.
    Assignments []SyncGroupRequestSyncGroupRequestAssignment
}

func (m *SyncGroupRequest) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.GroupId: The unique group identifier.
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
    {
        // reading m.GenerationId: The generation of the group.
        m.GenerationId = int32(binary.BigEndian.Uint32(buff[offset:]))
        offset += 4
    }
    {
        // reading m.MemberId: The member ID assigned by the group.
        if version >= 4 {
            // flexible and not nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l1 := int(u - 1)
            s := string(buff[offset: offset + l1])
            m.MemberId = &s
            offset += l1
        } else {
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
            // reading m.GroupInstanceId: The unique identifier of the consumer instance provided by end user.
            if version >= 4 {
                // flexible and nullable
                u, n := binary.Uvarint(buff[offset:])
                offset += n
                l2 := int(u - 1)
                if l2 > 0 {
                    s := string(buff[offset: offset + l2])
                    m.GroupInstanceId = &s
                    offset += l2
                } else {
                    m.GroupInstanceId = nil
                }
            } else {
                // non flexible and nullable
                var l2 int
                l2 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
                offset += 2
                if l2 > 0 {
                    s := string(buff[offset: offset + l2])
                    m.GroupInstanceId = &s
                    offset += l2
                } else {
                    m.GroupInstanceId = nil
                }
            }
        }
    }
    if version >= 5 {
        {
            // reading m.ProtocolType: The group protocol type.
            // flexible and nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l3 := int(u - 1)
            if l3 > 0 {
                s := string(buff[offset: offset + l3])
                m.ProtocolType = &s
                offset += l3
            } else {
                m.ProtocolType = nil
            }
        }
        {
            // reading m.ProtocolName: The group protocol name.
            // flexible and nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l4 := int(u - 1)
            if l4 > 0 {
                s := string(buff[offset: offset + l4])
                m.ProtocolName = &s
                offset += l4
            } else {
                m.ProtocolName = nil
            }
        }
    }
    {
        // reading m.Assignments: Each assignment.
        var l5 int
        if version >= 4 {
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
            assignments := make([]SyncGroupRequestSyncGroupRequestAssignment, l5)
            for i0 := 0; i0 < l5; i0++ {
                // reading non tagged fields
                {
                    // reading assignments[i0].MemberId: The ID of the member to assign.
                    if version >= 4 {
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l6 := int(u - 1)
                        s := string(buff[offset: offset + l6])
                        assignments[i0].MemberId = &s
                        offset += l6
                    } else {
                        // non flexible and non nullable
                        var l6 int
                        l6 = int(binary.BigEndian.Uint16(buff[offset:]))
                        offset += 2
                        s := string(buff[offset: offset + l6])
                        assignments[i0].MemberId = &s
                        offset += l6
                    }
                }
                {
                    // reading assignments[i0].Assignment: The member assignment.
                    if version >= 4 {
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l7 := int(u - 1)
                        assignments[i0].Assignment = common.ByteSliceCopy(buff[offset: offset + l7])
                        offset += l7
                    } else {
                        // non flexible and non nullable
                        var l7 int
                        l7 = int(binary.BigEndian.Uint32(buff[offset:]))
                        offset += 4
                        assignments[i0].Assignment = common.ByteSliceCopy(buff[offset: offset + l7])
                        offset += l7
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
        m.Assignments = assignments
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

func (m *SyncGroupRequest) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.GroupId: The unique group identifier.
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
    // writing m.GenerationId: The generation of the group.
    buff = binary.BigEndian.AppendUint32(buff, uint32(m.GenerationId))
    // writing m.MemberId: The member ID assigned by the group.
    if version >= 4 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(*m.MemberId) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.MemberId)))
    }
    if m.MemberId != nil {
        buff = append(buff, *m.MemberId...)
    }
    if version >= 3 {
        // writing m.GroupInstanceId: The unique identifier of the consumer instance provided by end user.
        if version >= 4 {
            // flexible and nullable
            if m.GroupInstanceId == nil {
                // null
                buff = append(buff, 0)
            } else {
                // not null
                buff = binary.AppendUvarint(buff, uint64(len(*m.GroupInstanceId) + 1))
            }
        } else {
            // non flexible and nullable
            if m.GroupInstanceId == nil {
                // null
                buff = binary.BigEndian.AppendUint16(buff, 65535)
            } else {
                // not null
                buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.GroupInstanceId)))
            }
        }
        if m.GroupInstanceId != nil {
            buff = append(buff, *m.GroupInstanceId...)
        }
    }
    if version >= 5 {
        // writing m.ProtocolType: The group protocol type.
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
        // writing m.ProtocolName: The group protocol name.
        // flexible and nullable
        if m.ProtocolName == nil {
            // null
            buff = append(buff, 0)
        } else {
            // not null
            buff = binary.AppendUvarint(buff, uint64(len(*m.ProtocolName) + 1))
        }
        if m.ProtocolName != nil {
            buff = append(buff, *m.ProtocolName...)
        }
    }
    // writing m.Assignments: Each assignment.
    if version >= 4 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.Assignments) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.Assignments)))
    }
    for _, assignments := range m.Assignments {
        // writing non tagged fields
        // writing assignments.MemberId: The ID of the member to assign.
        if version >= 4 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*assignments.MemberId) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*assignments.MemberId)))
        }
        if assignments.MemberId != nil {
            buff = append(buff, *assignments.MemberId...)
        }
        // writing assignments.Assignment: The member assignment.
        if version >= 4 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(assignments.Assignment) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint32(buff, uint32(len(assignments.Assignment)))
        }
        if assignments.Assignment != nil {
            buff = append(buff, assignments.Assignment...)
        }
        if version >= 4 {
            numTaggedFields9 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields9))
        }
    }
    if version >= 4 {
        numTaggedFields10 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields10))
    }
    return buff
}

func (m *SyncGroupRequest) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.GroupId: The unique group identifier.
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
    // size for m.GenerationId: The generation of the group.
    size += 4
    // size for m.MemberId: The member ID assigned by the group.
    if version >= 4 {
        // flexible and not nullable
        size += sizeofUvarint(len(*m.MemberId) + 1)
    } else {
        // non flexible and non nullable
        size += 2
    }
    if m.MemberId != nil {
        size += len(*m.MemberId)
    }
    if version >= 3 {
        // size for m.GroupInstanceId: The unique identifier of the consumer instance provided by end user.
        if version >= 4 {
            // flexible and nullable
            if m.GroupInstanceId == nil {
                // null
                size += 1
            } else {
                // not null
                size += sizeofUvarint(len(*m.GroupInstanceId) + 1)
            }
        } else {
            // non flexible and nullable
            size += 2
        }
        if m.GroupInstanceId != nil {
            size += len(*m.GroupInstanceId)
        }
    }
    if version >= 5 {
        // size for m.ProtocolType: The group protocol type.
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
        // size for m.ProtocolName: The group protocol name.
        // flexible and nullable
        if m.ProtocolName == nil {
            // null
            size += 1
        } else {
            // not null
            size += sizeofUvarint(len(*m.ProtocolName) + 1)
        }
        if m.ProtocolName != nil {
            size += len(*m.ProtocolName)
        }
    }
    // size for m.Assignments: Each assignment.
    if version >= 4 {
        // flexible and not nullable
        size += sizeofUvarint(len(m.Assignments) + 1)
    } else {
        // non flexible and non nullable
        size += 4
    }
    for _, assignments := range m.Assignments {
        size += 0 * int(unsafe.Sizeof(assignments)) // hack to make sure loop variable is always used
        // calculating size for non tagged fields
        numTaggedFields1:= 0
        numTaggedFields1 += 0
        // size for assignments.MemberId: The ID of the member to assign.
        if version >= 4 {
            // flexible and not nullable
            size += sizeofUvarint(len(*assignments.MemberId) + 1)
        } else {
            // non flexible and non nullable
            size += 2
        }
        if assignments.MemberId != nil {
            size += len(*assignments.MemberId)
        }
        // size for assignments.Assignment: The member assignment.
        if version >= 4 {
            // flexible and not nullable
            size += sizeofUvarint(len(assignments.Assignment) + 1)
        } else {
            // non flexible and non nullable
            size += 4
        }
        if assignments.Assignment != nil {
            size += len(assignments.Assignment)
        }
        numTaggedFields2:= 0
        numTaggedFields2 += 0
        if version >= 4 {
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields2)
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

func (m *SyncGroupRequest) HeaderVersions(version int16) (int16, int16) {
    if version >= 4 {
        return 2, 1
    } else {
        return 1, 0
    }
}

func (m *SyncGroupRequest) SupportedApiVersions() (int16, int16) {
    return 0, 0
}
