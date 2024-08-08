// Package protocol - This is a generated file, please do not edit

package protocol

import "encoding/binary"

type HeartbeatRequest struct {
    // The group id.
    GroupId *string
    // The generation of the group.
    GenerationId int32
    // The member ID.
    MemberId *string
    // The unique identifier of the consumer instance provided by end user.
    GroupInstanceId *string
}

func (m *HeartbeatRequest) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.GroupId: The group id.
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
        // reading m.MemberId: The member ID.
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

func (m *HeartbeatRequest) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.GroupId: The group id.
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
    // writing m.MemberId: The member ID.
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
    if version >= 4 {
        numTaggedFields4 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields4))
    }
    return buff
}

func (m *HeartbeatRequest) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.GroupId: The group id.
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
    // size for m.MemberId: The member ID.
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
    numTaggedFields1:= 0
    numTaggedFields1 += 0
    if version >= 4 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields1)
    }
    return size, tagSizes
}

func (m *HeartbeatRequest) HeaderVersions(version int16) (int16, int16) {
    if version >= 4 {
        return 2, 1
    } else {
        return 1, 0
    }
}
