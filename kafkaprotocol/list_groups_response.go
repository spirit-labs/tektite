// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"
import "unsafe"

type ListGroupsResponseListedGroup struct {
    // The group ID.
    GroupId *string
    // The group protocol type.
    ProtocolType *string
    // The group state name.
    GroupState *string
    // The group type name.
    GroupType *string
}

type ListGroupsResponse struct {
    // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ThrottleTimeMs int32
    // The error code, or 0 if there was no error.
    ErrorCode int16
    // Each group in the response.
    Groups []ListGroupsResponseListedGroup
}

func (m *ListGroupsResponse) Read(version int16, buff []byte) (int, error) {
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
    {
        // reading m.Groups: Each group in the response.
        var l0 int
        if version >= 3 {
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
            groups := make([]ListGroupsResponseListedGroup, l0)
            for i0 := 0; i0 < l0; i0++ {
                // reading non tagged fields
                {
                    // reading groups[i0].GroupId: The group ID.
                    if version >= 3 {
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
                    // reading groups[i0].ProtocolType: The group protocol type.
                    if version >= 3 {
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l2 := int(u - 1)
                        s := string(buff[offset: offset + l2])
                        groups[i0].ProtocolType = &s
                        offset += l2
                    } else {
                        // non flexible and non nullable
                        var l2 int
                        l2 = int(binary.BigEndian.Uint16(buff[offset:]))
                        offset += 2
                        s := string(buff[offset: offset + l2])
                        groups[i0].ProtocolType = &s
                        offset += l2
                    }
                }
                if version >= 4 {
                    {
                        // reading groups[i0].GroupState: The group state name.
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l3 := int(u - 1)
                        s := string(buff[offset: offset + l3])
                        groups[i0].GroupState = &s
                        offset += l3
                    }
                }
                if version >= 5 {
                    {
                        // reading groups[i0].GroupType: The group type name.
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l4 := int(u - 1)
                        s := string(buff[offset: offset + l4])
                        groups[i0].GroupType = &s
                        offset += l4
                    }
                }
                if version >= 3 {
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
    if version >= 3 {
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

func (m *ListGroupsResponse) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    if version >= 1 {
        // writing m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        buff = binary.BigEndian.AppendUint32(buff, uint32(m.ThrottleTimeMs))
    }
    // writing m.ErrorCode: The error code, or 0 if there was no error.
    buff = binary.BigEndian.AppendUint16(buff, uint16(m.ErrorCode))
    // writing m.Groups: Each group in the response.
    if version >= 3 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.Groups) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.Groups)))
    }
    for _, groups := range m.Groups {
        // writing non tagged fields
        // writing groups.GroupId: The group ID.
        if version >= 3 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*groups.GroupId) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*groups.GroupId)))
        }
        if groups.GroupId != nil {
            buff = append(buff, *groups.GroupId...)
        }
        // writing groups.ProtocolType: The group protocol type.
        if version >= 3 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*groups.ProtocolType) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*groups.ProtocolType)))
        }
        if groups.ProtocolType != nil {
            buff = append(buff, *groups.ProtocolType...)
        }
        if version >= 4 {
            // writing groups.GroupState: The group state name.
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*groups.GroupState) + 1))
            if groups.GroupState != nil {
                buff = append(buff, *groups.GroupState...)
            }
        }
        if version >= 5 {
            // writing groups.GroupType: The group type name.
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*groups.GroupType) + 1))
            if groups.GroupType != nil {
                buff = append(buff, *groups.GroupType...)
            }
        }
        if version >= 3 {
            numTaggedFields7 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields7))
        }
    }
    if version >= 3 {
        numTaggedFields8 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields8))
    }
    return buff
}

func (m *ListGroupsResponse) CalcSize(version int16, tagSizes []int) (int, []int) {
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
    // size for m.Groups: Each group in the response.
    if version >= 3 {
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
        // size for groups.GroupId: The group ID.
        if version >= 3 {
            // flexible and not nullable
            size += sizeofUvarint(len(*groups.GroupId) + 1)
        } else {
            // non flexible and non nullable
            size += 2
        }
        if groups.GroupId != nil {
            size += len(*groups.GroupId)
        }
        // size for groups.ProtocolType: The group protocol type.
        if version >= 3 {
            // flexible and not nullable
            size += sizeofUvarint(len(*groups.ProtocolType) + 1)
        } else {
            // non flexible and non nullable
            size += 2
        }
        if groups.ProtocolType != nil {
            size += len(*groups.ProtocolType)
        }
        if version >= 4 {
            // size for groups.GroupState: The group state name.
            // flexible and not nullable
            size += sizeofUvarint(len(*groups.GroupState) + 1)
            if groups.GroupState != nil {
                size += len(*groups.GroupState)
            }
        }
        if version >= 5 {
            // size for groups.GroupType: The group type name.
            // flexible and not nullable
            size += sizeofUvarint(len(*groups.GroupType) + 1)
            if groups.GroupType != nil {
                size += len(*groups.GroupType)
            }
        }
        numTaggedFields2:= 0
        numTaggedFields2 += 0
        if version >= 3 {
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields2)
        }
    }
    numTaggedFields3:= 0
    numTaggedFields3 += 0
    if version >= 3 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields3)
    }
    return size, tagSizes
}


