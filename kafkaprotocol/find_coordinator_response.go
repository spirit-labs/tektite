// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"
import "unsafe"

type FindCoordinatorResponseCoordinator struct {
    // The coordinator key.
    Key *string
    // The node id.
    NodeId int32
    // The host name.
    Host *string
    // The port.
    Port int32
    // The error code, or 0 if there was no error.
    ErrorCode int16
    // The error message, or null if there was no error.
    ErrorMessage *string
}

type FindCoordinatorResponse struct {
    // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ThrottleTimeMs int32
    // The error code, or 0 if there was no error.
    ErrorCode int16
    // The error message, or null if there was no error.
    ErrorMessage *string
    // The node id.
    NodeId int32
    // The host name.
    Host *string
    // The port.
    Port int32
    // Each coordinator result in the response
    Coordinators []FindCoordinatorResponseCoordinator
}

func (m *FindCoordinatorResponse) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    if version >= 1 {
        {
            // reading m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
            m.ThrottleTimeMs = int32(binary.BigEndian.Uint32(buff[offset:]))
            offset += 4
        }
    }
    if version <= 3 {
        {
            // reading m.ErrorCode: The error code, or 0 if there was no error.
            m.ErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
            offset += 2
        }
    }
    if version >= 1 && version <= 3 {
        {
            // reading m.ErrorMessage: The error message, or null if there was no error.
            if version >= 3 {
                if version >= 1 && version <= 3 {
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
                    // flexible and not nullable
                    u, n := binary.Uvarint(buff[offset:])
                    offset += n
                    l0 := int(u - 1)
                    s := string(buff[offset: offset + l0])
                    m.ErrorMessage = &s
                    offset += l0
                }
            } else {
                if version >= 1 && version <= 3 {
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
                } else {
                    // non flexible and non nullable
                    var l0 int
                    l0 = int(binary.BigEndian.Uint16(buff[offset:]))
                    offset += 2
                    s := string(buff[offset: offset + l0])
                    m.ErrorMessage = &s
                    offset += l0
                }
            }
        }
    }
    if version <= 3 {
        {
            // reading m.NodeId: The node id.
            m.NodeId = int32(binary.BigEndian.Uint32(buff[offset:]))
            offset += 4
        }
        {
            // reading m.Host: The host name.
            if version >= 3 {
                // flexible and not nullable
                u, n := binary.Uvarint(buff[offset:])
                offset += n
                l1 := int(u - 1)
                s := string(buff[offset: offset + l1])
                m.Host = &s
                offset += l1
            } else {
                // non flexible and non nullable
                var l1 int
                l1 = int(binary.BigEndian.Uint16(buff[offset:]))
                offset += 2
                s := string(buff[offset: offset + l1])
                m.Host = &s
                offset += l1
            }
        }
        {
            // reading m.Port: The port.
            m.Port = int32(binary.BigEndian.Uint32(buff[offset:]))
            offset += 4
        }
    }
    if version >= 4 {
        {
            // reading m.Coordinators: Each coordinator result in the response
            var l2 int
            // flexible and not nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l2 = int(u - 1)
            if l2 >= 0 {
                // length will be -1 if field is null
                coordinators := make([]FindCoordinatorResponseCoordinator, l2)
                for i0 := 0; i0 < l2; i0++ {
                    // reading non tagged fields
                    {
                        // reading coordinators[i0].Key: The coordinator key.
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l3 := int(u - 1)
                        s := string(buff[offset: offset + l3])
                        coordinators[i0].Key = &s
                        offset += l3
                    }
                    {
                        // reading coordinators[i0].NodeId: The node id.
                        coordinators[i0].NodeId = int32(binary.BigEndian.Uint32(buff[offset:]))
                        offset += 4
                    }
                    {
                        // reading coordinators[i0].Host: The host name.
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l4 := int(u - 1)
                        s := string(buff[offset: offset + l4])
                        coordinators[i0].Host = &s
                        offset += l4
                    }
                    {
                        // reading coordinators[i0].Port: The port.
                        coordinators[i0].Port = int32(binary.BigEndian.Uint32(buff[offset:]))
                        offset += 4
                    }
                    {
                        // reading coordinators[i0].ErrorCode: The error code, or 0 if there was no error.
                        coordinators[i0].ErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
                        offset += 2
                    }
                    {
                        // reading coordinators[i0].ErrorMessage: The error message, or null if there was no error.
                        // flexible and nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l5 := int(u - 1)
                        if l5 > 0 {
                            s := string(buff[offset: offset + l5])
                            coordinators[i0].ErrorMessage = &s
                            offset += l5
                        } else {
                            coordinators[i0].ErrorMessage = nil
                        }
                    }
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
            m.Coordinators = coordinators
            }
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

func (m *FindCoordinatorResponse) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    if version >= 1 {
        // writing m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        buff = binary.BigEndian.AppendUint32(buff, uint32(m.ThrottleTimeMs))
    }
    if version <= 3 {
        // writing m.ErrorCode: The error code, or 0 if there was no error.
        buff = binary.BigEndian.AppendUint16(buff, uint16(m.ErrorCode))
    }
    if version >= 1 && version <= 3 {
        // writing m.ErrorMessage: The error message, or null if there was no error.
        if version >= 3 {
            if version >= 1 && version <= 3 {
                // flexible and nullable
                if m.ErrorMessage == nil {
                    // null
                    buff = append(buff, 0)
                } else {
                    // not null
                    buff = binary.AppendUvarint(buff, uint64(len(*m.ErrorMessage) + 1))
                }
            } else {
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(*m.ErrorMessage) + 1))
            }
        } else {
            if version >= 1 && version <= 3 {
                // non flexible and nullable
                if m.ErrorMessage == nil {
                    // null
                    buff = binary.BigEndian.AppendUint16(buff, 65535)
                } else {
                    // not null
                    buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.ErrorMessage)))
                }
            } else {
                // non flexible and non nullable
                buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.ErrorMessage)))
            }
        }
        if m.ErrorMessage != nil {
            buff = append(buff, *m.ErrorMessage...)
        }
    }
    if version <= 3 {
        // writing m.NodeId: The node id.
        buff = binary.BigEndian.AppendUint32(buff, uint32(m.NodeId))
        // writing m.Host: The host name.
        if version >= 3 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*m.Host) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.Host)))
        }
        if m.Host != nil {
            buff = append(buff, *m.Host...)
        }
        // writing m.Port: The port.
        buff = binary.BigEndian.AppendUint32(buff, uint32(m.Port))
    }
    if version >= 4 {
        // writing m.Coordinators: Each coordinator result in the response
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.Coordinators) + 1))
        for _, coordinators := range m.Coordinators {
            // writing non tagged fields
            // writing coordinators.Key: The coordinator key.
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*coordinators.Key) + 1))
            if coordinators.Key != nil {
                buff = append(buff, *coordinators.Key...)
            }
            // writing coordinators.NodeId: The node id.
            buff = binary.BigEndian.AppendUint32(buff, uint32(coordinators.NodeId))
            // writing coordinators.Host: The host name.
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*coordinators.Host) + 1))
            if coordinators.Host != nil {
                buff = append(buff, *coordinators.Host...)
            }
            // writing coordinators.Port: The port.
            buff = binary.BigEndian.AppendUint32(buff, uint32(coordinators.Port))
            // writing coordinators.ErrorCode: The error code, or 0 if there was no error.
            buff = binary.BigEndian.AppendUint16(buff, uint16(coordinators.ErrorCode))
            // writing coordinators.ErrorMessage: The error message, or null if there was no error.
            // flexible and nullable
            if coordinators.ErrorMessage == nil {
                // null
                buff = append(buff, 0)
            } else {
                // not null
                buff = binary.AppendUvarint(buff, uint64(len(*coordinators.ErrorMessage) + 1))
            }
            if coordinators.ErrorMessage != nil {
                buff = append(buff, *coordinators.ErrorMessage...)
            }
            numTaggedFields13 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields13))
        }
    }
    if version >= 3 {
        numTaggedFields14 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields14))
    }
    return buff
}

func (m *FindCoordinatorResponse) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    if version >= 1 {
        // size for m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        size += 4
    }
    if version <= 3 {
        // size for m.ErrorCode: The error code, or 0 if there was no error.
        size += 2
    }
    if version >= 1 && version <= 3 {
        // size for m.ErrorMessage: The error message, or null if there was no error.
        if version >= 3 {
            if version >= 1 && version <= 3 {
                // flexible and nullable
                if m.ErrorMessage == nil {
                    // null
                    size += 1
                } else {
                    // not null
                    size += sizeofUvarint(len(*m.ErrorMessage) + 1)
                }
            } else {
                // flexible and not nullable
                size += sizeofUvarint(len(*m.ErrorMessage) + 1)
            }
        } else {
            if version >= 1 && version <= 3 {
                // non flexible and nullable
                size += 2
            } else {
                // non flexible and non nullable
                size += 2
            }
        }
        if m.ErrorMessage != nil {
            size += len(*m.ErrorMessage)
        }
    }
    if version <= 3 {
        // size for m.NodeId: The node id.
        size += 4
        // size for m.Host: The host name.
        if version >= 3 {
            // flexible and not nullable
            size += sizeofUvarint(len(*m.Host) + 1)
        } else {
            // non flexible and non nullable
            size += 2
        }
        if m.Host != nil {
            size += len(*m.Host)
        }
        // size for m.Port: The port.
        size += 4
    }
    if version >= 4 {
        // size for m.Coordinators: Each coordinator result in the response
        // flexible and not nullable
        size += sizeofUvarint(len(m.Coordinators) + 1)
        for _, coordinators := range m.Coordinators {
            size += 0 * int(unsafe.Sizeof(coordinators)) // hack to make sure loop variable is always used
            // calculating size for non tagged fields
            numTaggedFields1:= 0
            numTaggedFields1 += 0
            // size for coordinators.Key: The coordinator key.
            // flexible and not nullable
            size += sizeofUvarint(len(*coordinators.Key) + 1)
            if coordinators.Key != nil {
                size += len(*coordinators.Key)
            }
            // size for coordinators.NodeId: The node id.
            size += 4
            // size for coordinators.Host: The host name.
            // flexible and not nullable
            size += sizeofUvarint(len(*coordinators.Host) + 1)
            if coordinators.Host != nil {
                size += len(*coordinators.Host)
            }
            // size for coordinators.Port: The port.
            size += 4
            // size for coordinators.ErrorCode: The error code, or 0 if there was no error.
            size += 2
            // size for coordinators.ErrorMessage: The error message, or null if there was no error.
            // flexible and nullable
            if coordinators.ErrorMessage == nil {
                // null
                size += 1
            } else {
                // not null
                size += sizeofUvarint(len(*coordinators.ErrorMessage) + 1)
            }
            if coordinators.ErrorMessage != nil {
                size += len(*coordinators.ErrorMessage)
            }
            numTaggedFields2:= 0
            numTaggedFields2 += 0
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


