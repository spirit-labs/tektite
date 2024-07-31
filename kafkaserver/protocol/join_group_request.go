// Package protocol - This is a generated file, please do not edit

package protocol

import "encoding/binary"
import "github.com/spirit-labs/tektite/common"
import "unsafe"

type JoinGroupRequestJoinGroupRequestProtocol struct {
    // The protocol name.
    Name *string
    // The protocol metadata.
    Metadata []byte
}

type JoinGroupRequest struct {
    // The group identifier.
    GroupId *string
    // The coordinator considers the consumer dead if it receives no heartbeat after this timeout in milliseconds.
    SessionTimeoutMs int32
    // The maximum time in milliseconds that the coordinator will wait for each member to rejoin when rebalancing the group.
    RebalanceTimeoutMs int32
    // The member id assigned by the group coordinator.
    MemberId *string
    // The unique identifier of the consumer instance provided by end user.
    GroupInstanceId *string
    // The unique name the for class of protocols implemented by the group we want to join.
    ProtocolType *string
    // The list of protocols that the member supports.
    Protocols []JoinGroupRequestJoinGroupRequestProtocol
    // The reason why the member (re-)joins the group.
    Reason *string
}

func (m *JoinGroupRequest) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.GroupId: The group identifier.
        if version >= 6 {
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
        // reading m.SessionTimeoutMs: The coordinator considers the consumer dead if it receives no heartbeat after this timeout in milliseconds.
        m.SessionTimeoutMs = int32(binary.BigEndian.Uint32(buff[offset:]))
        offset += 4
    }
    if version >= 1 {
        {
            // reading m.RebalanceTimeoutMs: The maximum time in milliseconds that the coordinator will wait for each member to rejoin when rebalancing the group.
            m.RebalanceTimeoutMs = int32(binary.BigEndian.Uint32(buff[offset:]))
            offset += 4
        }
    }
    {
        // reading m.MemberId: The member id assigned by the group coordinator.
        if version >= 6 {
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
    if version >= 5 {
        {
            // reading m.GroupInstanceId: The unique identifier of the consumer instance provided by end user.
            if version >= 6 {
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
    {
        // reading m.ProtocolType: The unique name the for class of protocols implemented by the group we want to join.
        if version >= 6 {
            // flexible and not nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l3 := int(u - 1)
            s := string(buff[offset: offset + l3])
            m.ProtocolType = &s
            offset += l3
        } else {
            // non flexible and non nullable
            var l3 int
            l3 = int(binary.BigEndian.Uint16(buff[offset:]))
            offset += 2
            s := string(buff[offset: offset + l3])
            m.ProtocolType = &s
            offset += l3
        }
    }
    {
        // reading m.Protocols: The list of protocols that the member supports.
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
            protocols := make([]JoinGroupRequestJoinGroupRequestProtocol, l4)
            for i0 := 0; i0 < l4; i0++ {
                // reading non tagged fields
                {
                    // reading protocols[i0].Name: The protocol name.
                    if version >= 6 {
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l5 := int(u - 1)
                        s := string(buff[offset: offset + l5])
                        protocols[i0].Name = &s
                        offset += l5
                    } else {
                        // non flexible and non nullable
                        var l5 int
                        l5 = int(binary.BigEndian.Uint16(buff[offset:]))
                        offset += 2
                        s := string(buff[offset: offset + l5])
                        protocols[i0].Name = &s
                        offset += l5
                    }
                }
                {
                    // reading protocols[i0].Metadata: The protocol metadata.
                    if version >= 6 {
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l6 := int(u - 1)
                        protocols[i0].Metadata = common.CopyByteSlice(buff[offset: offset + l6])
                        offset += l6
                    } else {
                        // non flexible and non nullable
                        var l6 int
                        l6 = int(binary.BigEndian.Uint32(buff[offset:]))
                        offset += 4
                        protocols[i0].Metadata = common.CopyByteSlice(buff[offset: offset + l6])
                        offset += l6
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
        m.Protocols = protocols
        }
    }
    if version >= 8 {
        {
            // reading m.Reason: The reason why the member (re-)joins the group.
            // flexible and nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l7 := int(u - 1)
            if l7 > 0 {
                s := string(buff[offset: offset + l7])
                m.Reason = &s
                offset += l7
            } else {
                m.Reason = nil
            }
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

func (m *JoinGroupRequest) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.GroupId: The group identifier.
    if version >= 6 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(*m.GroupId) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.GroupId)))
    }
    if m.GroupId != nil {
        buff = append(buff, *m.GroupId...)
    }
    // writing m.SessionTimeoutMs: The coordinator considers the consumer dead if it receives no heartbeat after this timeout in milliseconds.
    buff = binary.BigEndian.AppendUint32(buff, uint32(m.SessionTimeoutMs))
    if version >= 1 {
        // writing m.RebalanceTimeoutMs: The maximum time in milliseconds that the coordinator will wait for each member to rejoin when rebalancing the group.
        buff = binary.BigEndian.AppendUint32(buff, uint32(m.RebalanceTimeoutMs))
    }
    // writing m.MemberId: The member id assigned by the group coordinator.
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
    if version >= 5 {
        // writing m.GroupInstanceId: The unique identifier of the consumer instance provided by end user.
        if version >= 6 {
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
    // writing m.ProtocolType: The unique name the for class of protocols implemented by the group we want to join.
    if version >= 6 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(*m.ProtocolType) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.ProtocolType)))
    }
    if m.ProtocolType != nil {
        buff = append(buff, *m.ProtocolType...)
    }
    // writing m.Protocols: The list of protocols that the member supports.
    if version >= 6 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.Protocols) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.Protocols)))
    }
    for _, protocols := range m.Protocols {
        // writing non tagged fields
        // writing protocols.Name: The protocol name.
        if version >= 6 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*protocols.Name) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*protocols.Name)))
        }
        if protocols.Name != nil {
            buff = append(buff, *protocols.Name...)
        }
        // writing protocols.Metadata: The protocol metadata.
        if version >= 6 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(protocols.Metadata) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint32(buff, uint32(len(protocols.Metadata)))
        }
        if protocols.Metadata != nil {
            buff = append(buff, protocols.Metadata...)
        }
        if version >= 6 {
            numTaggedFields9 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields9))
        }
    }
    if version >= 8 {
        // writing m.Reason: The reason why the member (re-)joins the group.
        // flexible and nullable
        if m.Reason == nil {
            // null
            buff = append(buff, 0)
        } else {
            // not null
            buff = binary.AppendUvarint(buff, uint64(len(*m.Reason) + 1))
        }
        if m.Reason != nil {
            buff = append(buff, *m.Reason...)
        }
    }
    if version >= 6 {
        numTaggedFields11 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields11))
    }
    return buff
}

func (m *JoinGroupRequest) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.GroupId: The group identifier.
    if version >= 6 {
        // flexible and not nullable
        size += sizeofUvarint(len(*m.GroupId) + 1)
    } else {
        // non flexible and non nullable
        size += 2
    }
    if m.GroupId != nil {
        size += len(*m.GroupId)
    }
    // size for m.SessionTimeoutMs: The coordinator considers the consumer dead if it receives no heartbeat after this timeout in milliseconds.
    size += 4
    if version >= 1 {
        // size for m.RebalanceTimeoutMs: The maximum time in milliseconds that the coordinator will wait for each member to rejoin when rebalancing the group.
        size += 4
    }
    // size for m.MemberId: The member id assigned by the group coordinator.
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
    if version >= 5 {
        // size for m.GroupInstanceId: The unique identifier of the consumer instance provided by end user.
        if version >= 6 {
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
    // size for m.ProtocolType: The unique name the for class of protocols implemented by the group we want to join.
    if version >= 6 {
        // flexible and not nullable
        size += sizeofUvarint(len(*m.ProtocolType) + 1)
    } else {
        // non flexible and non nullable
        size += 2
    }
    if m.ProtocolType != nil {
        size += len(*m.ProtocolType)
    }
    // size for m.Protocols: The list of protocols that the member supports.
    if version >= 6 {
        // flexible and not nullable
        size += sizeofUvarint(len(m.Protocols) + 1)
    } else {
        // non flexible and non nullable
        size += 4
    }
    for _, protocols := range m.Protocols {
        size += 0 * int(unsafe.Sizeof(protocols)) // hack to make sure loop variable is always used
        // calculating size for non tagged fields
        numTaggedFields1:= 0
        numTaggedFields1 += 0
        // size for protocols.Name: The protocol name.
        if version >= 6 {
            // flexible and not nullable
            size += sizeofUvarint(len(*protocols.Name) + 1)
        } else {
            // non flexible and non nullable
            size += 2
        }
        if protocols.Name != nil {
            size += len(*protocols.Name)
        }
        // size for protocols.Metadata: The protocol metadata.
        if version >= 6 {
            // flexible and not nullable
            size += sizeofUvarint(len(protocols.Metadata) + 1)
        } else {
            // non flexible and non nullable
            size += 4
        }
        if protocols.Metadata != nil {
            size += len(protocols.Metadata)
        }
        numTaggedFields2:= 0
        numTaggedFields2 += 0
        if version >= 6 {
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields2)
        }
    }
    if version >= 8 {
        // size for m.Reason: The reason why the member (re-)joins the group.
        // flexible and nullable
        if m.Reason == nil {
            // null
            size += 1
        } else {
            // not null
            size += sizeofUvarint(len(*m.Reason) + 1)
        }
        if m.Reason != nil {
            size += len(*m.Reason)
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

func (m *JoinGroupRequest) HeaderVersions(version int16) (int16, int16) {
    if version >= 6 {
        return 2, 1
    } else {
        return 1, 0
    }
}
