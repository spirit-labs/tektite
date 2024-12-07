// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"
import "unsafe"

type DescribeClusterResponseDescribeClusterBroker struct {
    // The broker ID.
    BrokerId int32
    // The broker hostname.
    Host *string
    // The broker port.
    Port int32
    // The rack of the broker, or null if it has not been assigned to a rack.
    Rack *string
}

type DescribeClusterResponse struct {
    // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ThrottleTimeMs int32
    // The top-level error code, or 0 if there was no error
    ErrorCode int16
    // The top-level error message, or null if there was no error.
    ErrorMessage *string
    // The endpoint type that was described. 1=brokers, 2=controllers.
    EndpointType int8
    // The cluster ID that responding broker belongs to.
    ClusterId *string
    // The ID of the controller broker.
    ControllerId int32
    // Each broker in the response.
    Brokers []DescribeClusterResponseDescribeClusterBroker
    // 32-bit bitfield to represent authorized operations for this cluster.
    ClusterAuthorizedOperations int32
}

func (m *DescribeClusterResponse) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        m.ThrottleTimeMs = int32(binary.BigEndian.Uint32(buff[offset:]))
        offset += 4
    }
    {
        // reading m.ErrorCode: The top-level error code, or 0 if there was no error
        m.ErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
        offset += 2
    }
    {
        // reading m.ErrorMessage: The top-level error message, or null if there was no error.
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
    }
    if version >= 1 {
        {
            // reading m.EndpointType: The endpoint type that was described. 1=brokers, 2=controllers.
            m.EndpointType = int8(buff[offset])
            offset++
        }
    }
    {
        // reading m.ClusterId: The cluster ID that responding broker belongs to.
        // flexible and not nullable
        u, n := binary.Uvarint(buff[offset:])
        offset += n
        l1 := int(u - 1)
        s := string(buff[offset: offset + l1])
        m.ClusterId = &s
        offset += l1
    }
    {
        // reading m.ControllerId: The ID of the controller broker.
        m.ControllerId = int32(binary.BigEndian.Uint32(buff[offset:]))
        offset += 4
    }
    {
        // reading m.Brokers: Each broker in the response.
        var l2 int
        // flexible and not nullable
        u, n := binary.Uvarint(buff[offset:])
        offset += n
        l2 = int(u - 1)
        if l2 >= 0 {
            // length will be -1 if field is null
            brokers := make([]DescribeClusterResponseDescribeClusterBroker, l2)
            for i0 := 0; i0 < l2; i0++ {
                // reading non tagged fields
                {
                    // reading brokers[i0].BrokerId: The broker ID.
                    brokers[i0].BrokerId = int32(binary.BigEndian.Uint32(buff[offset:]))
                    offset += 4
                }
                {
                    // reading brokers[i0].Host: The broker hostname.
                    // flexible and not nullable
                    u, n := binary.Uvarint(buff[offset:])
                    offset += n
                    l3 := int(u - 1)
                    s := string(buff[offset: offset + l3])
                    brokers[i0].Host = &s
                    offset += l3
                }
                {
                    // reading brokers[i0].Port: The broker port.
                    brokers[i0].Port = int32(binary.BigEndian.Uint32(buff[offset:]))
                    offset += 4
                }
                {
                    // reading brokers[i0].Rack: The rack of the broker, or null if it has not been assigned to a rack.
                    // flexible and nullable
                    u, n := binary.Uvarint(buff[offset:])
                    offset += n
                    l4 := int(u - 1)
                    if l4 > 0 {
                        s := string(buff[offset: offset + l4])
                        brokers[i0].Rack = &s
                        offset += l4
                    } else {
                        brokers[i0].Rack = nil
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
        m.Brokers = brokers
        }
    }
    {
        // reading m.ClusterAuthorizedOperations: 32-bit bitfield to represent authorized operations for this cluster.
        m.ClusterAuthorizedOperations = int32(binary.BigEndian.Uint32(buff[offset:]))
        offset += 4
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
    return offset, nil
}

func (m *DescribeClusterResponse) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    buff = binary.BigEndian.AppendUint32(buff, uint32(m.ThrottleTimeMs))
    // writing m.ErrorCode: The top-level error code, or 0 if there was no error
    buff = binary.BigEndian.AppendUint16(buff, uint16(m.ErrorCode))
    // writing m.ErrorMessage: The top-level error message, or null if there was no error.
    // flexible and nullable
    if m.ErrorMessage == nil {
        // null
        buff = append(buff, 0)
    } else {
        // not null
        buff = binary.AppendUvarint(buff, uint64(len(*m.ErrorMessage) + 1))
    }
    if m.ErrorMessage != nil {
        buff = append(buff, *m.ErrorMessage...)
    }
    if version >= 1 {
        // writing m.EndpointType: The endpoint type that was described. 1=brokers, 2=controllers.
        buff = append(buff, byte(m.EndpointType))
    }
    // writing m.ClusterId: The cluster ID that responding broker belongs to.
    // flexible and not nullable
    buff = binary.AppendUvarint(buff, uint64(len(*m.ClusterId) + 1))
    if m.ClusterId != nil {
        buff = append(buff, *m.ClusterId...)
    }
    // writing m.ControllerId: The ID of the controller broker.
    buff = binary.BigEndian.AppendUint32(buff, uint32(m.ControllerId))
    // writing m.Brokers: Each broker in the response.
    // flexible and not nullable
    buff = binary.AppendUvarint(buff, uint64(len(m.Brokers) + 1))
    for _, brokers := range m.Brokers {
        // writing non tagged fields
        // writing brokers.BrokerId: The broker ID.
        buff = binary.BigEndian.AppendUint32(buff, uint32(brokers.BrokerId))
        // writing brokers.Host: The broker hostname.
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(*brokers.Host) + 1))
        if brokers.Host != nil {
            buff = append(buff, *brokers.Host...)
        }
        // writing brokers.Port: The broker port.
        buff = binary.BigEndian.AppendUint32(buff, uint32(brokers.Port))
        // writing brokers.Rack: The rack of the broker, or null if it has not been assigned to a rack.
        // flexible and nullable
        if brokers.Rack == nil {
            // null
            buff = append(buff, 0)
        } else {
            // not null
            buff = binary.AppendUvarint(buff, uint64(len(*brokers.Rack) + 1))
        }
        if brokers.Rack != nil {
            buff = append(buff, *brokers.Rack...)
        }
        numTaggedFields11 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields11))
    }
    // writing m.ClusterAuthorizedOperations: 32-bit bitfield to represent authorized operations for this cluster.
    buff = binary.BigEndian.AppendUint32(buff, uint32(m.ClusterAuthorizedOperations))
    numTaggedFields13 := 0
    // write number of tagged fields
    buff = binary.AppendUvarint(buff, uint64(numTaggedFields13))
    return buff
}

func (m *DescribeClusterResponse) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    size += 4
    // size for m.ErrorCode: The top-level error code, or 0 if there was no error
    size += 2
    // size for m.ErrorMessage: The top-level error message, or null if there was no error.
    // flexible and nullable
    if m.ErrorMessage == nil {
        // null
        size += 1
    } else {
        // not null
        size += sizeofUvarint(len(*m.ErrorMessage) + 1)
    }
    if m.ErrorMessage != nil {
        size += len(*m.ErrorMessage)
    }
    if version >= 1 {
        // size for m.EndpointType: The endpoint type that was described. 1=brokers, 2=controllers.
        size += 1
    }
    // size for m.ClusterId: The cluster ID that responding broker belongs to.
    // flexible and not nullable
    size += sizeofUvarint(len(*m.ClusterId) + 1)
    if m.ClusterId != nil {
        size += len(*m.ClusterId)
    }
    // size for m.ControllerId: The ID of the controller broker.
    size += 4
    // size for m.Brokers: Each broker in the response.
    // flexible and not nullable
    size += sizeofUvarint(len(m.Brokers) + 1)
    for _, brokers := range m.Brokers {
        size += 0 * int(unsafe.Sizeof(brokers)) // hack to make sure loop variable is always used
        // calculating size for non tagged fields
        numTaggedFields1:= 0
        numTaggedFields1 += 0
        // size for brokers.BrokerId: The broker ID.
        size += 4
        // size for brokers.Host: The broker hostname.
        // flexible and not nullable
        size += sizeofUvarint(len(*brokers.Host) + 1)
        if brokers.Host != nil {
            size += len(*brokers.Host)
        }
        // size for brokers.Port: The broker port.
        size += 4
        // size for brokers.Rack: The rack of the broker, or null if it has not been assigned to a rack.
        // flexible and nullable
        if brokers.Rack == nil {
            // null
            size += 1
        } else {
            // not null
            size += sizeofUvarint(len(*brokers.Rack) + 1)
        }
        if brokers.Rack != nil {
            size += len(*brokers.Rack)
        }
        numTaggedFields2:= 0
        numTaggedFields2 += 0
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields2)
    }
    // size for m.ClusterAuthorizedOperations: 32-bit bitfield to represent authorized operations for this cluster.
    size += 4
    numTaggedFields3:= 0
    numTaggedFields3 += 0
    // writing size of num tagged fields field
    size += sizeofUvarint(numTaggedFields3)
    return size, tagSizes
}


