// Package protocol - This is a generated file, please do not edit

package protocol

import "encoding/binary"
import "github.com/spirit-labs/tektite/common"
import "unsafe"

type MetadataResponseMetadataResponseBroker struct {
    // The broker ID.
    NodeId int32
    // The broker hostname.
    Host *string
    // The broker port.
    Port int32
    // The rack of the broker, or null if it has not been assigned to a rack.
    Rack *string
}

type MetadataResponseMetadataResponsePartition struct {
    // The partition error, or 0 if there was no error.
    ErrorCode int16
    // The partition index.
    PartitionIndex int32
    // The ID of the leader broker.
    LeaderId int32
    // The leader epoch of this partition.
    LeaderEpoch int32
    // The set of all nodes that host this partition.
    ReplicaNodes []int32
    // The set of nodes that are in sync with the leader for this partition.
    IsrNodes []int32
    // The set of offline replicas of this partition.
    OfflineReplicas []int32
}

type MetadataResponseMetadataResponseTopic struct {
    // The topic error, or 0 if there was no error.
    ErrorCode int16
    // The topic name. Null for non-existing topics queried by ID. This is never null when ErrorCode is zero. One of Name and TopicId is always populated.
    Name *string
    // The topic id. Zero for non-existing topics queried by name. This is never zero when ErrorCode is zero. One of Name and TopicId is always populated.
    TopicId []byte
    // True if the topic is internal.
    IsInternal bool
    // Each partition in the topic.
    Partitions []MetadataResponseMetadataResponsePartition
    // 32-bit bitfield to represent authorized operations for this topic.
    TopicAuthorizedOperations int32
}

type MetadataResponse struct {
    // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ThrottleTimeMs int32
    // A list of brokers present in the cluster.
    Brokers []MetadataResponseMetadataResponseBroker
    // The cluster ID that responding broker belongs to.
    ClusterId *string
    // The ID of the controller broker.
    ControllerId int32
    // Each topic in the response.
    Topics []MetadataResponseMetadataResponseTopic
    // 32-bit bitfield to represent authorized operations for this cluster.
    ClusterAuthorizedOperations int32
}

func (m *MetadataResponse) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    if version >= 3 {
        {
            // reading m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
            m.ThrottleTimeMs = int32(binary.BigEndian.Uint32(buff[offset:]))
            offset += 4
        }
    }
    {
        // reading m.Brokers: A list of brokers present in the cluster.
        var l0 int
        if version >= 9 {
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
            brokers := make([]MetadataResponseMetadataResponseBroker, l0)
            for i0 := 0; i0 < l0; i0++ {
                // reading non tagged fields
                {
                    // reading brokers[i0].NodeId: The broker ID.
                    brokers[i0].NodeId = int32(binary.BigEndian.Uint32(buff[offset:]))
                    offset += 4
                }
                {
                    // reading brokers[i0].Host: The broker hostname.
                    if version >= 9 {
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l1 := int(u - 1)
                        s := string(buff[offset: offset + l1])
                        brokers[i0].Host = &s
                        offset += l1
                    } else {
                        // non flexible and non nullable
                        var l1 int
                        l1 = int(binary.BigEndian.Uint16(buff[offset:]))
                        offset += 2
                        s := string(buff[offset: offset + l1])
                        brokers[i0].Host = &s
                        offset += l1
                    }
                }
                {
                    // reading brokers[i0].Port: The broker port.
                    brokers[i0].Port = int32(binary.BigEndian.Uint32(buff[offset:]))
                    offset += 4
                }
                if version >= 1 {
                    {
                        // reading brokers[i0].Rack: The rack of the broker, or null if it has not been assigned to a rack.
                        if version >= 9 {
                            // flexible and nullable
                            u, n := binary.Uvarint(buff[offset:])
                            offset += n
                            l2 := int(u - 1)
                            if l2 > 0 {
                                s := string(buff[offset: offset + l2])
                                brokers[i0].Rack = &s
                                offset += l2
                            } else {
                                brokers[i0].Rack = nil
                            }
                        } else {
                            // non flexible and nullable
                            var l2 int
                            l2 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
                            offset += 2
                            if l2 > 0 {
                                s := string(buff[offset: offset + l2])
                                brokers[i0].Rack = &s
                                offset += l2
                            } else {
                                brokers[i0].Rack = nil
                            }
                        }
                    }
                }
                if version >= 9 {
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
        m.Brokers = brokers
        }
    }
    if version >= 2 {
        {
            // reading m.ClusterId: The cluster ID that responding broker belongs to.
            if version >= 9 {
                // flexible and nullable
                u, n := binary.Uvarint(buff[offset:])
                offset += n
                l3 := int(u - 1)
                if l3 > 0 {
                    s := string(buff[offset: offset + l3])
                    m.ClusterId = &s
                    offset += l3
                } else {
                    m.ClusterId = nil
                }
            } else {
                // non flexible and nullable
                var l3 int
                l3 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
                offset += 2
                if l3 > 0 {
                    s := string(buff[offset: offset + l3])
                    m.ClusterId = &s
                    offset += l3
                } else {
                    m.ClusterId = nil
                }
            }
        }
    }
    if version >= 1 {
        {
            // reading m.ControllerId: The ID of the controller broker.
            m.ControllerId = int32(binary.BigEndian.Uint32(buff[offset:]))
            offset += 4
        }
    }
    {
        // reading m.Topics: Each topic in the response.
        var l4 int
        if version >= 9 {
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
            topics := make([]MetadataResponseMetadataResponseTopic, l4)
            for i1 := 0; i1 < l4; i1++ {
                // reading non tagged fields
                {
                    // reading topics[i1].ErrorCode: The topic error, or 0 if there was no error.
                    topics[i1].ErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
                    offset += 2
                }
                {
                    // reading topics[i1].Name: The topic name. Null for non-existing topics queried by ID. This is never null when ErrorCode is zero. One of Name and TopicId is always populated.
                    if version >= 9 {
                        if version >= 12 {
                            // flexible and nullable
                            u, n := binary.Uvarint(buff[offset:])
                            offset += n
                            l5 := int(u - 1)
                            if l5 > 0 {
                                s := string(buff[offset: offset + l5])
                                topics[i1].Name = &s
                                offset += l5
                            } else {
                                topics[i1].Name = nil
                            }
                        } else {
                            // flexible and not nullable
                            u, n := binary.Uvarint(buff[offset:])
                            offset += n
                            l5 := int(u - 1)
                            s := string(buff[offset: offset + l5])
                            topics[i1].Name = &s
                            offset += l5
                        }
                    } else {
                        if version >= 12 {
                            // non flexible and nullable
                            var l5 int
                            l5 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
                            offset += 2
                            if l5 > 0 {
                                s := string(buff[offset: offset + l5])
                                topics[i1].Name = &s
                                offset += l5
                            } else {
                                topics[i1].Name = nil
                            }
                        } else {
                            // non flexible and non nullable
                            var l5 int
                            l5 = int(binary.BigEndian.Uint16(buff[offset:]))
                            offset += 2
                            s := string(buff[offset: offset + l5])
                            topics[i1].Name = &s
                            offset += l5
                        }
                    }
                }
                if version >= 10 {
                    {
                        // reading topics[i1].TopicId: The topic id. Zero for non-existing topics queried by name. This is never zero when ErrorCode is zero. One of Name and TopicId is always populated.
                        topics[i1].TopicId = common.ByteSliceCopy(buff[offset: offset + 16])
                        offset += 16
                    }
                }
                if version >= 1 {
                    {
                        // reading topics[i1].IsInternal: True if the topic is internal.
                        topics[i1].IsInternal = buff[offset] == 1
                        offset++
                    }
                }
                {
                    // reading topics[i1].Partitions: Each partition in the topic.
                    var l6 int
                    if version >= 9 {
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l6 = int(u - 1)
                    } else {
                        // non flexible and non nullable
                        l6 = int(binary.BigEndian.Uint32(buff[offset:]))
                        offset += 4
                    }
                    if l6 >= 0 {
                        // length will be -1 if field is null
                        partitions := make([]MetadataResponseMetadataResponsePartition, l6)
                        for i2 := 0; i2 < l6; i2++ {
                            // reading non tagged fields
                            {
                                // reading partitions[i2].ErrorCode: The partition error, or 0 if there was no error.
                                partitions[i2].ErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
                                offset += 2
                            }
                            {
                                // reading partitions[i2].PartitionIndex: The partition index.
                                partitions[i2].PartitionIndex = int32(binary.BigEndian.Uint32(buff[offset:]))
                                offset += 4
                            }
                            {
                                // reading partitions[i2].LeaderId: The ID of the leader broker.
                                partitions[i2].LeaderId = int32(binary.BigEndian.Uint32(buff[offset:]))
                                offset += 4
                            }
                            if version >= 7 {
                                {
                                    // reading partitions[i2].LeaderEpoch: The leader epoch of this partition.
                                    partitions[i2].LeaderEpoch = int32(binary.BigEndian.Uint32(buff[offset:]))
                                    offset += 4
                                }
                            }
                            {
                                // reading partitions[i2].ReplicaNodes: The set of all nodes that host this partition.
                                var l7 int
                                if version >= 9 {
                                    // flexible and not nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l7 = int(u - 1)
                                } else {
                                    // non flexible and non nullable
                                    l7 = int(binary.BigEndian.Uint32(buff[offset:]))
                                    offset += 4
                                }
                                if l7 >= 0 {
                                    // length will be -1 if field is null
                                    replicaNodes := make([]int32, l7)
                                    for i3 := 0; i3 < l7; i3++ {
                                        replicaNodes[i3] = int32(binary.BigEndian.Uint32(buff[offset:]))
                                        offset += 4
                                    }
                                    partitions[i2].ReplicaNodes = replicaNodes
                                }
                            }
                            {
                                // reading partitions[i2].IsrNodes: The set of nodes that are in sync with the leader for this partition.
                                var l8 int
                                if version >= 9 {
                                    // flexible and not nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l8 = int(u - 1)
                                } else {
                                    // non flexible and non nullable
                                    l8 = int(binary.BigEndian.Uint32(buff[offset:]))
                                    offset += 4
                                }
                                if l8 >= 0 {
                                    // length will be -1 if field is null
                                    isrNodes := make([]int32, l8)
                                    for i4 := 0; i4 < l8; i4++ {
                                        isrNodes[i4] = int32(binary.BigEndian.Uint32(buff[offset:]))
                                        offset += 4
                                    }
                                    partitions[i2].IsrNodes = isrNodes
                                }
                            }
                            if version >= 5 {
                                {
                                    // reading partitions[i2].OfflineReplicas: The set of offline replicas of this partition.
                                    var l9 int
                                    if version >= 9 {
                                        // flexible and not nullable
                                        u, n := binary.Uvarint(buff[offset:])
                                        offset += n
                                        l9 = int(u - 1)
                                    } else {
                                        // non flexible and non nullable
                                        l9 = int(binary.BigEndian.Uint32(buff[offset:]))
                                        offset += 4
                                    }
                                    if l9 >= 0 {
                                        // length will be -1 if field is null
                                        offlineReplicas := make([]int32, l9)
                                        for i5 := 0; i5 < l9; i5++ {
                                            offlineReplicas[i5] = int32(binary.BigEndian.Uint32(buff[offset:]))
                                            offset += 4
                                        }
                                        partitions[i2].OfflineReplicas = offlineReplicas
                                    }
                                }
                            }
                            if version >= 9 {
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
                    topics[i1].Partitions = partitions
                    }
                }
                if version >= 8 {
                    {
                        // reading topics[i1].TopicAuthorizedOperations: 32-bit bitfield to represent authorized operations for this topic.
                        topics[i1].TopicAuthorizedOperations = int32(binary.BigEndian.Uint32(buff[offset:]))
                        offset += 4
                    }
                }
                if version >= 9 {
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
        m.Topics = topics
        }
    }
    if version >= 8 && version <= 10 {
        {
            // reading m.ClusterAuthorizedOperations: 32-bit bitfield to represent authorized operations for this cluster.
            m.ClusterAuthorizedOperations = int32(binary.BigEndian.Uint32(buff[offset:]))
            offset += 4
        }
    }
    if version >= 9 {
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

func (m *MetadataResponse) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    if version >= 3 {
        // writing m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        buff = binary.BigEndian.AppendUint32(buff, uint32(m.ThrottleTimeMs))
    }
    // writing m.Brokers: A list of brokers present in the cluster.
    if version >= 9 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.Brokers) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.Brokers)))
    }
    for _, brokers := range m.Brokers {
        // writing non tagged fields
        // writing brokers.NodeId: The broker ID.
        buff = binary.BigEndian.AppendUint32(buff, uint32(brokers.NodeId))
        // writing brokers.Host: The broker hostname.
        if version >= 9 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*brokers.Host) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*brokers.Host)))
        }
        if brokers.Host != nil {
            buff = append(buff, *brokers.Host...)
        }
        // writing brokers.Port: The broker port.
        buff = binary.BigEndian.AppendUint32(buff, uint32(brokers.Port))
        if version >= 1 {
            // writing brokers.Rack: The rack of the broker, or null if it has not been assigned to a rack.
            if version >= 9 {
                // flexible and nullable
                if brokers.Rack == nil {
                    // null
                    buff = append(buff, 0)
                } else {
                    // not null
                    buff = binary.AppendUvarint(buff, uint64(len(*brokers.Rack) + 1))
                }
            } else {
                // non flexible and nullable
                if brokers.Rack == nil {
                    // null
                    buff = binary.BigEndian.AppendUint16(buff, 65535)
                } else {
                    // not null
                    buff = binary.BigEndian.AppendUint16(buff, uint16(len(*brokers.Rack)))
                }
            }
            if brokers.Rack != nil {
                buff = append(buff, *brokers.Rack...)
            }
        }
        if version >= 9 {
            numTaggedFields6 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields6))
        }
    }
    if version >= 2 {
        // writing m.ClusterId: The cluster ID that responding broker belongs to.
        if version >= 9 {
            // flexible and nullable
            if m.ClusterId == nil {
                // null
                buff = append(buff, 0)
            } else {
                // not null
                buff = binary.AppendUvarint(buff, uint64(len(*m.ClusterId) + 1))
            }
        } else {
            // non flexible and nullable
            if m.ClusterId == nil {
                // null
                buff = binary.BigEndian.AppendUint16(buff, 65535)
            } else {
                // not null
                buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.ClusterId)))
            }
        }
        if m.ClusterId != nil {
            buff = append(buff, *m.ClusterId...)
        }
    }
    if version >= 1 {
        // writing m.ControllerId: The ID of the controller broker.
        buff = binary.BigEndian.AppendUint32(buff, uint32(m.ControllerId))
    }
    // writing m.Topics: Each topic in the response.
    if version >= 9 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.Topics) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.Topics)))
    }
    for _, topics := range m.Topics {
        // writing non tagged fields
        // writing topics.ErrorCode: The topic error, or 0 if there was no error.
        buff = binary.BigEndian.AppendUint16(buff, uint16(topics.ErrorCode))
        // writing topics.Name: The topic name. Null for non-existing topics queried by ID. This is never null when ErrorCode is zero. One of Name and TopicId is always populated.
        if version >= 9 {
            if version >= 12 {
                // flexible and nullable
                if topics.Name == nil {
                    // null
                    buff = append(buff, 0)
                } else {
                    // not null
                    buff = binary.AppendUvarint(buff, uint64(len(*topics.Name) + 1))
                }
            } else {
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(*topics.Name) + 1))
            }
        } else {
            if version >= 12 {
                // non flexible and nullable
                if topics.Name == nil {
                    // null
                    buff = binary.BigEndian.AppendUint16(buff, 65535)
                } else {
                    // not null
                    buff = binary.BigEndian.AppendUint16(buff, uint16(len(*topics.Name)))
                }
            } else {
                // non flexible and non nullable
                buff = binary.BigEndian.AppendUint16(buff, uint16(len(*topics.Name)))
            }
        }
        if topics.Name != nil {
            buff = append(buff, *topics.Name...)
        }
        if version >= 10 {
            // writing topics.TopicId: The topic id. Zero for non-existing topics queried by name. This is never zero when ErrorCode is zero. One of Name and TopicId is always populated.
            if topics.TopicId != nil {
                buff = append(buff, topics.TopicId...)
            }
        }
        if version >= 1 {
            // writing topics.IsInternal: True if the topic is internal.
            if topics.IsInternal {
                buff = append(buff, 1)
            } else {
                buff = append(buff, 0)
            }
        }
        // writing topics.Partitions: Each partition in the topic.
        if version >= 9 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(topics.Partitions) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint32(buff, uint32(len(topics.Partitions)))
        }
        for _, partitions := range topics.Partitions {
            // writing non tagged fields
            // writing partitions.ErrorCode: The partition error, or 0 if there was no error.
            buff = binary.BigEndian.AppendUint16(buff, uint16(partitions.ErrorCode))
            // writing partitions.PartitionIndex: The partition index.
            buff = binary.BigEndian.AppendUint32(buff, uint32(partitions.PartitionIndex))
            // writing partitions.LeaderId: The ID of the leader broker.
            buff = binary.BigEndian.AppendUint32(buff, uint32(partitions.LeaderId))
            if version >= 7 {
                // writing partitions.LeaderEpoch: The leader epoch of this partition.
                buff = binary.BigEndian.AppendUint32(buff, uint32(partitions.LeaderEpoch))
            }
            // writing partitions.ReplicaNodes: The set of all nodes that host this partition.
            if version >= 9 {
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(partitions.ReplicaNodes) + 1))
            } else {
                // non flexible and non nullable
                buff = binary.BigEndian.AppendUint32(buff, uint32(len(partitions.ReplicaNodes)))
            }
            for _, replicaNodes := range partitions.ReplicaNodes {
                buff = binary.BigEndian.AppendUint32(buff, uint32(replicaNodes))
            }
            // writing partitions.IsrNodes: The set of nodes that are in sync with the leader for this partition.
            if version >= 9 {
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(partitions.IsrNodes) + 1))
            } else {
                // non flexible and non nullable
                buff = binary.BigEndian.AppendUint32(buff, uint32(len(partitions.IsrNodes)))
            }
            for _, isrNodes := range partitions.IsrNodes {
                buff = binary.BigEndian.AppendUint32(buff, uint32(isrNodes))
            }
            if version >= 5 {
                // writing partitions.OfflineReplicas: The set of offline replicas of this partition.
                if version >= 9 {
                    // flexible and not nullable
                    buff = binary.AppendUvarint(buff, uint64(len(partitions.OfflineReplicas) + 1))
                } else {
                    // non flexible and non nullable
                    buff = binary.BigEndian.AppendUint32(buff, uint32(len(partitions.OfflineReplicas)))
                }
                for _, offlineReplicas := range partitions.OfflineReplicas {
                    buff = binary.BigEndian.AppendUint32(buff, uint32(offlineReplicas))
                }
            }
            if version >= 9 {
                numTaggedFields22 := 0
                // write number of tagged fields
                buff = binary.AppendUvarint(buff, uint64(numTaggedFields22))
            }
        }
        if version >= 8 {
            // writing topics.TopicAuthorizedOperations: 32-bit bitfield to represent authorized operations for this topic.
            buff = binary.BigEndian.AppendUint32(buff, uint32(topics.TopicAuthorizedOperations))
        }
        if version >= 9 {
            numTaggedFields24 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields24))
        }
    }
    if version >= 8 && version <= 10 {
        // writing m.ClusterAuthorizedOperations: 32-bit bitfield to represent authorized operations for this cluster.
        buff = binary.BigEndian.AppendUint32(buff, uint32(m.ClusterAuthorizedOperations))
    }
    if version >= 9 {
        numTaggedFields26 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields26))
    }
    return buff
}

func (m *MetadataResponse) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    if version >= 3 {
        // size for m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        size += 4
    }
    // size for m.Brokers: A list of brokers present in the cluster.
    if version >= 9 {
        // flexible and not nullable
        size += sizeofUvarint(len(m.Brokers) + 1)
    } else {
        // non flexible and non nullable
        size += 4
    }
    for _, brokers := range m.Brokers {
        size += 0 * int(unsafe.Sizeof(brokers)) // hack to make sure loop variable is always used
        // calculating size for non tagged fields
        numTaggedFields1:= 0
        numTaggedFields1 += 0
        // size for brokers.NodeId: The broker ID.
        size += 4
        // size for brokers.Host: The broker hostname.
        if version >= 9 {
            // flexible and not nullable
            size += sizeofUvarint(len(*brokers.Host) + 1)
        } else {
            // non flexible and non nullable
            size += 2
        }
        if brokers.Host != nil {
            size += len(*brokers.Host)
        }
        // size for brokers.Port: The broker port.
        size += 4
        if version >= 1 {
            // size for brokers.Rack: The rack of the broker, or null if it has not been assigned to a rack.
            if version >= 9 {
                // flexible and nullable
                if brokers.Rack == nil {
                    // null
                    size += 1
                } else {
                    // not null
                    size += sizeofUvarint(len(*brokers.Rack) + 1)
                }
            } else {
                // non flexible and nullable
                size += 2
            }
            if brokers.Rack != nil {
                size += len(*brokers.Rack)
            }
        }
        numTaggedFields2:= 0
        numTaggedFields2 += 0
        if version >= 9 {
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields2)
        }
    }
    if version >= 2 {
        // size for m.ClusterId: The cluster ID that responding broker belongs to.
        if version >= 9 {
            // flexible and nullable
            if m.ClusterId == nil {
                // null
                size += 1
            } else {
                // not null
                size += sizeofUvarint(len(*m.ClusterId) + 1)
            }
        } else {
            // non flexible and nullable
            size += 2
        }
        if m.ClusterId != nil {
            size += len(*m.ClusterId)
        }
    }
    if version >= 1 {
        // size for m.ControllerId: The ID of the controller broker.
        size += 4
    }
    // size for m.Topics: Each topic in the response.
    if version >= 9 {
        // flexible and not nullable
        size += sizeofUvarint(len(m.Topics) + 1)
    } else {
        // non flexible and non nullable
        size += 4
    }
    for _, topics := range m.Topics {
        size += 0 * int(unsafe.Sizeof(topics)) // hack to make sure loop variable is always used
        // calculating size for non tagged fields
        numTaggedFields3:= 0
        numTaggedFields3 += 0
        // size for topics.ErrorCode: The topic error, or 0 if there was no error.
        size += 2
        // size for topics.Name: The topic name. Null for non-existing topics queried by ID. This is never null when ErrorCode is zero. One of Name and TopicId is always populated.
        if version >= 9 {
            if version >= 12 {
                // flexible and nullable
                if topics.Name == nil {
                    // null
                    size += 1
                } else {
                    // not null
                    size += sizeofUvarint(len(*topics.Name) + 1)
                }
            } else {
                // flexible and not nullable
                size += sizeofUvarint(len(*topics.Name) + 1)
            }
        } else {
            if version >= 12 {
                // non flexible and nullable
                size += 2
            } else {
                // non flexible and non nullable
                size += 2
            }
        }
        if topics.Name != nil {
            size += len(*topics.Name)
        }
        if version >= 10 {
            // size for topics.TopicId: The topic id. Zero for non-existing topics queried by name. This is never zero when ErrorCode is zero. One of Name and TopicId is always populated.
            size += 16
        }
        if version >= 1 {
            // size for topics.IsInternal: True if the topic is internal.
            size += 1
        }
        // size for topics.Partitions: Each partition in the topic.
        if version >= 9 {
            // flexible and not nullable
            size += sizeofUvarint(len(topics.Partitions) + 1)
        } else {
            // non flexible and non nullable
            size += 4
        }
        for _, partitions := range topics.Partitions {
            size += 0 * int(unsafe.Sizeof(partitions)) // hack to make sure loop variable is always used
            // calculating size for non tagged fields
            numTaggedFields4:= 0
            numTaggedFields4 += 0
            // size for partitions.ErrorCode: The partition error, or 0 if there was no error.
            size += 2
            // size for partitions.PartitionIndex: The partition index.
            size += 4
            // size for partitions.LeaderId: The ID of the leader broker.
            size += 4
            if version >= 7 {
                // size for partitions.LeaderEpoch: The leader epoch of this partition.
                size += 4
            }
            // size for partitions.ReplicaNodes: The set of all nodes that host this partition.
            if version >= 9 {
                // flexible and not nullable
                size += sizeofUvarint(len(partitions.ReplicaNodes) + 1)
            } else {
                // non flexible and non nullable
                size += 4
            }
            for _, replicaNodes := range partitions.ReplicaNodes {
                size += 0 * int(unsafe.Sizeof(replicaNodes)) // hack to make sure loop variable is always used
                size += 4
            }
            // size for partitions.IsrNodes: The set of nodes that are in sync with the leader for this partition.
            if version >= 9 {
                // flexible and not nullable
                size += sizeofUvarint(len(partitions.IsrNodes) + 1)
            } else {
                // non flexible and non nullable
                size += 4
            }
            for _, isrNodes := range partitions.IsrNodes {
                size += 0 * int(unsafe.Sizeof(isrNodes)) // hack to make sure loop variable is always used
                size += 4
            }
            if version >= 5 {
                // size for partitions.OfflineReplicas: The set of offline replicas of this partition.
                if version >= 9 {
                    // flexible and not nullable
                    size += sizeofUvarint(len(partitions.OfflineReplicas) + 1)
                } else {
                    // non flexible and non nullable
                    size += 4
                }
                for _, offlineReplicas := range partitions.OfflineReplicas {
                    size += 0 * int(unsafe.Sizeof(offlineReplicas)) // hack to make sure loop variable is always used
                    size += 4
                }
            }
            numTaggedFields5:= 0
            numTaggedFields5 += 0
            if version >= 9 {
                // writing size of num tagged fields field
                size += sizeofUvarint(numTaggedFields5)
            }
        }
        if version >= 8 {
            // size for topics.TopicAuthorizedOperations: 32-bit bitfield to represent authorized operations for this topic.
            size += 4
        }
        numTaggedFields6:= 0
        numTaggedFields6 += 0
        if version >= 9 {
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields6)
        }
    }
    if version >= 8 && version <= 10 {
        // size for m.ClusterAuthorizedOperations: 32-bit bitfield to represent authorized operations for this cluster.
        size += 4
    }
    numTaggedFields7:= 0
    numTaggedFields7 += 0
    if version >= 9 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields7)
    }
    return size, tagSizes
}

