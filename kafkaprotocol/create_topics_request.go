// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"
import "unsafe"

type CreateTopicsRequestCreatableReplicaAssignment struct {
    // The partition index.
    PartitionIndex int32
    // The brokers to place the partition on.
    BrokerIds []int32
}

type CreateTopicsRequestCreatableTopicConfig struct {
    // The configuration name.
    Name *string
    // The configuration value.
    Value *string
}

type CreateTopicsRequestCreatableTopic struct {
    // The topic name.
    Name *string
    // The number of partitions to create in the topic, or -1 if we are either specifying a manual partition assignment or using the default partitions.
    NumPartitions int32
    // The number of replicas to create for each partition in the topic, or -1 if we are either specifying a manual partition assignment or using the default replication factor.
    ReplicationFactor int16
    // The manual partition assignment, or the empty array if we are using automatic assignment.
    Assignments []CreateTopicsRequestCreatableReplicaAssignment
    // The custom topic configurations to set.
    Configs []CreateTopicsRequestCreatableTopicConfig
}

type CreateTopicsRequest struct {
    // The topics to create.
    Topics []CreateTopicsRequestCreatableTopic
    // How long to wait in milliseconds before timing out the request.
    timeoutMs int32
    // If true, check that the topics can be created as specified, but don't create anything.
    validateOnly bool
}

func (m *CreateTopicsRequest) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.Topics: The topics to create.
        var l0 int
        if version >= 5 {
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
            topics := make([]CreateTopicsRequestCreatableTopic, l0)
            for i0 := 0; i0 < l0; i0++ {
                // reading non tagged fields
                {
                    // reading topics[i0].Name: The topic name.
                    if version >= 5 {
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l1 := int(u - 1)
                        s := string(buff[offset: offset + l1])
                        topics[i0].Name = &s
                        offset += l1
                    } else {
                        // non flexible and non nullable
                        var l1 int
                        l1 = int(binary.BigEndian.Uint16(buff[offset:]))
                        offset += 2
                        s := string(buff[offset: offset + l1])
                        topics[i0].Name = &s
                        offset += l1
                    }
                }
                {
                    // reading topics[i0].NumPartitions: The number of partitions to create in the topic, or -1 if we are either specifying a manual partition assignment or using the default partitions.
                    topics[i0].NumPartitions = int32(binary.BigEndian.Uint32(buff[offset:]))
                    offset += 4
                }
                {
                    // reading topics[i0].ReplicationFactor: The number of replicas to create for each partition in the topic, or -1 if we are either specifying a manual partition assignment or using the default replication factor.
                    topics[i0].ReplicationFactor = int16(binary.BigEndian.Uint16(buff[offset:]))
                    offset += 2
                }
                {
                    // reading topics[i0].Assignments: The manual partition assignment, or the empty array if we are using automatic assignment.
                    var l2 int
                    if version >= 5 {
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
                        assignments := make([]CreateTopicsRequestCreatableReplicaAssignment, l2)
                        for i1 := 0; i1 < l2; i1++ {
                            // reading non tagged fields
                            {
                                // reading assignments[i1].PartitionIndex: The partition index.
                                assignments[i1].PartitionIndex = int32(binary.BigEndian.Uint32(buff[offset:]))
                                offset += 4
                            }
                            {
                                // reading assignments[i1].BrokerIds: The brokers to place the partition on.
                                var l3 int
                                if version >= 5 {
                                    // flexible and not nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l3 = int(u - 1)
                                } else {
                                    // non flexible and non nullable
                                    l3 = int(binary.BigEndian.Uint32(buff[offset:]))
                                    offset += 4
                                }
                                if l3 >= 0 {
                                    // length will be -1 if field is null
                                    brokerIds := make([]int32, l3)
                                    for i2 := 0; i2 < l3; i2++ {
                                        brokerIds[i2] = int32(binary.BigEndian.Uint32(buff[offset:]))
                                        offset += 4
                                    }
                                    assignments[i1].BrokerIds = brokerIds
                                }
                            }
                            if version >= 5 {
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
                    topics[i0].Assignments = assignments
                    }
                }
                {
                    // reading topics[i0].Configs: The custom topic configurations to set.
                    var l4 int
                    if version >= 5 {
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
                        configs := make([]CreateTopicsRequestCreatableTopicConfig, l4)
                        for i3 := 0; i3 < l4; i3++ {
                            // reading non tagged fields
                            {
                                // reading configs[i3].Name: The configuration name.
                                if version >= 5 {
                                    // flexible and not nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l5 := int(u - 1)
                                    s := string(buff[offset: offset + l5])
                                    configs[i3].Name = &s
                                    offset += l5
                                } else {
                                    // non flexible and non nullable
                                    var l5 int
                                    l5 = int(binary.BigEndian.Uint16(buff[offset:]))
                                    offset += 2
                                    s := string(buff[offset: offset + l5])
                                    configs[i3].Name = &s
                                    offset += l5
                                }
                            }
                            {
                                // reading configs[i3].Value: The configuration value.
                                if version >= 5 {
                                    // flexible and nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l6 := int(u - 1)
                                    if l6 > 0 {
                                        s := string(buff[offset: offset + l6])
                                        configs[i3].Value = &s
                                        offset += l6
                                    } else {
                                        configs[i3].Value = nil
                                    }
                                } else {
                                    // non flexible and nullable
                                    var l6 int
                                    l6 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
                                    offset += 2
                                    if l6 > 0 {
                                        s := string(buff[offset: offset + l6])
                                        configs[i3].Value = &s
                                        offset += l6
                                    } else {
                                        configs[i3].Value = nil
                                    }
                                }
                            }
                            if version >= 5 {
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
                    topics[i0].Configs = configs
                    }
                }
                if version >= 5 {
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
    {
        // reading m.timeoutMs: How long to wait in milliseconds before timing out the request.
        m.timeoutMs = int32(binary.BigEndian.Uint32(buff[offset:]))
        offset += 4
    }
    if version >= 1 {
        {
            // reading m.validateOnly: If true, check that the topics can be created as specified, but don't create anything.
            m.validateOnly = buff[offset] == 1
            offset++
        }
    }
    if version >= 5 {
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

func (m *CreateTopicsRequest) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.Topics: The topics to create.
    if version >= 5 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.Topics) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.Topics)))
    }
    for _, topics := range m.Topics {
        // writing non tagged fields
        // writing topics.Name: The topic name.
        if version >= 5 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*topics.Name) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*topics.Name)))
        }
        if topics.Name != nil {
            buff = append(buff, *topics.Name...)
        }
        // writing topics.NumPartitions: The number of partitions to create in the topic, or -1 if we are either specifying a manual partition assignment or using the default partitions.
        buff = binary.BigEndian.AppendUint32(buff, uint32(topics.NumPartitions))
        // writing topics.ReplicationFactor: The number of replicas to create for each partition in the topic, or -1 if we are either specifying a manual partition assignment or using the default replication factor.
        buff = binary.BigEndian.AppendUint16(buff, uint16(topics.ReplicationFactor))
        // writing topics.Assignments: The manual partition assignment, or the empty array if we are using automatic assignment.
        if version >= 5 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(topics.Assignments) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint32(buff, uint32(len(topics.Assignments)))
        }
        for _, assignments := range topics.Assignments {
            // writing non tagged fields
            // writing assignments.PartitionIndex: The partition index.
            buff = binary.BigEndian.AppendUint32(buff, uint32(assignments.PartitionIndex))
            // writing assignments.BrokerIds: The brokers to place the partition on.
            if version >= 5 {
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(assignments.BrokerIds) + 1))
            } else {
                // non flexible and non nullable
                buff = binary.BigEndian.AppendUint32(buff, uint32(len(assignments.BrokerIds)))
            }
            for _, brokerIds := range assignments.BrokerIds {
                buff = binary.BigEndian.AppendUint32(buff, uint32(brokerIds))
            }
            if version >= 5 {
                numTaggedFields7 := 0
                // write number of tagged fields
                buff = binary.AppendUvarint(buff, uint64(numTaggedFields7))
            }
        }
        // writing topics.Configs: The custom topic configurations to set.
        if version >= 5 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(topics.Configs) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint32(buff, uint32(len(topics.Configs)))
        }
        for _, configs := range topics.Configs {
            // writing non tagged fields
            // writing configs.Name: The configuration name.
            if version >= 5 {
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(*configs.Name) + 1))
            } else {
                // non flexible and non nullable
                buff = binary.BigEndian.AppendUint16(buff, uint16(len(*configs.Name)))
            }
            if configs.Name != nil {
                buff = append(buff, *configs.Name...)
            }
            // writing configs.Value: The configuration value.
            if version >= 5 {
                // flexible and nullable
                if configs.Value == nil {
                    // null
                    buff = append(buff, 0)
                } else {
                    // not null
                    buff = binary.AppendUvarint(buff, uint64(len(*configs.Value) + 1))
                }
            } else {
                // non flexible and nullable
                if configs.Value == nil {
                    // null
                    buff = binary.BigEndian.AppendUint16(buff, 65535)
                } else {
                    // not null
                    buff = binary.BigEndian.AppendUint16(buff, uint16(len(*configs.Value)))
                }
            }
            if configs.Value != nil {
                buff = append(buff, *configs.Value...)
            }
            if version >= 5 {
                numTaggedFields11 := 0
                // write number of tagged fields
                buff = binary.AppendUvarint(buff, uint64(numTaggedFields11))
            }
        }
        if version >= 5 {
            numTaggedFields12 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields12))
        }
    }
    // writing m.timeoutMs: How long to wait in milliseconds before timing out the request.
    buff = binary.BigEndian.AppendUint32(buff, uint32(m.timeoutMs))
    if version >= 1 {
        // writing m.validateOnly: If true, check that the topics can be created as specified, but don't create anything.
        if m.validateOnly {
            buff = append(buff, 1)
        } else {
            buff = append(buff, 0)
        }
    }
    if version >= 5 {
        numTaggedFields15 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields15))
    }
    return buff
}

func (m *CreateTopicsRequest) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.Topics: The topics to create.
    if version >= 5 {
        // flexible and not nullable
        size += sizeofUvarint(len(m.Topics) + 1)
    } else {
        // non flexible and non nullable
        size += 4
    }
    for _, topics := range m.Topics {
        size += 0 * int(unsafe.Sizeof(topics)) // hack to make sure loop variable is always used
        // calculating size for non tagged fields
        numTaggedFields1:= 0
        numTaggedFields1 += 0
        // size for topics.Name: The topic name.
        if version >= 5 {
            // flexible and not nullable
            size += sizeofUvarint(len(*topics.Name) + 1)
        } else {
            // non flexible and non nullable
            size += 2
        }
        if topics.Name != nil {
            size += len(*topics.Name)
        }
        // size for topics.NumPartitions: The number of partitions to create in the topic, or -1 if we are either specifying a manual partition assignment or using the default partitions.
        size += 4
        // size for topics.ReplicationFactor: The number of replicas to create for each partition in the topic, or -1 if we are either specifying a manual partition assignment or using the default replication factor.
        size += 2
        // size for topics.Assignments: The manual partition assignment, or the empty array if we are using automatic assignment.
        if version >= 5 {
            // flexible and not nullable
            size += sizeofUvarint(len(topics.Assignments) + 1)
        } else {
            // non flexible and non nullable
            size += 4
        }
        for _, assignments := range topics.Assignments {
            size += 0 * int(unsafe.Sizeof(assignments)) // hack to make sure loop variable is always used
            // calculating size for non tagged fields
            numTaggedFields2:= 0
            numTaggedFields2 += 0
            // size for assignments.PartitionIndex: The partition index.
            size += 4
            // size for assignments.BrokerIds: The brokers to place the partition on.
            if version >= 5 {
                // flexible and not nullable
                size += sizeofUvarint(len(assignments.BrokerIds) + 1)
            } else {
                // non flexible and non nullable
                size += 4
            }
            for _, brokerIds := range assignments.BrokerIds {
                size += 0 * int(unsafe.Sizeof(brokerIds)) // hack to make sure loop variable is always used
                size += 4
            }
            numTaggedFields3:= 0
            numTaggedFields3 += 0
            if version >= 5 {
                // writing size of num tagged fields field
                size += sizeofUvarint(numTaggedFields3)
            }
        }
        // size for topics.Configs: The custom topic configurations to set.
        if version >= 5 {
            // flexible and not nullable
            size += sizeofUvarint(len(topics.Configs) + 1)
        } else {
            // non flexible and non nullable
            size += 4
        }
        for _, configs := range topics.Configs {
            size += 0 * int(unsafe.Sizeof(configs)) // hack to make sure loop variable is always used
            // calculating size for non tagged fields
            numTaggedFields4:= 0
            numTaggedFields4 += 0
            // size for configs.Name: The configuration name.
            if version >= 5 {
                // flexible and not nullable
                size += sizeofUvarint(len(*configs.Name) + 1)
            } else {
                // non flexible and non nullable
                size += 2
            }
            if configs.Name != nil {
                size += len(*configs.Name)
            }
            // size for configs.Value: The configuration value.
            if version >= 5 {
                // flexible and nullable
                if configs.Value == nil {
                    // null
                    size += 1
                } else {
                    // not null
                    size += sizeofUvarint(len(*configs.Value) + 1)
                }
            } else {
                // non flexible and nullable
                size += 2
            }
            if configs.Value != nil {
                size += len(*configs.Value)
            }
            numTaggedFields5:= 0
            numTaggedFields5 += 0
            if version >= 5 {
                // writing size of num tagged fields field
                size += sizeofUvarint(numTaggedFields5)
            }
        }
        numTaggedFields6:= 0
        numTaggedFields6 += 0
        if version >= 5 {
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields6)
        }
    }
    // size for m.timeoutMs: How long to wait in milliseconds before timing out the request.
    size += 4
    if version >= 1 {
        // size for m.validateOnly: If true, check that the topics can be created as specified, but don't create anything.
        size += 1
    }
    numTaggedFields7:= 0
    numTaggedFields7 += 0
    if version >= 5 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields7)
    }
    return size, tagSizes
}

func (m *CreateTopicsRequest) HeaderVersions(version int16) (int16, int16) {
    if version >= 5 {
        return 2, 1
    } else {
        return 1, 0
    }
}

func (m *CreateTopicsRequest) SupportedApiVersions() (int16, int16) {
    return -1, -1
}
