// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"
import "unsafe"

type TxnOffsetCommitRequestTxnOffsetCommitRequestPartition struct {
    // The index of the partition within the topic.
    PartitionIndex int32
    // The message offset to be committed.
    CommittedOffset int64
    // The leader epoch of the last consumed record.
    CommittedLeaderEpoch int32
    // Any associated metadata the client wants to keep.
    CommittedMetadata *string
}

type TxnOffsetCommitRequestTxnOffsetCommitRequestTopic struct {
    // The topic name.
    Name *string
    // The partitions inside the topic that we want to commit offsets for.
    Partitions []TxnOffsetCommitRequestTxnOffsetCommitRequestPartition
}

type TxnOffsetCommitRequest struct {
    // The ID of the transaction.
    TransactionalId *string
    // The ID of the group.
    GroupId *string
    // The current producer ID in use by the transactional ID.
    ProducerId int64
    // The current epoch associated with the producer ID.
    ProducerEpoch int16
    // The generation of the consumer.
    GenerationId int32
    // The member ID assigned by the group coordinator.
    MemberId *string
    // The unique identifier of the consumer instance provided by end user.
    GroupInstanceId *string
    // Each topic that we want to commit offsets for.
    Topics []TxnOffsetCommitRequestTxnOffsetCommitRequestTopic
}

func (m *TxnOffsetCommitRequest) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.TransactionalId: The ID of the transaction.
        if version >= 3 {
            // flexible and not nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l0 := int(u - 1)
            s := string(buff[offset: offset + l0])
            m.TransactionalId = &s
            offset += l0
        } else {
            // non flexible and non nullable
            var l0 int
            l0 = int(binary.BigEndian.Uint16(buff[offset:]))
            offset += 2
            s := string(buff[offset: offset + l0])
            m.TransactionalId = &s
            offset += l0
        }
    }
    {
        // reading m.GroupId: The ID of the group.
        if version >= 3 {
            // flexible and not nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l1 := int(u - 1)
            s := string(buff[offset: offset + l1])
            m.GroupId = &s
            offset += l1
        } else {
            // non flexible and non nullable
            var l1 int
            l1 = int(binary.BigEndian.Uint16(buff[offset:]))
            offset += 2
            s := string(buff[offset: offset + l1])
            m.GroupId = &s
            offset += l1
        }
    }
    {
        // reading m.ProducerId: The current producer ID in use by the transactional ID.
        m.ProducerId = int64(binary.BigEndian.Uint64(buff[offset:]))
        offset += 8
    }
    {
        // reading m.ProducerEpoch: The current epoch associated with the producer ID.
        m.ProducerEpoch = int16(binary.BigEndian.Uint16(buff[offset:]))
        offset += 2
    }
    if version >= 3 {
        {
            // reading m.GenerationId: The generation of the consumer.
            m.GenerationId = int32(binary.BigEndian.Uint32(buff[offset:]))
            offset += 4
        }
        {
            // reading m.MemberId: The member ID assigned by the group coordinator.
            // flexible and not nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l2 := int(u - 1)
            s := string(buff[offset: offset + l2])
            m.MemberId = &s
            offset += l2
        }
        {
            // reading m.GroupInstanceId: The unique identifier of the consumer instance provided by end user.
            // flexible and nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l3 := int(u - 1)
            if l3 > 0 {
                s := string(buff[offset: offset + l3])
                m.GroupInstanceId = &s
                offset += l3
            } else {
                m.GroupInstanceId = nil
            }
        }
    }
    {
        // reading m.Topics: Each topic that we want to commit offsets for.
        var l4 int
        if version >= 3 {
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
            topics := make([]TxnOffsetCommitRequestTxnOffsetCommitRequestTopic, l4)
            for i0 := 0; i0 < l4; i0++ {
                // reading non tagged fields
                {
                    // reading topics[i0].Name: The topic name.
                    if version >= 3 {
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l5 := int(u - 1)
                        s := string(buff[offset: offset + l5])
                        topics[i0].Name = &s
                        offset += l5
                    } else {
                        // non flexible and non nullable
                        var l5 int
                        l5 = int(binary.BigEndian.Uint16(buff[offset:]))
                        offset += 2
                        s := string(buff[offset: offset + l5])
                        topics[i0].Name = &s
                        offset += l5
                    }
                }
                {
                    // reading topics[i0].Partitions: The partitions inside the topic that we want to commit offsets for.
                    var l6 int
                    if version >= 3 {
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
                        partitions := make([]TxnOffsetCommitRequestTxnOffsetCommitRequestPartition, l6)
                        for i1 := 0; i1 < l6; i1++ {
                            // reading non tagged fields
                            {
                                // reading partitions[i1].PartitionIndex: The index of the partition within the topic.
                                partitions[i1].PartitionIndex = int32(binary.BigEndian.Uint32(buff[offset:]))
                                offset += 4
                            }
                            {
                                // reading partitions[i1].CommittedOffset: The message offset to be committed.
                                partitions[i1].CommittedOffset = int64(binary.BigEndian.Uint64(buff[offset:]))
                                offset += 8
                            }
                            if version >= 2 {
                                {
                                    // reading partitions[i1].CommittedLeaderEpoch: The leader epoch of the last consumed record.
                                    partitions[i1].CommittedLeaderEpoch = int32(binary.BigEndian.Uint32(buff[offset:]))
                                    offset += 4
                                }
                            }
                            {
                                // reading partitions[i1].CommittedMetadata: Any associated metadata the client wants to keep.
                                if version >= 3 {
                                    // flexible and nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l7 := int(u - 1)
                                    if l7 > 0 {
                                        s := string(buff[offset: offset + l7])
                                        partitions[i1].CommittedMetadata = &s
                                        offset += l7
                                    } else {
                                        partitions[i1].CommittedMetadata = nil
                                    }
                                } else {
                                    // non flexible and nullable
                                    var l7 int
                                    l7 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
                                    offset += 2
                                    if l7 > 0 {
                                        s := string(buff[offset: offset + l7])
                                        partitions[i1].CommittedMetadata = &s
                                        offset += l7
                                    } else {
                                        partitions[i1].CommittedMetadata = nil
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
                        }
                    topics[i0].Partitions = partitions
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
        m.Topics = topics
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

func (m *TxnOffsetCommitRequest) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.TransactionalId: The ID of the transaction.
    if version >= 3 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(*m.TransactionalId) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.TransactionalId)))
    }
    if m.TransactionalId != nil {
        buff = append(buff, *m.TransactionalId...)
    }
    // writing m.GroupId: The ID of the group.
    if version >= 3 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(*m.GroupId) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.GroupId)))
    }
    if m.GroupId != nil {
        buff = append(buff, *m.GroupId...)
    }
    // writing m.ProducerId: The current producer ID in use by the transactional ID.
    buff = binary.BigEndian.AppendUint64(buff, uint64(m.ProducerId))
    // writing m.ProducerEpoch: The current epoch associated with the producer ID.
    buff = binary.BigEndian.AppendUint16(buff, uint16(m.ProducerEpoch))
    if version >= 3 {
        // writing m.GenerationId: The generation of the consumer.
        buff = binary.BigEndian.AppendUint32(buff, uint32(m.GenerationId))
        // writing m.MemberId: The member ID assigned by the group coordinator.
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(*m.MemberId) + 1))
        if m.MemberId != nil {
            buff = append(buff, *m.MemberId...)
        }
        // writing m.GroupInstanceId: The unique identifier of the consumer instance provided by end user.
        // flexible and nullable
        if m.GroupInstanceId == nil {
            // null
            buff = append(buff, 0)
        } else {
            // not null
            buff = binary.AppendUvarint(buff, uint64(len(*m.GroupInstanceId) + 1))
        }
        if m.GroupInstanceId != nil {
            buff = append(buff, *m.GroupInstanceId...)
        }
    }
    // writing m.Topics: Each topic that we want to commit offsets for.
    if version >= 3 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.Topics) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.Topics)))
    }
    for _, topics := range m.Topics {
        // writing non tagged fields
        // writing topics.Name: The topic name.
        if version >= 3 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*topics.Name) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*topics.Name)))
        }
        if topics.Name != nil {
            buff = append(buff, *topics.Name...)
        }
        // writing topics.Partitions: The partitions inside the topic that we want to commit offsets for.
        if version >= 3 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(topics.Partitions) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint32(buff, uint32(len(topics.Partitions)))
        }
        for _, partitions := range topics.Partitions {
            // writing non tagged fields
            // writing partitions.PartitionIndex: The index of the partition within the topic.
            buff = binary.BigEndian.AppendUint32(buff, uint32(partitions.PartitionIndex))
            // writing partitions.CommittedOffset: The message offset to be committed.
            buff = binary.BigEndian.AppendUint64(buff, uint64(partitions.CommittedOffset))
            if version >= 2 {
                // writing partitions.CommittedLeaderEpoch: The leader epoch of the last consumed record.
                buff = binary.BigEndian.AppendUint32(buff, uint32(partitions.CommittedLeaderEpoch))
            }
            // writing partitions.CommittedMetadata: Any associated metadata the client wants to keep.
            if version >= 3 {
                // flexible and nullable
                if partitions.CommittedMetadata == nil {
                    // null
                    buff = append(buff, 0)
                } else {
                    // not null
                    buff = binary.AppendUvarint(buff, uint64(len(*partitions.CommittedMetadata) + 1))
                }
            } else {
                // non flexible and nullable
                if partitions.CommittedMetadata == nil {
                    // null
                    buff = binary.BigEndian.AppendUint16(buff, 65535)
                } else {
                    // not null
                    buff = binary.BigEndian.AppendUint16(buff, uint16(len(*partitions.CommittedMetadata)))
                }
            }
            if partitions.CommittedMetadata != nil {
                buff = append(buff, *partitions.CommittedMetadata...)
            }
            if version >= 3 {
                numTaggedFields14 := 0
                // write number of tagged fields
                buff = binary.AppendUvarint(buff, uint64(numTaggedFields14))
            }
        }
        if version >= 3 {
            numTaggedFields15 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields15))
        }
    }
    if version >= 3 {
        numTaggedFields16 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields16))
    }
    return buff
}

func (m *TxnOffsetCommitRequest) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.TransactionalId: The ID of the transaction.
    if version >= 3 {
        // flexible and not nullable
        size += sizeofUvarint(len(*m.TransactionalId) + 1)
    } else {
        // non flexible and non nullable
        size += 2
    }
    if m.TransactionalId != nil {
        size += len(*m.TransactionalId)
    }
    // size for m.GroupId: The ID of the group.
    if version >= 3 {
        // flexible and not nullable
        size += sizeofUvarint(len(*m.GroupId) + 1)
    } else {
        // non flexible and non nullable
        size += 2
    }
    if m.GroupId != nil {
        size += len(*m.GroupId)
    }
    // size for m.ProducerId: The current producer ID in use by the transactional ID.
    size += 8
    // size for m.ProducerEpoch: The current epoch associated with the producer ID.
    size += 2
    if version >= 3 {
        // size for m.GenerationId: The generation of the consumer.
        size += 4
        // size for m.MemberId: The member ID assigned by the group coordinator.
        // flexible and not nullable
        size += sizeofUvarint(len(*m.MemberId) + 1)
        if m.MemberId != nil {
            size += len(*m.MemberId)
        }
        // size for m.GroupInstanceId: The unique identifier of the consumer instance provided by end user.
        // flexible and nullable
        if m.GroupInstanceId == nil {
            // null
            size += 1
        } else {
            // not null
            size += sizeofUvarint(len(*m.GroupInstanceId) + 1)
        }
        if m.GroupInstanceId != nil {
            size += len(*m.GroupInstanceId)
        }
    }
    // size for m.Topics: Each topic that we want to commit offsets for.
    if version >= 3 {
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
        if version >= 3 {
            // flexible and not nullable
            size += sizeofUvarint(len(*topics.Name) + 1)
        } else {
            // non flexible and non nullable
            size += 2
        }
        if topics.Name != nil {
            size += len(*topics.Name)
        }
        // size for topics.Partitions: The partitions inside the topic that we want to commit offsets for.
        if version >= 3 {
            // flexible and not nullable
            size += sizeofUvarint(len(topics.Partitions) + 1)
        } else {
            // non flexible and non nullable
            size += 4
        }
        for _, partitions := range topics.Partitions {
            size += 0 * int(unsafe.Sizeof(partitions)) // hack to make sure loop variable is always used
            // calculating size for non tagged fields
            numTaggedFields2:= 0
            numTaggedFields2 += 0
            // size for partitions.PartitionIndex: The index of the partition within the topic.
            size += 4
            // size for partitions.CommittedOffset: The message offset to be committed.
            size += 8
            if version >= 2 {
                // size for partitions.CommittedLeaderEpoch: The leader epoch of the last consumed record.
                size += 4
            }
            // size for partitions.CommittedMetadata: Any associated metadata the client wants to keep.
            if version >= 3 {
                // flexible and nullable
                if partitions.CommittedMetadata == nil {
                    // null
                    size += 1
                } else {
                    // not null
                    size += sizeofUvarint(len(*partitions.CommittedMetadata) + 1)
                }
            } else {
                // non flexible and nullable
                size += 2
            }
            if partitions.CommittedMetadata != nil {
                size += len(*partitions.CommittedMetadata)
            }
            numTaggedFields3:= 0
            numTaggedFields3 += 0
            if version >= 3 {
                // writing size of num tagged fields field
                size += sizeofUvarint(numTaggedFields3)
            }
        }
        numTaggedFields4:= 0
        numTaggedFields4 += 0
        if version >= 3 {
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields4)
        }
    }
    numTaggedFields5:= 0
    numTaggedFields5 += 0
    if version >= 3 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields5)
    }
    return size, tagSizes
}

func (m *TxnOffsetCommitRequest) HeaderVersions(version int16) (int16, int16) {
    if version >= 3 {
        return 2, 1
    } else {
        return 1, 0
    }
}

func (m *TxnOffsetCommitRequest) SupportedApiVersions() (int16, int16) {
    return -1, -1
}
