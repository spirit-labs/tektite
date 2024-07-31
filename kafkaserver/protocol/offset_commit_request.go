// Package protocol - This is a generated file, please do not edit

package protocol

import "encoding/binary"
import "unsafe"

type OffsetCommitRequestOffsetCommitRequestPartition struct {
    // The partition index.
    PartitionIndex int32
    // The message offset to be committed.
    CommittedOffset int64
    // The leader epoch of this partition.
    CommittedLeaderEpoch int32
    // The timestamp of the commit.
    CommitTimestamp int64
    // Any associated metadata the client wants to keep.
    CommittedMetadata *string
}

type OffsetCommitRequestOffsetCommitRequestTopic struct {
    // The topic name.
    Name *string
    // Each partition to commit offsets for.
    Partitions []OffsetCommitRequestOffsetCommitRequestPartition
}

type OffsetCommitRequest struct {
    // The unique group identifier.
    GroupId *string
    // The generation of the group if using the classic group protocol or the member epoch if using the consumer protocol.
    GenerationIdOrMemberEpoch int32
    // The member ID assigned by the group coordinator.
    MemberId *string
    // The unique identifier of the consumer instance provided by end user.
    GroupInstanceId *string
    // The time period in ms to retain the offset.
    RetentionTimeMs int64
    // The topics to commit offsets for.
    Topics []OffsetCommitRequestOffsetCommitRequestTopic
}

func (m *OffsetCommitRequest) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.GroupId: The unique group identifier.
        if version >= 8 {
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
    if version >= 1 {
        {
            // reading m.GenerationIdOrMemberEpoch: The generation of the group if using the classic group protocol or the member epoch if using the consumer protocol.
            m.GenerationIdOrMemberEpoch = int32(binary.BigEndian.Uint32(buff[offset:]))
            offset += 4
        }
        {
            // reading m.MemberId: The member ID assigned by the group coordinator.
            if version >= 8 {
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
    }
    if version >= 7 {
        {
            // reading m.GroupInstanceId: The unique identifier of the consumer instance provided by end user.
            if version >= 8 {
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
    if version >= 2 && version <= 4 {
        {
            // reading m.RetentionTimeMs: The time period in ms to retain the offset.
            m.RetentionTimeMs = int64(binary.BigEndian.Uint64(buff[offset:]))
            offset += 8
        }
    }
    {
        // reading m.Topics: The topics to commit offsets for.
        var l3 int
        if version >= 8 {
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
            topics := make([]OffsetCommitRequestOffsetCommitRequestTopic, l3)
            for i0 := 0; i0 < l3; i0++ {
                // reading non tagged fields
                {
                    // reading topics[i0].Name: The topic name.
                    if version >= 8 {
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l4 := int(u - 1)
                        s := string(buff[offset: offset + l4])
                        topics[i0].Name = &s
                        offset += l4
                    } else {
                        // non flexible and non nullable
                        var l4 int
                        l4 = int(binary.BigEndian.Uint16(buff[offset:]))
                        offset += 2
                        s := string(buff[offset: offset + l4])
                        topics[i0].Name = &s
                        offset += l4
                    }
                }
                {
                    // reading topics[i0].Partitions: Each partition to commit offsets for.
                    var l5 int
                    if version >= 8 {
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
                        partitions := make([]OffsetCommitRequestOffsetCommitRequestPartition, l5)
                        for i1 := 0; i1 < l5; i1++ {
                            // reading non tagged fields
                            {
                                // reading partitions[i1].PartitionIndex: The partition index.
                                partitions[i1].PartitionIndex = int32(binary.BigEndian.Uint32(buff[offset:]))
                                offset += 4
                            }
                            {
                                // reading partitions[i1].CommittedOffset: The message offset to be committed.
                                partitions[i1].CommittedOffset = int64(binary.BigEndian.Uint64(buff[offset:]))
                                offset += 8
                            }
                            if version >= 6 {
                                {
                                    // reading partitions[i1].CommittedLeaderEpoch: The leader epoch of this partition.
                                    partitions[i1].CommittedLeaderEpoch = int32(binary.BigEndian.Uint32(buff[offset:]))
                                    offset += 4
                                }
                            }
                            if version >= 1 && version <= 1 {
                                {
                                    // reading partitions[i1].CommitTimestamp: The timestamp of the commit.
                                    partitions[i1].CommitTimestamp = int64(binary.BigEndian.Uint64(buff[offset:]))
                                    offset += 8
                                }
                            }
                            {
                                // reading partitions[i1].CommittedMetadata: Any associated metadata the client wants to keep.
                                if version >= 8 {
                                    // flexible and nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l6 := int(u - 1)
                                    if l6 > 0 {
                                        s := string(buff[offset: offset + l6])
                                        partitions[i1].CommittedMetadata = &s
                                        offset += l6
                                    } else {
                                        partitions[i1].CommittedMetadata = nil
                                    }
                                } else {
                                    // non flexible and nullable
                                    var l6 int
                                    l6 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
                                    offset += 2
                                    if l6 > 0 {
                                        s := string(buff[offset: offset + l6])
                                        partitions[i1].CommittedMetadata = &s
                                        offset += l6
                                    } else {
                                        partitions[i1].CommittedMetadata = nil
                                    }
                                }
                            }
                            if version >= 8 {
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
                if version >= 8 {
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
    if version >= 8 {
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

func (m *OffsetCommitRequest) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.GroupId: The unique group identifier.
    if version >= 8 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(*m.GroupId) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.GroupId)))
    }
    if m.GroupId != nil {
        buff = append(buff, *m.GroupId...)
    }
    if version >= 1 {
        // writing m.GenerationIdOrMemberEpoch: The generation of the group if using the classic group protocol or the member epoch if using the consumer protocol.
        buff = binary.BigEndian.AppendUint32(buff, uint32(m.GenerationIdOrMemberEpoch))
        // writing m.MemberId: The member ID assigned by the group coordinator.
        if version >= 8 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*m.MemberId) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.MemberId)))
        }
        if m.MemberId != nil {
            buff = append(buff, *m.MemberId...)
        }
    }
    if version >= 7 {
        // writing m.GroupInstanceId: The unique identifier of the consumer instance provided by end user.
        if version >= 8 {
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
    if version >= 2 && version <= 4 {
        // writing m.RetentionTimeMs: The time period in ms to retain the offset.
        buff = binary.BigEndian.AppendUint64(buff, uint64(m.RetentionTimeMs))
    }
    // writing m.Topics: The topics to commit offsets for.
    if version >= 8 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.Topics) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.Topics)))
    }
    for _, topics := range m.Topics {
        // writing non tagged fields
        // writing topics.Name: The topic name.
        if version >= 8 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*topics.Name) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*topics.Name)))
        }
        if topics.Name != nil {
            buff = append(buff, *topics.Name...)
        }
        // writing topics.Partitions: Each partition to commit offsets for.
        if version >= 8 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(topics.Partitions) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint32(buff, uint32(len(topics.Partitions)))
        }
        for _, partitions := range topics.Partitions {
            // writing non tagged fields
            // writing partitions.PartitionIndex: The partition index.
            buff = binary.BigEndian.AppendUint32(buff, uint32(partitions.PartitionIndex))
            // writing partitions.CommittedOffset: The message offset to be committed.
            buff = binary.BigEndian.AppendUint64(buff, uint64(partitions.CommittedOffset))
            if version >= 6 {
                // writing partitions.CommittedLeaderEpoch: The leader epoch of this partition.
                buff = binary.BigEndian.AppendUint32(buff, uint32(partitions.CommittedLeaderEpoch))
            }
            if version >= 1 && version <= 1 {
                // writing partitions.CommitTimestamp: The timestamp of the commit.
                buff = binary.BigEndian.AppendUint64(buff, uint64(partitions.CommitTimestamp))
            }
            // writing partitions.CommittedMetadata: Any associated metadata the client wants to keep.
            if version >= 8 {
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
            if version >= 8 {
                numTaggedFields13 := 0
                // write number of tagged fields
                buff = binary.AppendUvarint(buff, uint64(numTaggedFields13))
            }
        }
        if version >= 8 {
            numTaggedFields14 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields14))
        }
    }
    if version >= 8 {
        numTaggedFields15 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields15))
    }
    return buff
}

func (m *OffsetCommitRequest) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.GroupId: The unique group identifier.
    if version >= 8 {
        // flexible and not nullable
        size += sizeofUvarint(len(*m.GroupId) + 1)
    } else {
        // non flexible and non nullable
        size += 2
    }
    if m.GroupId != nil {
        size += len(*m.GroupId)
    }
    if version >= 1 {
        // size for m.GenerationIdOrMemberEpoch: The generation of the group if using the classic group protocol or the member epoch if using the consumer protocol.
        size += 4
        // size for m.MemberId: The member ID assigned by the group coordinator.
        if version >= 8 {
            // flexible and not nullable
            size += sizeofUvarint(len(*m.MemberId) + 1)
        } else {
            // non flexible and non nullable
            size += 2
        }
        if m.MemberId != nil {
            size += len(*m.MemberId)
        }
    }
    if version >= 7 {
        // size for m.GroupInstanceId: The unique identifier of the consumer instance provided by end user.
        if version >= 8 {
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
    if version >= 2 && version <= 4 {
        // size for m.RetentionTimeMs: The time period in ms to retain the offset.
        size += 8
    }
    // size for m.Topics: The topics to commit offsets for.
    if version >= 8 {
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
        if version >= 8 {
            // flexible and not nullable
            size += sizeofUvarint(len(*topics.Name) + 1)
        } else {
            // non flexible and non nullable
            size += 2
        }
        if topics.Name != nil {
            size += len(*topics.Name)
        }
        // size for topics.Partitions: Each partition to commit offsets for.
        if version >= 8 {
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
            // size for partitions.PartitionIndex: The partition index.
            size += 4
            // size for partitions.CommittedOffset: The message offset to be committed.
            size += 8
            if version >= 6 {
                // size for partitions.CommittedLeaderEpoch: The leader epoch of this partition.
                size += 4
            }
            if version >= 1 && version <= 1 {
                // size for partitions.CommitTimestamp: The timestamp of the commit.
                size += 8
            }
            // size for partitions.CommittedMetadata: Any associated metadata the client wants to keep.
            if version >= 8 {
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
            if version >= 8 {
                // writing size of num tagged fields field
                size += sizeofUvarint(numTaggedFields3)
            }
        }
        numTaggedFields4:= 0
        numTaggedFields4 += 0
        if version >= 8 {
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields4)
        }
    }
    numTaggedFields5:= 0
    numTaggedFields5 += 0
    if version >= 8 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields5)
    }
    return size, tagSizes
}

func (m *OffsetCommitRequest) HeaderVersions(version int16) (int16, int16) {
    if version >= 8 {
        return 2, 1
    } else {
        return 1, 0
    }
}
