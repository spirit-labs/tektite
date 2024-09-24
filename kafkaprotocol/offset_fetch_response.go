// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"
import "unsafe"

type OffsetFetchResponseOffsetFetchResponsePartition struct {
    // The partition index.
    PartitionIndex int32
    // The committed message offset.
    CommittedOffset int64
    // The leader epoch.
    CommittedLeaderEpoch int32
    // The partition metadata.
    Metadata *string
    // The error code, or 0 if there was no error.
    ErrorCode int16
}

type OffsetFetchResponseOffsetFetchResponseTopic struct {
    // The topic name.
    Name *string
    // The responses per partition
    Partitions []OffsetFetchResponseOffsetFetchResponsePartition
}

type OffsetFetchResponseOffsetFetchResponsePartitions struct {
    // The partition index.
    PartitionIndex int32
    // The committed message offset.
    CommittedOffset int64
    // The leader epoch.
    CommittedLeaderEpoch int32
    // The partition metadata.
    Metadata *string
    // The partition-level error code, or 0 if there was no error.
    ErrorCode int16
}

type OffsetFetchResponseOffsetFetchResponseTopics struct {
    // The topic name.
    Name *string
    // The responses per partition
    Partitions []OffsetFetchResponseOffsetFetchResponsePartitions
}

type OffsetFetchResponseOffsetFetchResponseGroup struct {
    // The group ID.
    GroupId *string
    // The responses per topic.
    Topics []OffsetFetchResponseOffsetFetchResponseTopics
    // The group-level error code, or 0 if there was no error.
    ErrorCode int16
}

type OffsetFetchResponse struct {
    // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ThrottleTimeMs int32
    // The responses per topic.
    Topics []OffsetFetchResponseOffsetFetchResponseTopic
    // The top-level error code, or 0 if there was no error.
    ErrorCode int16
    // The responses per group id.
    Groups []OffsetFetchResponseOffsetFetchResponseGroup
}

func (m *OffsetFetchResponse) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    if version >= 3 {
        {
            // reading m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
            m.ThrottleTimeMs = int32(binary.BigEndian.Uint32(buff[offset:]))
            offset += 4
        }
    }
    if version <= 7 {
        {
            // reading m.Topics: The responses per topic.
            var l0 int
            if version >= 6 {
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
                topics := make([]OffsetFetchResponseOffsetFetchResponseTopic, l0)
                for i0 := 0; i0 < l0; i0++ {
                    // reading non tagged fields
                    {
                        // reading topics[i0].Name: The topic name.
                        if version >= 6 {
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
                        // reading topics[i0].Partitions: The responses per partition
                        var l2 int
                        if version >= 6 {
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
                            partitions := make([]OffsetFetchResponseOffsetFetchResponsePartition, l2)
                            for i1 := 0; i1 < l2; i1++ {
                                // reading non tagged fields
                                {
                                    // reading partitions[i1].PartitionIndex: The partition index.
                                    partitions[i1].PartitionIndex = int32(binary.BigEndian.Uint32(buff[offset:]))
                                    offset += 4
                                }
                                {
                                    // reading partitions[i1].CommittedOffset: The committed message offset.
                                    partitions[i1].CommittedOffset = int64(binary.BigEndian.Uint64(buff[offset:]))
                                    offset += 8
                                }
                                if version >= 5 && version <= 7 {
                                    {
                                        // reading partitions[i1].CommittedLeaderEpoch: The leader epoch.
                                        partitions[i1].CommittedLeaderEpoch = int32(binary.BigEndian.Uint32(buff[offset:]))
                                        offset += 4
                                    }
                                }
                                {
                                    // reading partitions[i1].Metadata: The partition metadata.
                                    if version >= 6 {
                                        if version <= 7 {
                                            // flexible and nullable
                                            u, n := binary.Uvarint(buff[offset:])
                                            offset += n
                                            l3 := int(u - 1)
                                            if l3 > 0 {
                                                s := string(buff[offset: offset + l3])
                                                partitions[i1].Metadata = &s
                                                offset += l3
                                            } else {
                                                partitions[i1].Metadata = nil
                                            }
                                        } else {
                                            // flexible and not nullable
                                            u, n := binary.Uvarint(buff[offset:])
                                            offset += n
                                            l3 := int(u - 1)
                                            s := string(buff[offset: offset + l3])
                                            partitions[i1].Metadata = &s
                                            offset += l3
                                        }
                                    } else {
                                        if version <= 7 {
                                            // non flexible and nullable
                                            var l3 int
                                            l3 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
                                            offset += 2
                                            if l3 > 0 {
                                                s := string(buff[offset: offset + l3])
                                                partitions[i1].Metadata = &s
                                                offset += l3
                                            } else {
                                                partitions[i1].Metadata = nil
                                            }
                                        } else {
                                            // non flexible and non nullable
                                            var l3 int
                                            l3 = int(binary.BigEndian.Uint16(buff[offset:]))
                                            offset += 2
                                            s := string(buff[offset: offset + l3])
                                            partitions[i1].Metadata = &s
                                            offset += l3
                                        }
                                    }
                                }
                                {
                                    // reading partitions[i1].ErrorCode: The error code, or 0 if there was no error.
                                    partitions[i1].ErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
                                    offset += 2
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
                        topics[i0].Partitions = partitions
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
            m.Topics = topics
            }
        }
    }
    if version >= 2 && version <= 7 {
        {
            // reading m.ErrorCode: The top-level error code, or 0 if there was no error.
            m.ErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
            offset += 2
        }
    }
    if version >= 8 {
        {
            // reading m.Groups: The responses per group id.
            var l4 int
            // flexible and not nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l4 = int(u - 1)
            if l4 >= 0 {
                // length will be -1 if field is null
                groups := make([]OffsetFetchResponseOffsetFetchResponseGroup, l4)
                for i2 := 0; i2 < l4; i2++ {
                    // reading non tagged fields
                    {
                        // reading groups[i2].GroupId: The group ID.
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l5 := int(u - 1)
                        s := string(buff[offset: offset + l5])
                        groups[i2].GroupId = &s
                        offset += l5
                    }
                    {
                        // reading groups[i2].Topics: The responses per topic.
                        var l6 int
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l6 = int(u - 1)
                        if l6 >= 0 {
                            // length will be -1 if field is null
                            topics := make([]OffsetFetchResponseOffsetFetchResponseTopics, l6)
                            for i3 := 0; i3 < l6; i3++ {
                                // reading non tagged fields
                                {
                                    // reading topics[i3].Name: The topic name.
                                    // flexible and not nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l7 := int(u - 1)
                                    s := string(buff[offset: offset + l7])
                                    topics[i3].Name = &s
                                    offset += l7
                                }
                                {
                                    // reading topics[i3].Partitions: The responses per partition
                                    var l8 int
                                    // flexible and not nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l8 = int(u - 1)
                                    if l8 >= 0 {
                                        // length will be -1 if field is null
                                        partitions := make([]OffsetFetchResponseOffsetFetchResponsePartitions, l8)
                                        for i4 := 0; i4 < l8; i4++ {
                                            // reading non tagged fields
                                            {
                                                // reading partitions[i4].PartitionIndex: The partition index.
                                                partitions[i4].PartitionIndex = int32(binary.BigEndian.Uint32(buff[offset:]))
                                                offset += 4
                                            }
                                            {
                                                // reading partitions[i4].CommittedOffset: The committed message offset.
                                                partitions[i4].CommittedOffset = int64(binary.BigEndian.Uint64(buff[offset:]))
                                                offset += 8
                                            }
                                            {
                                                // reading partitions[i4].CommittedLeaderEpoch: The leader epoch.
                                                partitions[i4].CommittedLeaderEpoch = int32(binary.BigEndian.Uint32(buff[offset:]))
                                                offset += 4
                                            }
                                            {
                                                // reading partitions[i4].Metadata: The partition metadata.
                                                // flexible and nullable
                                                u, n := binary.Uvarint(buff[offset:])
                                                offset += n
                                                l9 := int(u - 1)
                                                if l9 > 0 {
                                                    s := string(buff[offset: offset + l9])
                                                    partitions[i4].Metadata = &s
                                                    offset += l9
                                                } else {
                                                    partitions[i4].Metadata = nil
                                                }
                                            }
                                            {
                                                // reading partitions[i4].ErrorCode: The partition-level error code, or 0 if there was no error.
                                                partitions[i4].ErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
                                                offset += 2
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
                                    topics[i3].Partitions = partitions
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
                        groups[i2].Topics = topics
                        }
                    }
                    {
                        // reading groups[i2].ErrorCode: The group-level error code, or 0 if there was no error.
                        groups[i2].ErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
                        offset += 2
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
            m.Groups = groups
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

func (m *OffsetFetchResponse) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    if version >= 3 {
        // writing m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        buff = binary.BigEndian.AppendUint32(buff, uint32(m.ThrottleTimeMs))
    }
    if version <= 7 {
        // writing m.Topics: The responses per topic.
        if version >= 6 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(m.Topics) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.Topics)))
        }
        for _, topics := range m.Topics {
            // writing non tagged fields
            // writing topics.Name: The topic name.
            if version >= 6 {
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(*topics.Name) + 1))
            } else {
                // non flexible and non nullable
                buff = binary.BigEndian.AppendUint16(buff, uint16(len(*topics.Name)))
            }
            if topics.Name != nil {
                buff = append(buff, *topics.Name...)
            }
            // writing topics.Partitions: The responses per partition
            if version >= 6 {
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
                // writing partitions.CommittedOffset: The committed message offset.
                buff = binary.BigEndian.AppendUint64(buff, uint64(partitions.CommittedOffset))
                if version >= 5 && version <= 7 {
                    // writing partitions.CommittedLeaderEpoch: The leader epoch.
                    buff = binary.BigEndian.AppendUint32(buff, uint32(partitions.CommittedLeaderEpoch))
                }
                // writing partitions.Metadata: The partition metadata.
                if version >= 6 {
                    if version <= 7 {
                        // flexible and nullable
                        if partitions.Metadata == nil {
                            // null
                            buff = append(buff, 0)
                        } else {
                            // not null
                            buff = binary.AppendUvarint(buff, uint64(len(*partitions.Metadata) + 1))
                        }
                    } else {
                        // flexible and not nullable
                        buff = binary.AppendUvarint(buff, uint64(len(*partitions.Metadata) + 1))
                    }
                } else {
                    if version <= 7 {
                        // non flexible and nullable
                        if partitions.Metadata == nil {
                            // null
                            buff = binary.BigEndian.AppendUint16(buff, 65535)
                        } else {
                            // not null
                            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*partitions.Metadata)))
                        }
                    } else {
                        // non flexible and non nullable
                        buff = binary.BigEndian.AppendUint16(buff, uint16(len(*partitions.Metadata)))
                    }
                }
                if partitions.Metadata != nil {
                    buff = append(buff, *partitions.Metadata...)
                }
                // writing partitions.ErrorCode: The error code, or 0 if there was no error.
                buff = binary.BigEndian.AppendUint16(buff, uint16(partitions.ErrorCode))
                if version >= 6 {
                    numTaggedFields9 := 0
                    // write number of tagged fields
                    buff = binary.AppendUvarint(buff, uint64(numTaggedFields9))
                }
            }
            if version >= 6 {
                numTaggedFields10 := 0
                // write number of tagged fields
                buff = binary.AppendUvarint(buff, uint64(numTaggedFields10))
            }
        }
    }
    if version >= 2 && version <= 7 {
        // writing m.ErrorCode: The top-level error code, or 0 if there was no error.
        buff = binary.BigEndian.AppendUint16(buff, uint16(m.ErrorCode))
    }
    if version >= 8 {
        // writing m.Groups: The responses per group id.
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.Groups) + 1))
        for _, groups := range m.Groups {
            // writing non tagged fields
            // writing groups.GroupId: The group ID.
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*groups.GroupId) + 1))
            if groups.GroupId != nil {
                buff = append(buff, *groups.GroupId...)
            }
            // writing groups.Topics: The responses per topic.
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(groups.Topics) + 1))
            for _, topics := range groups.Topics {
                // writing non tagged fields
                // writing topics.Name: The topic name.
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(*topics.Name) + 1))
                if topics.Name != nil {
                    buff = append(buff, *topics.Name...)
                }
                // writing topics.Partitions: The responses per partition
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(topics.Partitions) + 1))
                for _, partitions := range topics.Partitions {
                    // writing non tagged fields
                    // writing partitions.PartitionIndex: The partition index.
                    buff = binary.BigEndian.AppendUint32(buff, uint32(partitions.PartitionIndex))
                    // writing partitions.CommittedOffset: The committed message offset.
                    buff = binary.BigEndian.AppendUint64(buff, uint64(partitions.CommittedOffset))
                    // writing partitions.CommittedLeaderEpoch: The leader epoch.
                    buff = binary.BigEndian.AppendUint32(buff, uint32(partitions.CommittedLeaderEpoch))
                    // writing partitions.Metadata: The partition metadata.
                    // flexible and nullable
                    if partitions.Metadata == nil {
                        // null
                        buff = append(buff, 0)
                    } else {
                        // not null
                        buff = binary.AppendUvarint(buff, uint64(len(*partitions.Metadata) + 1))
                    }
                    if partitions.Metadata != nil {
                        buff = append(buff, *partitions.Metadata...)
                    }
                    // writing partitions.ErrorCode: The partition-level error code, or 0 if there was no error.
                    buff = binary.BigEndian.AppendUint16(buff, uint16(partitions.ErrorCode))
                    numTaggedFields22 := 0
                    // write number of tagged fields
                    buff = binary.AppendUvarint(buff, uint64(numTaggedFields22))
                }
                numTaggedFields23 := 0
                // write number of tagged fields
                buff = binary.AppendUvarint(buff, uint64(numTaggedFields23))
            }
            // writing groups.ErrorCode: The group-level error code, or 0 if there was no error.
            buff = binary.BigEndian.AppendUint16(buff, uint16(groups.ErrorCode))
            numTaggedFields25 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields25))
        }
    }
    if version >= 6 {
        numTaggedFields26 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields26))
    }
    return buff
}

func (m *OffsetFetchResponse) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    if version >= 3 {
        // size for m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        size += 4
    }
    if version <= 7 {
        // size for m.Topics: The responses per topic.
        if version >= 6 {
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
            if version >= 6 {
                // flexible and not nullable
                size += sizeofUvarint(len(*topics.Name) + 1)
            } else {
                // non flexible and non nullable
                size += 2
            }
            if topics.Name != nil {
                size += len(*topics.Name)
            }
            // size for topics.Partitions: The responses per partition
            if version >= 6 {
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
                // size for partitions.CommittedOffset: The committed message offset.
                size += 8
                if version >= 5 && version <= 7 {
                    // size for partitions.CommittedLeaderEpoch: The leader epoch.
                    size += 4
                }
                // size for partitions.Metadata: The partition metadata.
                if version >= 6 {
                    if version <= 7 {
                        // flexible and nullable
                        if partitions.Metadata == nil {
                            // null
                            size += 1
                        } else {
                            // not null
                            size += sizeofUvarint(len(*partitions.Metadata) + 1)
                        }
                    } else {
                        // flexible and not nullable
                        size += sizeofUvarint(len(*partitions.Metadata) + 1)
                    }
                } else {
                    if version <= 7 {
                        // non flexible and nullable
                        size += 2
                    } else {
                        // non flexible and non nullable
                        size += 2
                    }
                }
                if partitions.Metadata != nil {
                    size += len(*partitions.Metadata)
                }
                // size for partitions.ErrorCode: The error code, or 0 if there was no error.
                size += 2
                numTaggedFields3:= 0
                numTaggedFields3 += 0
                if version >= 6 {
                    // writing size of num tagged fields field
                    size += sizeofUvarint(numTaggedFields3)
                }
            }
            numTaggedFields4:= 0
            numTaggedFields4 += 0
            if version >= 6 {
                // writing size of num tagged fields field
                size += sizeofUvarint(numTaggedFields4)
            }
        }
    }
    if version >= 2 && version <= 7 {
        // size for m.ErrorCode: The top-level error code, or 0 if there was no error.
        size += 2
    }
    if version >= 8 {
        // size for m.Groups: The responses per group id.
        // flexible and not nullable
        size += sizeofUvarint(len(m.Groups) + 1)
        for _, groups := range m.Groups {
            size += 0 * int(unsafe.Sizeof(groups)) // hack to make sure loop variable is always used
            // calculating size for non tagged fields
            numTaggedFields5:= 0
            numTaggedFields5 += 0
            // size for groups.GroupId: The group ID.
            // flexible and not nullable
            size += sizeofUvarint(len(*groups.GroupId) + 1)
            if groups.GroupId != nil {
                size += len(*groups.GroupId)
            }
            // size for groups.Topics: The responses per topic.
            // flexible and not nullable
            size += sizeofUvarint(len(groups.Topics) + 1)
            for _, topics := range groups.Topics {
                size += 0 * int(unsafe.Sizeof(topics)) // hack to make sure loop variable is always used
                // calculating size for non tagged fields
                numTaggedFields6:= 0
                numTaggedFields6 += 0
                // size for topics.Name: The topic name.
                // flexible and not nullable
                size += sizeofUvarint(len(*topics.Name) + 1)
                if topics.Name != nil {
                    size += len(*topics.Name)
                }
                // size for topics.Partitions: The responses per partition
                // flexible and not nullable
                size += sizeofUvarint(len(topics.Partitions) + 1)
                for _, partitions := range topics.Partitions {
                    size += 0 * int(unsafe.Sizeof(partitions)) // hack to make sure loop variable is always used
                    // calculating size for non tagged fields
                    numTaggedFields7:= 0
                    numTaggedFields7 += 0
                    // size for partitions.PartitionIndex: The partition index.
                    size += 4
                    // size for partitions.CommittedOffset: The committed message offset.
                    size += 8
                    // size for partitions.CommittedLeaderEpoch: The leader epoch.
                    size += 4
                    // size for partitions.Metadata: The partition metadata.
                    // flexible and nullable
                    if partitions.Metadata == nil {
                        // null
                        size += 1
                    } else {
                        // not null
                        size += sizeofUvarint(len(*partitions.Metadata) + 1)
                    }
                    if partitions.Metadata != nil {
                        size += len(*partitions.Metadata)
                    }
                    // size for partitions.ErrorCode: The partition-level error code, or 0 if there was no error.
                    size += 2
                    numTaggedFields8:= 0
                    numTaggedFields8 += 0
                    // writing size of num tagged fields field
                    size += sizeofUvarint(numTaggedFields8)
                }
                numTaggedFields9:= 0
                numTaggedFields9 += 0
                // writing size of num tagged fields field
                size += sizeofUvarint(numTaggedFields9)
            }
            // size for groups.ErrorCode: The group-level error code, or 0 if there was no error.
            size += 2
            numTaggedFields10:= 0
            numTaggedFields10 += 0
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields10)
        }
    }
    numTaggedFields11:= 0
    numTaggedFields11 += 0
    if version >= 6 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields11)
    }
    return size, tagSizes
}


