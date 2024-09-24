// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"
import "unsafe"

type OffsetFetchRequestOffsetFetchRequestTopic struct {
    // The topic name.
    Name *string
    // The partition indexes we would like to fetch offsets for.
    PartitionIndexes []int32
}

type OffsetFetchRequestOffsetFetchRequestTopics struct {
    // The topic name.
    Name *string
    // The partition indexes we would like to fetch offsets for.
    PartitionIndexes []int32
}

type OffsetFetchRequestOffsetFetchRequestGroup struct {
    // The group ID.
    GroupId *string
    // The member ID assigned by the group coordinator if using the new consumer protocol (KIP-848).
    MemberId *string
    // The member epoch if using the new consumer protocol (KIP-848).
    MemberEpoch int32
    // Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
    Topics []OffsetFetchRequestOffsetFetchRequestTopics
}

type OffsetFetchRequest struct {
    // The group to fetch offsets for.
    GroupId *string
    // Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
    Topics []OffsetFetchRequestOffsetFetchRequestTopic
    // Each group we would like to fetch offsets for
    Groups []OffsetFetchRequestOffsetFetchRequestGroup
    // Whether broker should hold on returning unstable offsets but set a retriable error code for the partitions.
    RequireStable bool
}

func (m *OffsetFetchRequest) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    if version <= 7 {
        {
            // reading m.GroupId: The group to fetch offsets for.
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
            // reading m.Topics: Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
            var l1 int
            if version >= 6 {
                if version >= 2 && version <= 7 {
                    // flexible and nullable
                    u, n := binary.Uvarint(buff[offset:])
                    offset += n
                    l1 = int(u - 1)
                } else {
                    // flexible and not nullable
                    u, n := binary.Uvarint(buff[offset:])
                    offset += n
                    l1 = int(u - 1)
                }
            } else {
                if version >= 2 && version <= 7 {
                    // non flexible and nullable
                    l1 = int(int32(binary.BigEndian.Uint32(buff[offset:])))
                    offset += 4
                } else {
                    // non flexible and non nullable
                    l1 = int(binary.BigEndian.Uint32(buff[offset:]))
                    offset += 4
                }
            }
            if l1 >= 0 {
                // length will be -1 if field is null
                topics := make([]OffsetFetchRequestOffsetFetchRequestTopic, l1)
                for i0 := 0; i0 < l1; i0++ {
                    // reading non tagged fields
                    {
                        // reading topics[i0].Name: The topic name.
                        if version >= 6 {
                            // flexible and not nullable
                            u, n := binary.Uvarint(buff[offset:])
                            offset += n
                            l2 := int(u - 1)
                            s := string(buff[offset: offset + l2])
                            topics[i0].Name = &s
                            offset += l2
                        } else {
                            // non flexible and non nullable
                            var l2 int
                            l2 = int(binary.BigEndian.Uint16(buff[offset:]))
                            offset += 2
                            s := string(buff[offset: offset + l2])
                            topics[i0].Name = &s
                            offset += l2
                        }
                    }
                    {
                        // reading topics[i0].PartitionIndexes: The partition indexes we would like to fetch offsets for.
                        var l3 int
                        if version >= 6 {
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
                            partitionIndexes := make([]int32, l3)
                            for i1 := 0; i1 < l3; i1++ {
                                partitionIndexes[i1] = int32(binary.BigEndian.Uint32(buff[offset:]))
                                offset += 4
                            }
                            topics[i0].PartitionIndexes = partitionIndexes
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
    if version >= 8 {
        {
            // reading m.Groups: Each group we would like to fetch offsets for
            var l4 int
            // flexible and not nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l4 = int(u - 1)
            if l4 >= 0 {
                // length will be -1 if field is null
                groups := make([]OffsetFetchRequestOffsetFetchRequestGroup, l4)
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
                    if version >= 9 {
                        {
                            // reading groups[i2].MemberId: The member ID assigned by the group coordinator if using the new consumer protocol (KIP-848).
                            // flexible and nullable
                            u, n := binary.Uvarint(buff[offset:])
                            offset += n
                            l6 := int(u - 1)
                            if l6 > 0 {
                                s := string(buff[offset: offset + l6])
                                groups[i2].MemberId = &s
                                offset += l6
                            } else {
                                groups[i2].MemberId = nil
                            }
                        }
                        {
                            // reading groups[i2].MemberEpoch: The member epoch if using the new consumer protocol (KIP-848).
                            groups[i2].MemberEpoch = int32(binary.BigEndian.Uint32(buff[offset:]))
                            offset += 4
                        }
                    }
                    {
                        // reading groups[i2].Topics: Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
                        var l7 int
                        // flexible and nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l7 = int(u - 1)
                        if l7 >= 0 {
                            // length will be -1 if field is null
                            topics := make([]OffsetFetchRequestOffsetFetchRequestTopics, l7)
                            for i3 := 0; i3 < l7; i3++ {
                                // reading non tagged fields
                                {
                                    // reading topics[i3].Name: The topic name.
                                    // flexible and not nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l8 := int(u - 1)
                                    s := string(buff[offset: offset + l8])
                                    topics[i3].Name = &s
                                    offset += l8
                                }
                                {
                                    // reading topics[i3].PartitionIndexes: The partition indexes we would like to fetch offsets for.
                                    var l9 int
                                    // flexible and not nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l9 = int(u - 1)
                                    if l9 >= 0 {
                                        // length will be -1 if field is null
                                        partitionIndexes := make([]int32, l9)
                                        for i4 := 0; i4 < l9; i4++ {
                                            partitionIndexes[i4] = int32(binary.BigEndian.Uint32(buff[offset:]))
                                            offset += 4
                                        }
                                        topics[i3].PartitionIndexes = partitionIndexes
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
    if version >= 7 {
        {
            // reading m.RequireStable: Whether broker should hold on returning unstable offsets but set a retriable error code for the partitions.
            m.RequireStable = buff[offset] == 1
            offset++
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

func (m *OffsetFetchRequest) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    if version <= 7 {
        // writing m.GroupId: The group to fetch offsets for.
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
        // writing m.Topics: Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
        if version >= 6 {
            if version >= 2 && version <= 7 {
                // flexible and nullable
                if m.Topics == nil {
                    // null
                    buff = append(buff, 0)
                } else {
                    // not null
                    buff = binary.AppendUvarint(buff, uint64(len(m.Topics) + 1))
                }
            } else {
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(m.Topics) + 1))
            }
        } else {
            if version >= 2 && version <= 7 {
                // non flexible and nullable
                if m.Topics == nil {
                    // null
                    buff = binary.BigEndian.AppendUint32(buff, 4294967295)
                } else {
                    // not null
                    buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.Topics)))
                }
            } else {
                // non flexible and non nullable
                buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.Topics)))
            }
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
            // writing topics.PartitionIndexes: The partition indexes we would like to fetch offsets for.
            if version >= 6 {
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(topics.PartitionIndexes) + 1))
            } else {
                // non flexible and non nullable
                buff = binary.BigEndian.AppendUint32(buff, uint32(len(topics.PartitionIndexes)))
            }
            for _, partitionIndexes := range topics.PartitionIndexes {
                buff = binary.BigEndian.AppendUint32(buff, uint32(partitionIndexes))
            }
            if version >= 6 {
                numTaggedFields4 := 0
                // write number of tagged fields
                buff = binary.AppendUvarint(buff, uint64(numTaggedFields4))
            }
        }
    }
    if version >= 8 {
        // writing m.Groups: Each group we would like to fetch offsets for
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
            if version >= 9 {
                // writing groups.MemberId: The member ID assigned by the group coordinator if using the new consumer protocol (KIP-848).
                // flexible and nullable
                if groups.MemberId == nil {
                    // null
                    buff = append(buff, 0)
                } else {
                    // not null
                    buff = binary.AppendUvarint(buff, uint64(len(*groups.MemberId) + 1))
                }
                if groups.MemberId != nil {
                    buff = append(buff, *groups.MemberId...)
                }
                // writing groups.MemberEpoch: The member epoch if using the new consumer protocol (KIP-848).
                buff = binary.BigEndian.AppendUint32(buff, uint32(groups.MemberEpoch))
            }
            // writing groups.Topics: Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
            // flexible and nullable
            if groups.Topics == nil {
                // null
                buff = append(buff, 0)
            } else {
                // not null
                buff = binary.AppendUvarint(buff, uint64(len(groups.Topics) + 1))
            }
            for _, topics := range groups.Topics {
                // writing non tagged fields
                // writing topics.Name: The topic name.
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(*topics.Name) + 1))
                if topics.Name != nil {
                    buff = append(buff, *topics.Name...)
                }
                // writing topics.PartitionIndexes: The partition indexes we would like to fetch offsets for.
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(topics.PartitionIndexes) + 1))
                for _, partitionIndexes := range topics.PartitionIndexes {
                    buff = binary.BigEndian.AppendUint32(buff, uint32(partitionIndexes))
                }
                numTaggedFields12 := 0
                // write number of tagged fields
                buff = binary.AppendUvarint(buff, uint64(numTaggedFields12))
            }
            numTaggedFields13 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields13))
        }
    }
    if version >= 7 {
        // writing m.RequireStable: Whether broker should hold on returning unstable offsets but set a retriable error code for the partitions.
        if m.RequireStable {
            buff = append(buff, 1)
        } else {
            buff = append(buff, 0)
        }
    }
    if version >= 6 {
        numTaggedFields15 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields15))
    }
    return buff
}

func (m *OffsetFetchRequest) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    if version <= 7 {
        // size for m.GroupId: The group to fetch offsets for.
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
        // size for m.Topics: Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
        if version >= 6 {
            if version >= 2 && version <= 7 {
                // flexible and nullable
                if m.Topics == nil {
                    // null
                    size += 1
                } else {
                    // not null
                    size += sizeofUvarint(len(m.Topics) + 1)
                }
            } else {
                // flexible and not nullable
                size += sizeofUvarint(len(m.Topics) + 1)
            }
        } else {
            if version >= 2 && version <= 7 {
                // non flexible and nullable
                size += 4
            } else {
                // non flexible and non nullable
                size += 4
            }
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
            // size for topics.PartitionIndexes: The partition indexes we would like to fetch offsets for.
            if version >= 6 {
                // flexible and not nullable
                size += sizeofUvarint(len(topics.PartitionIndexes) + 1)
            } else {
                // non flexible and non nullable
                size += 4
            }
            for _, partitionIndexes := range topics.PartitionIndexes {
                size += 0 * int(unsafe.Sizeof(partitionIndexes)) // hack to make sure loop variable is always used
                size += 4
            }
            numTaggedFields2:= 0
            numTaggedFields2 += 0
            if version >= 6 {
                // writing size of num tagged fields field
                size += sizeofUvarint(numTaggedFields2)
            }
        }
    }
    if version >= 8 {
        // size for m.Groups: Each group we would like to fetch offsets for
        // flexible and not nullable
        size += sizeofUvarint(len(m.Groups) + 1)
        for _, groups := range m.Groups {
            size += 0 * int(unsafe.Sizeof(groups)) // hack to make sure loop variable is always used
            // calculating size for non tagged fields
            numTaggedFields3:= 0
            numTaggedFields3 += 0
            // size for groups.GroupId: The group ID.
            // flexible and not nullable
            size += sizeofUvarint(len(*groups.GroupId) + 1)
            if groups.GroupId != nil {
                size += len(*groups.GroupId)
            }
            if version >= 9 {
                // size for groups.MemberId: The member ID assigned by the group coordinator if using the new consumer protocol (KIP-848).
                // flexible and nullable
                if groups.MemberId == nil {
                    // null
                    size += 1
                } else {
                    // not null
                    size += sizeofUvarint(len(*groups.MemberId) + 1)
                }
                if groups.MemberId != nil {
                    size += len(*groups.MemberId)
                }
                // size for groups.MemberEpoch: The member epoch if using the new consumer protocol (KIP-848).
                size += 4
            }
            // size for groups.Topics: Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
            // flexible and nullable
            if groups.Topics == nil {
                // null
                size += 1
            } else {
                // not null
                size += sizeofUvarint(len(groups.Topics) + 1)
            }
            for _, topics := range groups.Topics {
                size += 0 * int(unsafe.Sizeof(topics)) // hack to make sure loop variable is always used
                // calculating size for non tagged fields
                numTaggedFields4:= 0
                numTaggedFields4 += 0
                // size for topics.Name: The topic name.
                // flexible and not nullable
                size += sizeofUvarint(len(*topics.Name) + 1)
                if topics.Name != nil {
                    size += len(*topics.Name)
                }
                // size for topics.PartitionIndexes: The partition indexes we would like to fetch offsets for.
                // flexible and not nullable
                size += sizeofUvarint(len(topics.PartitionIndexes) + 1)
                for _, partitionIndexes := range topics.PartitionIndexes {
                    size += 0 * int(unsafe.Sizeof(partitionIndexes)) // hack to make sure loop variable is always used
                    size += 4
                }
                numTaggedFields5:= 0
                numTaggedFields5 += 0
                // writing size of num tagged fields field
                size += sizeofUvarint(numTaggedFields5)
            }
            numTaggedFields6:= 0
            numTaggedFields6 += 0
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields6)
        }
    }
    if version >= 7 {
        // size for m.RequireStable: Whether broker should hold on returning unstable offsets but set a retriable error code for the partitions.
        size += 1
    }
    numTaggedFields7:= 0
    numTaggedFields7 += 0
    if version >= 6 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields7)
    }
    return size, tagSizes
}

func (m *OffsetFetchRequest) HeaderVersions(version int16) (int16, int16) {
    if version >= 6 {
        return 2, 1
    } else {
        return 1, 0
    }
}

func (m *OffsetFetchRequest) SupportedApiVersions() (int16, int16) {
    return 1, 1
}
