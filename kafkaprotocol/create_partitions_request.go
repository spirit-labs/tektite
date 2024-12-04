// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"
import "unsafe"

type CreatePartitionsRequestCreatePartitionsAssignment struct {
    // The assigned broker IDs.
    BrokerIds []int32
}

type CreatePartitionsRequestCreatePartitionsTopic struct {
    // The topic name.
    Name *string
    // The new partition count.
    Count int32
    // The new partition assignments.
    Assignments []CreatePartitionsRequestCreatePartitionsAssignment
}

type CreatePartitionsRequest struct {
    // Each topic that we want to create new partitions inside.
    Topics []CreatePartitionsRequestCreatePartitionsTopic
    // The time in ms to wait for the partitions to be created.
    TimeoutMs int32
    // If true, then validate the request, but don't actually increase the number of partitions.
    ValidateOnly bool
}

func (m *CreatePartitionsRequest) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.Topics: Each topic that we want to create new partitions inside.
        var l0 int
        if version >= 2 {
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
            topics := make([]CreatePartitionsRequestCreatePartitionsTopic, l0)
            for i0 := 0; i0 < l0; i0++ {
                // reading non tagged fields
                {
                    // reading topics[i0].Name: The topic name.
                    if version >= 2 {
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
                    // reading topics[i0].Count: The new partition count.
                    topics[i0].Count = int32(binary.BigEndian.Uint32(buff[offset:]))
                    offset += 4
                }
                {
                    // reading topics[i0].Assignments: The new partition assignments.
                    var l2 int
                    if version >= 2 {
                        // flexible and nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l2 = int(u - 1)
                    } else {
                        // non flexible and nullable
                        l2 = int(int32(binary.BigEndian.Uint32(buff[offset:])))
                        offset += 4
                    }
                    if l2 >= 0 {
                        // length will be -1 if field is null
                        assignments := make([]CreatePartitionsRequestCreatePartitionsAssignment, l2)
                        for i1 := 0; i1 < l2; i1++ {
                            // reading non tagged fields
                            {
                                // reading assignments[i1].BrokerIds: The assigned broker IDs.
                                var l3 int
                                if version >= 2 {
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
                            if version >= 2 {
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
                if version >= 2 {
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
        // reading m.TimeoutMs: The time in ms to wait for the partitions to be created.
        m.TimeoutMs = int32(binary.BigEndian.Uint32(buff[offset:]))
        offset += 4
    }
    {
        // reading m.ValidateOnly: If true, then validate the request, but don't actually increase the number of partitions.
        m.ValidateOnly = buff[offset] == 1
        offset++
    }
    if version >= 2 {
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

func (m *CreatePartitionsRequest) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.Topics: Each topic that we want to create new partitions inside.
    if version >= 2 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.Topics) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.Topics)))
    }
    for _, topics := range m.Topics {
        // writing non tagged fields
        // writing topics.Name: The topic name.
        if version >= 2 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*topics.Name) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*topics.Name)))
        }
        if topics.Name != nil {
            buff = append(buff, *topics.Name...)
        }
        // writing topics.Count: The new partition count.
        buff = binary.BigEndian.AppendUint32(buff, uint32(topics.Count))
        // writing topics.Assignments: The new partition assignments.
        if version >= 2 {
            // flexible and nullable
            if topics.Assignments == nil {
                // null
                buff = append(buff, 0)
            } else {
                // not null
                buff = binary.AppendUvarint(buff, uint64(len(topics.Assignments) + 1))
            }
        } else {
            // non flexible and nullable
            if topics.Assignments == nil {
                // null
                buff = binary.BigEndian.AppendUint32(buff, 4294967295)
            } else {
                // not null
                buff = binary.BigEndian.AppendUint32(buff, uint32(len(topics.Assignments)))
            }
        }
        for _, assignments := range topics.Assignments {
            // writing non tagged fields
            // writing assignments.BrokerIds: The assigned broker IDs.
            if version >= 2 {
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(assignments.BrokerIds) + 1))
            } else {
                // non flexible and non nullable
                buff = binary.BigEndian.AppendUint32(buff, uint32(len(assignments.BrokerIds)))
            }
            for _, brokerIds := range assignments.BrokerIds {
                buff = binary.BigEndian.AppendUint32(buff, uint32(brokerIds))
            }
            if version >= 2 {
                numTaggedFields5 := 0
                // write number of tagged fields
                buff = binary.AppendUvarint(buff, uint64(numTaggedFields5))
            }
        }
        if version >= 2 {
            numTaggedFields6 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields6))
        }
    }
    // writing m.TimeoutMs: The time in ms to wait for the partitions to be created.
    buff = binary.BigEndian.AppendUint32(buff, uint32(m.TimeoutMs))
    // writing m.ValidateOnly: If true, then validate the request, but don't actually increase the number of partitions.
    if m.ValidateOnly {
        buff = append(buff, 1)
    } else {
        buff = append(buff, 0)
    }
    if version >= 2 {
        numTaggedFields9 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields9))
    }
    return buff
}

func (m *CreatePartitionsRequest) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.Topics: Each topic that we want to create new partitions inside.
    if version >= 2 {
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
        if version >= 2 {
            // flexible and not nullable
            size += sizeofUvarint(len(*topics.Name) + 1)
        } else {
            // non flexible and non nullable
            size += 2
        }
        if topics.Name != nil {
            size += len(*topics.Name)
        }
        // size for topics.Count: The new partition count.
        size += 4
        // size for topics.Assignments: The new partition assignments.
        if version >= 2 {
            // flexible and nullable
            if topics.Assignments == nil {
                // null
                size += 1
            } else {
                // not null
                size += sizeofUvarint(len(topics.Assignments) + 1)
            }
        } else {
            // non flexible and nullable
            size += 4
        }
        for _, assignments := range topics.Assignments {
            size += 0 * int(unsafe.Sizeof(assignments)) // hack to make sure loop variable is always used
            // calculating size for non tagged fields
            numTaggedFields2:= 0
            numTaggedFields2 += 0
            // size for assignments.BrokerIds: The assigned broker IDs.
            if version >= 2 {
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
            if version >= 2 {
                // writing size of num tagged fields field
                size += sizeofUvarint(numTaggedFields3)
            }
        }
        numTaggedFields4:= 0
        numTaggedFields4 += 0
        if version >= 2 {
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields4)
        }
    }
    // size for m.TimeoutMs: The time in ms to wait for the partitions to be created.
    size += 4
    // size for m.ValidateOnly: If true, then validate the request, but don't actually increase the number of partitions.
    size += 1
    numTaggedFields5:= 0
    numTaggedFields5 += 0
    if version >= 2 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields5)
    }
    return size, tagSizes
}

func (m *CreatePartitionsRequest) HeaderVersions(version int16) (int16, int16) {
    if version >= 2 {
        return 2, 1
    } else {
        return 1, 0
    }
}

func (m *CreatePartitionsRequest) SupportedApiVersions() (int16, int16) {
    return 0, 0
}
