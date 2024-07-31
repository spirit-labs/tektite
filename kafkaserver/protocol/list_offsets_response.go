// Package protocol - This is a generated file, please do not edit

package protocol

import "encoding/binary"
import "unsafe"

type ListOffsetsResponseListOffsetsPartitionResponse struct {
    // The partition index.
    PartitionIndex int32
    // The partition error code, or 0 if there was no error.
    ErrorCode int16
    // The result offsets.
    OldStyleOffsets []int64
    // The timestamp associated with the returned offset.
    Timestamp int64
    // The returned offset.
    Offset int64
    LeaderEpoch int32
}

type ListOffsetsResponseListOffsetsTopicResponse struct {
    // The topic name
    Name *string
    // Each partition in the response.
    Partitions []ListOffsetsResponseListOffsetsPartitionResponse
}

type ListOffsetsResponse struct {
    // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ThrottleTimeMs int32
    // Each topic in the response.
    Topics []ListOffsetsResponseListOffsetsTopicResponse
}

func (m *ListOffsetsResponse) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    if version >= 2 {
        {
            // reading m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
            m.ThrottleTimeMs = int32(binary.BigEndian.Uint32(buff[offset:]))
            offset += 4
        }
    }
    {
        // reading m.Topics: Each topic in the response.
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
            topics := make([]ListOffsetsResponseListOffsetsTopicResponse, l0)
            for i0 := 0; i0 < l0; i0++ {
                // reading non tagged fields
                {
                    // reading topics[i0].Name: The topic name
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
                    // reading topics[i0].Partitions: Each partition in the response.
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
                        partitions := make([]ListOffsetsResponseListOffsetsPartitionResponse, l2)
                        for i1 := 0; i1 < l2; i1++ {
                            // reading non tagged fields
                            {
                                // reading partitions[i1].PartitionIndex: The partition index.
                                partitions[i1].PartitionIndex = int32(binary.BigEndian.Uint32(buff[offset:]))
                                offset += 4
                            }
                            {
                                // reading partitions[i1].ErrorCode: The partition error code, or 0 if there was no error.
                                partitions[i1].ErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
                                offset += 2
                            }
                            if version <= 0 {
                                {
                                    // reading partitions[i1].OldStyleOffsets: The result offsets.
                                    var l3 int
                                    // non flexible and non nullable
                                    l3 = int(binary.BigEndian.Uint32(buff[offset:]))
                                    offset += 4
                                    if l3 >= 0 {
                                        // length will be -1 if field is null
                                        oldStyleOffsets := make([]int64, l3)
                                        for i2 := 0; i2 < l3; i2++ {
                                            oldStyleOffsets[i2] = int64(binary.BigEndian.Uint64(buff[offset:]))
                                            offset += 8
                                        }
                                        partitions[i1].OldStyleOffsets = oldStyleOffsets
                                    }
                                }
                            }
                            if version >= 1 {
                                {
                                    // reading partitions[i1].Timestamp: The timestamp associated with the returned offset.
                                    partitions[i1].Timestamp = int64(binary.BigEndian.Uint64(buff[offset:]))
                                    offset += 8
                                }
                                {
                                    // reading partitions[i1].Offset: The returned offset.
                                    partitions[i1].Offset = int64(binary.BigEndian.Uint64(buff[offset:]))
                                    offset += 8
                                }
                            }
                            if version >= 4 {
                                {
                                    // reading partitions[i1].LeaderEpoch: 
                                    partitions[i1].LeaderEpoch = int32(binary.BigEndian.Uint32(buff[offset:]))
                                    offset += 4
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

func (m *ListOffsetsResponse) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    if version >= 2 {
        // writing m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        buff = binary.BigEndian.AppendUint32(buff, uint32(m.ThrottleTimeMs))
    }
    // writing m.Topics: Each topic in the response.
    if version >= 6 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.Topics) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.Topics)))
    }
    for _, topics := range m.Topics {
        // writing non tagged fields
        // writing topics.Name: The topic name
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
        // writing topics.Partitions: Each partition in the response.
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
            // writing partitions.ErrorCode: The partition error code, or 0 if there was no error.
            buff = binary.BigEndian.AppendUint16(buff, uint16(partitions.ErrorCode))
            if version <= 0 {
                // writing partitions.OldStyleOffsets: The result offsets.
                // non flexible and non nullable
                buff = binary.BigEndian.AppendUint32(buff, uint32(len(partitions.OldStyleOffsets)))
                for _, oldStyleOffsets := range partitions.OldStyleOffsets {
                    buff = binary.BigEndian.AppendUint64(buff, uint64(oldStyleOffsets))
                }
            }
            if version >= 1 {
                // writing partitions.Timestamp: The timestamp associated with the returned offset.
                buff = binary.BigEndian.AppendUint64(buff, uint64(partitions.Timestamp))
                // writing partitions.Offset: The returned offset.
                buff = binary.BigEndian.AppendUint64(buff, uint64(partitions.Offset))
            }
            if version >= 4 {
                // writing partitions.LeaderEpoch: 
                buff = binary.BigEndian.AppendUint32(buff, uint32(partitions.LeaderEpoch))
            }
            if version >= 6 {
                numTaggedFields10 := 0
                // write number of tagged fields
                buff = binary.AppendUvarint(buff, uint64(numTaggedFields10))
            }
        }
        if version >= 6 {
            numTaggedFields11 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields11))
        }
    }
    if version >= 6 {
        numTaggedFields12 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields12))
    }
    return buff
}

func (m *ListOffsetsResponse) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    if version >= 2 {
        // size for m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        size += 4
    }
    // size for m.Topics: Each topic in the response.
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
        // size for topics.Name: The topic name
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
        // size for topics.Partitions: Each partition in the response.
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
            // size for partitions.ErrorCode: The partition error code, or 0 if there was no error.
            size += 2
            if version <= 0 {
                // size for partitions.OldStyleOffsets: The result offsets.
                // non flexible and non nullable
                size += 4
                for _, oldStyleOffsets := range partitions.OldStyleOffsets {
                    size += 0 * int(unsafe.Sizeof(oldStyleOffsets)) // hack to make sure loop variable is always used
                    size += 8
                }
            }
            if version >= 1 {
                // size for partitions.Timestamp: The timestamp associated with the returned offset.
                size += 8
                // size for partitions.Offset: The returned offset.
                size += 8
            }
            if version >= 4 {
                // size for partitions.LeaderEpoch: 
                size += 4
            }
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
    numTaggedFields5:= 0
    numTaggedFields5 += 0
    if version >= 6 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields5)
    }
    return size, tagSizes
}

