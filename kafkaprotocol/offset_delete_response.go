// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"
import "unsafe"

type OffsetDeleteResponseOffsetDeleteResponsePartition struct {
    // The partition index.
    PartitionIndex int32
    // The error code, or 0 if there was no error.
    ErrorCode int16
}

type OffsetDeleteResponseOffsetDeleteResponseTopic struct {
    // The topic name.
    Name *string
    // The responses for each partition in the topic.
    Partitions []OffsetDeleteResponseOffsetDeleteResponsePartition
}

type OffsetDeleteResponse struct {
    // The top-level error code, or 0 if there was no error.
    ErrorCode int16
    // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ThrottleTimeMs int32
    // The responses for each topic.
    Topics []OffsetDeleteResponseOffsetDeleteResponseTopic
}

func (m *OffsetDeleteResponse) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.ErrorCode: The top-level error code, or 0 if there was no error.
        m.ErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
        offset += 2
    }
    {
        // reading m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        m.ThrottleTimeMs = int32(binary.BigEndian.Uint32(buff[offset:]))
        offset += 4
    }
    {
        // reading m.Topics: The responses for each topic.
        var l0 int
        // non flexible and non nullable
        l0 = int(binary.BigEndian.Uint32(buff[offset:]))
        offset += 4
        if l0 >= 0 {
            // length will be -1 if field is null
            topics := make([]OffsetDeleteResponseOffsetDeleteResponseTopic, l0)
            for i0 := 0; i0 < l0; i0++ {
                // reading non tagged fields
                {
                    // reading topics[i0].Name: The topic name.
                    // non flexible and non nullable
                    var l1 int
                    l1 = int(binary.BigEndian.Uint16(buff[offset:]))
                    offset += 2
                    s := string(buff[offset: offset + l1])
                    topics[i0].Name = &s
                    offset += l1
                }
                {
                    // reading topics[i0].Partitions: The responses for each partition in the topic.
                    var l2 int
                    // non flexible and non nullable
                    l2 = int(binary.BigEndian.Uint32(buff[offset:]))
                    offset += 4
                    if l2 >= 0 {
                        // length will be -1 if field is null
                        partitions := make([]OffsetDeleteResponseOffsetDeleteResponsePartition, l2)
                        for i1 := 0; i1 < l2; i1++ {
                            // reading non tagged fields
                            {
                                // reading partitions[i1].PartitionIndex: The partition index.
                                partitions[i1].PartitionIndex = int32(binary.BigEndian.Uint32(buff[offset:]))
                                offset += 4
                            }
                            {
                                // reading partitions[i1].ErrorCode: The error code, or 0 if there was no error.
                                partitions[i1].ErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
                                offset += 2
                            }
                        }
                    topics[i0].Partitions = partitions
                    }
                }
            }
        m.Topics = topics
        }
    }
    return offset, nil
}

func (m *OffsetDeleteResponse) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.ErrorCode: The top-level error code, or 0 if there was no error.
    buff = binary.BigEndian.AppendUint16(buff, uint16(m.ErrorCode))
    // writing m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    buff = binary.BigEndian.AppendUint32(buff, uint32(m.ThrottleTimeMs))
    // writing m.Topics: The responses for each topic.
    // non flexible and non nullable
    buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.Topics)))
    for _, topics := range m.Topics {
        // writing non tagged fields
        // writing topics.Name: The topic name.
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint16(buff, uint16(len(*topics.Name)))
        if topics.Name != nil {
            buff = append(buff, *topics.Name...)
        }
        // writing topics.Partitions: The responses for each partition in the topic.
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint32(buff, uint32(len(topics.Partitions)))
        for _, partitions := range topics.Partitions {
            // writing non tagged fields
            // writing partitions.PartitionIndex: The partition index.
            buff = binary.BigEndian.AppendUint32(buff, uint32(partitions.PartitionIndex))
            // writing partitions.ErrorCode: The error code, or 0 if there was no error.
            buff = binary.BigEndian.AppendUint16(buff, uint16(partitions.ErrorCode))
        }
    }
    return buff
}

func (m *OffsetDeleteResponse) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    // size for m.ErrorCode: The top-level error code, or 0 if there was no error.
    size += 2
    // size for m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    size += 4
    // size for m.Topics: The responses for each topic.
    // non flexible and non nullable
    size += 4
    for _, topics := range m.Topics {
        size += 0 * int(unsafe.Sizeof(topics)) // hack to make sure loop variable is always used
        // calculating size for non tagged fields
        // size for topics.Name: The topic name.
        // non flexible and non nullable
        size += 2
        if topics.Name != nil {
            size += len(*topics.Name)
        }
        // size for topics.Partitions: The responses for each partition in the topic.
        // non flexible and non nullable
        size += 4
        for _, partitions := range topics.Partitions {
            size += 0 * int(unsafe.Sizeof(partitions)) // hack to make sure loop variable is always used
            // calculating size for non tagged fields
            // size for partitions.PartitionIndex: The partition index.
            size += 4
            // size for partitions.ErrorCode: The error code, or 0 if there was no error.
            size += 2
        }
    }
    return size, tagSizes
}


