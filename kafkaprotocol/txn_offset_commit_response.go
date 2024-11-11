// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"
import "unsafe"

type TxnOffsetCommitResponseTxnOffsetCommitResponsePartition struct {
    // The partition index.
    PartitionIndex int32
    // The error code, or 0 if there was no error.
    ErrorCode int16
}

type TxnOffsetCommitResponseTxnOffsetCommitResponseTopic struct {
    // The topic name.
    Name *string
    // The responses for each partition in the topic.
    Partitions []TxnOffsetCommitResponseTxnOffsetCommitResponsePartition
}

type TxnOffsetCommitResponse struct {
    // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ThrottleTimeMs int32
    // The responses for each topic.
    Topics []TxnOffsetCommitResponseTxnOffsetCommitResponseTopic
}

func (m *TxnOffsetCommitResponse) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        m.ThrottleTimeMs = int32(binary.BigEndian.Uint32(buff[offset:]))
        offset += 4
    }
    {
        // reading m.Topics: The responses for each topic.
        var l0 int
        if version >= 3 {
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
            topics := make([]TxnOffsetCommitResponseTxnOffsetCommitResponseTopic, l0)
            for i0 := 0; i0 < l0; i0++ {
                // reading non tagged fields
                {
                    // reading topics[i0].Name: The topic name.
                    if version >= 3 {
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
                    // reading topics[i0].Partitions: The responses for each partition in the topic.
                    var l2 int
                    if version >= 3 {
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
                        partitions := make([]TxnOffsetCommitResponseTxnOffsetCommitResponsePartition, l2)
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

func (m *TxnOffsetCommitResponse) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    buff = binary.BigEndian.AppendUint32(buff, uint32(m.ThrottleTimeMs))
    // writing m.Topics: The responses for each topic.
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
        // writing topics.Partitions: The responses for each partition in the topic.
        if version >= 3 {
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
            // writing partitions.ErrorCode: The error code, or 0 if there was no error.
            buff = binary.BigEndian.AppendUint16(buff, uint16(partitions.ErrorCode))
            if version >= 3 {
                numTaggedFields6 := 0
                // write number of tagged fields
                buff = binary.AppendUvarint(buff, uint64(numTaggedFields6))
            }
        }
        if version >= 3 {
            numTaggedFields7 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields7))
        }
    }
    if version >= 3 {
        numTaggedFields8 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields8))
    }
    return buff
}

func (m *TxnOffsetCommitResponse) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    size += 4
    // size for m.Topics: The responses for each topic.
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
        // size for topics.Partitions: The responses for each partition in the topic.
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
            // size for partitions.PartitionIndex: The partition index.
            size += 4
            // size for partitions.ErrorCode: The error code, or 0 if there was no error.
            size += 2
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


