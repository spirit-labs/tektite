// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"
import "unsafe"

type ListOffsetsRequestListOffsetsPartition struct {
    // The partition index.
    PartitionIndex int32
    // The current leader epoch.
    CurrentLeaderEpoch int32
    // The current timestamp.
    Timestamp int64
    // The maximum number of offsets to report.
    MaxNumOffsets int32
}

type ListOffsetsRequestListOffsetsTopic struct {
    // The topic name.
    Name *string
    // Each partition in the request.
    Partitions []ListOffsetsRequestListOffsetsPartition
}

type ListOffsetsRequest struct {
    // The broker ID of the requester, or -1 if this request is being made by a normal consumer.
    ReplicaId int32
    // This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records
    IsolationLevel int8
    // Each topic in the request.
    Topics []ListOffsetsRequestListOffsetsTopic
}

func (m *ListOffsetsRequest) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.ReplicaId: The broker ID of the requester, or -1 if this request is being made by a normal consumer.
        m.ReplicaId = int32(binary.BigEndian.Uint32(buff[offset:]))
        offset += 4
    }
    if version >= 2 {
        {
            // reading m.IsolationLevel: This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records
            m.IsolationLevel = int8(buff[offset])
            offset++
        }
    }
    {
        // reading m.Topics: Each topic in the request.
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
            topics := make([]ListOffsetsRequestListOffsetsTopic, l0)
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
                    // reading topics[i0].Partitions: Each partition in the request.
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
                        partitions := make([]ListOffsetsRequestListOffsetsPartition, l2)
                        for i1 := 0; i1 < l2; i1++ {
                            // reading non tagged fields
                            {
                                // reading partitions[i1].PartitionIndex: The partition index.
                                partitions[i1].PartitionIndex = int32(binary.BigEndian.Uint32(buff[offset:]))
                                offset += 4
                            }
                            if version >= 4 {
                                {
                                    // reading partitions[i1].CurrentLeaderEpoch: The current leader epoch.
                                    partitions[i1].CurrentLeaderEpoch = int32(binary.BigEndian.Uint32(buff[offset:]))
                                    offset += 4
                                }
                            }
                            {
                                // reading partitions[i1].Timestamp: The current timestamp.
                                partitions[i1].Timestamp = int64(binary.BigEndian.Uint64(buff[offset:]))
                                offset += 8
                            }
                            if version <= 0 {
                                {
                                    // reading partitions[i1].MaxNumOffsets: The maximum number of offsets to report.
                                    partitions[i1].MaxNumOffsets = int32(binary.BigEndian.Uint32(buff[offset:]))
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

func (m *ListOffsetsRequest) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.ReplicaId: The broker ID of the requester, or -1 if this request is being made by a normal consumer.
    buff = binary.BigEndian.AppendUint32(buff, uint32(m.ReplicaId))
    if version >= 2 {
        // writing m.IsolationLevel: This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records
        buff = append(buff, byte(m.IsolationLevel))
    }
    // writing m.Topics: Each topic in the request.
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
        // writing topics.Partitions: Each partition in the request.
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
            if version >= 4 {
                // writing partitions.CurrentLeaderEpoch: The current leader epoch.
                buff = binary.BigEndian.AppendUint32(buff, uint32(partitions.CurrentLeaderEpoch))
            }
            // writing partitions.Timestamp: The current timestamp.
            buff = binary.BigEndian.AppendUint64(buff, uint64(partitions.Timestamp))
            if version <= 0 {
                // writing partitions.MaxNumOffsets: The maximum number of offsets to report.
                buff = binary.BigEndian.AppendUint32(buff, uint32(partitions.MaxNumOffsets))
            }
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
    if version >= 6 {
        numTaggedFields11 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields11))
    }
    return buff
}

func (m *ListOffsetsRequest) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.ReplicaId: The broker ID of the requester, or -1 if this request is being made by a normal consumer.
    size += 4
    if version >= 2 {
        // size for m.IsolationLevel: This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records
        size += 1
    }
    // size for m.Topics: Each topic in the request.
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
        // size for topics.Partitions: Each partition in the request.
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
            if version >= 4 {
                // size for partitions.CurrentLeaderEpoch: The current leader epoch.
                size += 4
            }
            // size for partitions.Timestamp: The current timestamp.
            size += 8
            if version <= 0 {
                // size for partitions.MaxNumOffsets: The maximum number of offsets to report.
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

func (m *ListOffsetsRequest) HeaderVersions(version int16) (int16, int16) {
    if version >= 6 {
        return 2, 1
    } else {
        return 1, 0
    }
}

func (m *ListOffsetsRequest) SupportedApiVersions() (int16, int16) {
    return 1, 1
}
