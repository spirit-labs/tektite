// Package protocol - This is a generated file, please do not edit

package protocol

import "encoding/binary"
import "github.com/spirit-labs/tektite/common"
import "unsafe"

type ProduceRequestPartitionProduceData struct {
    // The partition index.
    Index int32
    // The record data to be produced.
    Records [][]byte
}

type ProduceRequestTopicProduceData struct {
    // The topic name.
    Name *string
    // Each partition to produce to.
    PartitionData []ProduceRequestPartitionProduceData
}

type ProduceRequest struct {
    // The transactional ID, or null if the producer is not transactional.
    TransactionalId *string
    // The number of acknowledgments the producer requires the leader to have received before considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR.
    Acks int16
    // The timeout to await a response in milliseconds.
    TimeoutMs int32
    // Each topic to produce to.
    TopicData []ProduceRequestTopicProduceData
}

func (m *ProduceRequest) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    if version >= 3 {
        {
            // reading m.TransactionalId: The transactional ID, or null if the producer is not transactional.
            if version >= 9 {
                // flexible and nullable
                u, n := binary.Uvarint(buff[offset:])
                offset += n
                l0 := int(u - 1)
                if l0 > 0 {
                    s := string(buff[offset: offset + l0])
                    m.TransactionalId = &s
                    offset += l0
                } else {
                    m.TransactionalId = nil
                }
            } else {
                // non flexible and nullable
                var l0 int
                l0 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
                offset += 2
                if l0 > 0 {
                    s := string(buff[offset: offset + l0])
                    m.TransactionalId = &s
                    offset += l0
                } else {
                    m.TransactionalId = nil
                }
            }
        }
    }
    {
        // reading m.Acks: The number of acknowledgments the producer requires the leader to have received before considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR.
        m.Acks = int16(binary.BigEndian.Uint16(buff[offset:]))
        offset += 2
    }
    {
        // reading m.TimeoutMs: The timeout to await a response in milliseconds.
        m.TimeoutMs = int32(binary.BigEndian.Uint32(buff[offset:]))
        offset += 4
    }
    {
        // reading m.TopicData: Each topic to produce to.
        var l1 int
        if version >= 9 {
            // flexible and not nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l1 = int(u - 1)
        } else {
            // non flexible and non nullable
            l1 = int(binary.BigEndian.Uint32(buff[offset:]))
            offset += 4
        }
        if l1 >= 0 {
            // length will be -1 if field is null
            topicData := make([]ProduceRequestTopicProduceData, l1)
            for i0 := 0; i0 < l1; i0++ {
                // reading non tagged fields
                {
                    // reading topicData[i0].Name: The topic name.
                    if version >= 9 {
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l2 := int(u - 1)
                        s := string(buff[offset: offset + l2])
                        topicData[i0].Name = &s
                        offset += l2
                    } else {
                        // non flexible and non nullable
                        var l2 int
                        l2 = int(binary.BigEndian.Uint16(buff[offset:]))
                        offset += 2
                        s := string(buff[offset: offset + l2])
                        topicData[i0].Name = &s
                        offset += l2
                    }
                }
                {
                    // reading topicData[i0].PartitionData: Each partition to produce to.
                    var l3 int
                    if version >= 9 {
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
                        partitionData := make([]ProduceRequestPartitionProduceData, l3)
                        for i1 := 0; i1 < l3; i1++ {
                            // reading non tagged fields
                            {
                                // reading partitionData[i1].Index: The partition index.
                                partitionData[i1].Index = int32(binary.BigEndian.Uint32(buff[offset:]))
                                offset += 4
                            }
                            {
                                // reading partitionData[i1].Records: The record data to be produced.
                                if version >= 9 {
                                    // flexible and nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l4 := int(u - 1)
                                    if l4 > 0 {
                                        partitionData[i1].Records = [][]byte{common.ByteSliceCopy(buff[offset: offset + l4])}
                                        offset += l4
                                    } else {
                                        partitionData[i1].Records = nil
                                    }
                                } else {
                                    // non flexible and nullable
                                    var l4 int
                                    l4 = int(int32(binary.BigEndian.Uint32(buff[offset:])))
                                    offset += 4
                                    if l4 > 0 {
                                        partitionData[i1].Records = [][]byte{common.ByteSliceCopy(buff[offset: offset + l4])}
                                        offset += l4
                                    } else {
                                        partitionData[i1].Records = nil
                                    }
                                }
                            }
                            if version >= 9 {
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
                    topicData[i0].PartitionData = partitionData
                    }
                }
                if version >= 9 {
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
        m.TopicData = topicData
        }
    }
    if version >= 9 {
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

func (m *ProduceRequest) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    if version >= 3 {
        // writing m.TransactionalId: The transactional ID, or null if the producer is not transactional.
        if version >= 9 {
            // flexible and nullable
            if m.TransactionalId == nil {
                // null
                buff = append(buff, 0)
            } else {
                // not null
                buff = binary.AppendUvarint(buff, uint64(len(*m.TransactionalId) + 1))
            }
        } else {
            // non flexible and nullable
            if m.TransactionalId == nil {
                // null
                buff = binary.BigEndian.AppendUint16(buff, 65535)
            } else {
                // not null
                buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.TransactionalId)))
            }
        }
        if m.TransactionalId != nil {
            buff = append(buff, *m.TransactionalId...)
        }
    }
    // writing m.Acks: The number of acknowledgments the producer requires the leader to have received before considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR.
    buff = binary.BigEndian.AppendUint16(buff, uint16(m.Acks))
    // writing m.TimeoutMs: The timeout to await a response in milliseconds.
    buff = binary.BigEndian.AppendUint32(buff, uint32(m.TimeoutMs))
    // writing m.TopicData: Each topic to produce to.
    if version >= 9 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.TopicData) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.TopicData)))
    }
    for _, topicData := range m.TopicData {
        // writing non tagged fields
        // writing topicData.Name: The topic name.
        if version >= 9 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*topicData.Name) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*topicData.Name)))
        }
        if topicData.Name != nil {
            buff = append(buff, *topicData.Name...)
        }
        // writing topicData.PartitionData: Each partition to produce to.
        if version >= 9 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(topicData.PartitionData) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint32(buff, uint32(len(topicData.PartitionData)))
        }
        for _, partitionData := range topicData.PartitionData {
            // writing non tagged fields
            // writing partitionData.Index: The partition index.
            buff = binary.BigEndian.AppendUint32(buff, uint32(partitionData.Index))
            // writing partitionData.Records: The record data to be produced.
            if version >= 9 {
                // flexible and nullable
                if partitionData.Records == nil {
                    // null
                    buff = append(buff, 0)
                } else {
                    // not null
                    recordsTotSize8 := 0
                    for _, rec := range partitionData.Records {
                        recordsTotSize8 += len(rec)
                    }
                    buff = binary.AppendUvarint(buff, uint64(recordsTotSize8 + 1))
                }
            } else {
                // non flexible and nullable
                if partitionData.Records == nil {
                    // null
                    buff = binary.BigEndian.AppendUint32(buff, 4294967295)
                } else {
                    // not null
                    recordsTotSize9 := 0
                    for _, rec := range partitionData.Records {
                        recordsTotSize9 += len(rec)
                    }
                    buff = binary.BigEndian.AppendUint32(buff, uint32(recordsTotSize9))
                }
            }
            for _, rec := range partitionData.Records {
                buff = append(buff, rec...)
            }
            if version >= 9 {
                numTaggedFields10 := 0
                // write number of tagged fields
                buff = binary.AppendUvarint(buff, uint64(numTaggedFields10))
            }
        }
        if version >= 9 {
            numTaggedFields11 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields11))
        }
    }
    if version >= 9 {
        numTaggedFields12 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields12))
    }
    return buff
}

func (m *ProduceRequest) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    if version >= 3 {
        // size for m.TransactionalId: The transactional ID, or null if the producer is not transactional.
        if version >= 9 {
            // flexible and nullable
            if m.TransactionalId == nil {
                // null
                size += 1
            } else {
                // not null
                size += sizeofUvarint(len(*m.TransactionalId) + 1)
            }
        } else {
            // non flexible and nullable
            size += 2
        }
        if m.TransactionalId != nil {
            size += len(*m.TransactionalId)
        }
    }
    // size for m.Acks: The number of acknowledgments the producer requires the leader to have received before considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR.
    size += 2
    // size for m.TimeoutMs: The timeout to await a response in milliseconds.
    size += 4
    // size for m.TopicData: Each topic to produce to.
    if version >= 9 {
        // flexible and not nullable
        size += sizeofUvarint(len(m.TopicData) + 1)
    } else {
        // non flexible and non nullable
        size += 4
    }
    for _, topicData := range m.TopicData {
        size += 0 * int(unsafe.Sizeof(topicData)) // hack to make sure loop variable is always used
        // calculating size for non tagged fields
        numTaggedFields1:= 0
        numTaggedFields1 += 0
        // size for topicData.Name: The topic name.
        if version >= 9 {
            // flexible and not nullable
            size += sizeofUvarint(len(*topicData.Name) + 1)
        } else {
            // non flexible and non nullable
            size += 2
        }
        if topicData.Name != nil {
            size += len(*topicData.Name)
        }
        // size for topicData.PartitionData: Each partition to produce to.
        if version >= 9 {
            // flexible and not nullable
            size += sizeofUvarint(len(topicData.PartitionData) + 1)
        } else {
            // non flexible and non nullable
            size += 4
        }
        for _, partitionData := range topicData.PartitionData {
            size += 0 * int(unsafe.Sizeof(partitionData)) // hack to make sure loop variable is always used
            // calculating size for non tagged fields
            numTaggedFields2:= 0
            numTaggedFields2 += 0
            // size for partitionData.Index: The partition index.
            size += 4
            // size for partitionData.Records: The record data to be produced.
            if version >= 9 {
                // flexible and nullable
                if partitionData.Records == nil {
                    // null
                    size += 1
                } else {
                    // not null
                    recordsTotSize3 := 0
                    for _, rec := range partitionData.Records {
                        recordsTotSize3 += len(rec)
                    }
                    size += sizeofUvarint(recordsTotSize3 + 1)
                }
            } else {
                // non flexible and nullable
                size += 4
            }
            for _, rec := range partitionData.Records {
                size += len(rec)
            }
            numTaggedFields4:= 0
            numTaggedFields4 += 0
            if version >= 9 {
                // writing size of num tagged fields field
                size += sizeofUvarint(numTaggedFields4)
            }
        }
        numTaggedFields5:= 0
        numTaggedFields5 += 0
        if version >= 9 {
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields5)
        }
    }
    numTaggedFields6:= 0
    numTaggedFields6 += 0
    if version >= 9 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields6)
    }
    return size, tagSizes
}

func (m *ProduceRequest) HeaderVersions(version int16) (int16, int16) {
    if version >= 9 {
        return 2, 1
    } else {
        return 1, 0
    }
}

func (m *ProduceRequest) SupportedApiVersions() (int16, int16) {
    return 3, 3
}
