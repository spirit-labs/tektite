// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"
import "unsafe"

type OffsetDeleteRequestOffsetDeleteRequestPartition struct {
    // The partition index.
    PartitionIndex int32
}

type OffsetDeleteRequestOffsetDeleteRequestTopic struct {
    // The topic name.
    Name *string
    // Each partition to delete offsets for.
    Partitions []OffsetDeleteRequestOffsetDeleteRequestPartition
}

type OffsetDeleteRequest struct {
    // The unique group identifier.
    GroupId *string
    // The topics to delete offsets for
    Topics []OffsetDeleteRequestOffsetDeleteRequestTopic
}

func (m *OffsetDeleteRequest) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.GroupId: The unique group identifier.
        // non flexible and non nullable
        var l0 int
        l0 = int(binary.BigEndian.Uint16(buff[offset:]))
        offset += 2
        s := string(buff[offset: offset + l0])
        m.GroupId = &s
        offset += l0
    }
    {
        // reading m.Topics: The topics to delete offsets for
        var l1 int
        // non flexible and non nullable
        l1 = int(binary.BigEndian.Uint32(buff[offset:]))
        offset += 4
        if l1 >= 0 {
            // length will be -1 if field is null
            topics := make([]OffsetDeleteRequestOffsetDeleteRequestTopic, l1)
            for i0 := 0; i0 < l1; i0++ {
                // reading non tagged fields
                {
                    // reading topics[i0].Name: The topic name.
                    // non flexible and non nullable
                    var l2 int
                    l2 = int(binary.BigEndian.Uint16(buff[offset:]))
                    offset += 2
                    s := string(buff[offset: offset + l2])
                    topics[i0].Name = &s
                    offset += l2
                }
                {
                    // reading topics[i0].Partitions: Each partition to delete offsets for.
                    var l3 int
                    // non flexible and non nullable
                    l3 = int(binary.BigEndian.Uint32(buff[offset:]))
                    offset += 4
                    if l3 >= 0 {
                        // length will be -1 if field is null
                        partitions := make([]OffsetDeleteRequestOffsetDeleteRequestPartition, l3)
                        for i1 := 0; i1 < l3; i1++ {
                            // reading non tagged fields
                            {
                                // reading partitions[i1].PartitionIndex: The partition index.
                                partitions[i1].PartitionIndex = int32(binary.BigEndian.Uint32(buff[offset:]))
                                offset += 4
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

func (m *OffsetDeleteRequest) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.GroupId: The unique group identifier.
    // non flexible and non nullable
    buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.GroupId)))
    if m.GroupId != nil {
        buff = append(buff, *m.GroupId...)
    }
    // writing m.Topics: The topics to delete offsets for
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
        // writing topics.Partitions: Each partition to delete offsets for.
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint32(buff, uint32(len(topics.Partitions)))
        for _, partitions := range topics.Partitions {
            // writing non tagged fields
            // writing partitions.PartitionIndex: The partition index.
            buff = binary.BigEndian.AppendUint32(buff, uint32(partitions.PartitionIndex))
        }
    }
    return buff
}

func (m *OffsetDeleteRequest) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    // size for m.GroupId: The unique group identifier.
    // non flexible and non nullable
    size += 2
    if m.GroupId != nil {
        size += len(*m.GroupId)
    }
    // size for m.Topics: The topics to delete offsets for
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
        // size for topics.Partitions: Each partition to delete offsets for.
        // non flexible and non nullable
        size += 4
        for _, partitions := range topics.Partitions {
            size += 0 * int(unsafe.Sizeof(partitions)) // hack to make sure loop variable is always used
            // calculating size for non tagged fields
            // size for partitions.PartitionIndex: The partition index.
            size += 4
        }
    }
    return size, tagSizes
}

func (m *OffsetDeleteRequest) HeaderVersions(version int16) (int16, int16) {
    return 1, 0
}

func (m *OffsetDeleteRequest) SupportedApiVersions() (int16, int16) {
    return 0, 0
}
