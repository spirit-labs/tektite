// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"
import "github.com/spirit-labs/tektite/common"
import "unsafe"

type DeleteTopicsRequestDeleteTopicState struct {
    // The topic name
    Name *string
    // The unique topic ID
    TopicId []byte
}

type DeleteTopicsRequest struct {
    // The name or topic ID of the topic
    Topics []DeleteTopicsRequestDeleteTopicState
    // The names of the topics to delete
    TopicNames []*string
    // The length of time in milliseconds to wait for the deletions to complete.
    TimeoutMs int32
}

func (m *DeleteTopicsRequest) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    if version >= 6 {
        {
            // reading m.Topics: The name or topic ID of the topic
            var l0 int
            // flexible and not nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l0 = int(u - 1)
            if l0 >= 0 {
                // length will be -1 if field is null
                topics := make([]DeleteTopicsRequestDeleteTopicState, l0)
                for i0 := 0; i0 < l0; i0++ {
                    // reading non tagged fields
                    {
                        // reading topics[i0].Name: The topic name
                        // flexible and nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l1 := int(u - 1)
                        if l1 > 0 {
                            s := string(buff[offset: offset + l1])
                            topics[i0].Name = &s
                            offset += l1
                        } else {
                            topics[i0].Name = nil
                        }
                    }
                    {
                        // reading topics[i0].TopicId: The unique topic ID
                        topics[i0].TopicId = common.ByteSliceCopy(buff[offset: offset + 16])
                        offset += 16
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
            m.Topics = topics
            }
        }
    }
    if version <= 5 {
        {
            // reading m.TopicNames: The names of the topics to delete
            var l2 int
            if version >= 4 {
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
                topicNames := make([]*string, l2)
                for i1 := 0; i1 < l2; i1++ {
                    if version >= 4 {
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l3 := int(u - 1)
                        s := string(buff[offset: offset + l3])
                        topicNames[i1] = &s
                        offset += l3
                    } else {
                        // non flexible and non nullable
                        var l3 int
                        l3 = int(binary.BigEndian.Uint16(buff[offset:]))
                        offset += 2
                        s := string(buff[offset: offset + l3])
                        topicNames[i1] = &s
                        offset += l3
                    }
                }
                m.TopicNames = topicNames
            }
        }
    }
    {
        // reading m.TimeoutMs: The length of time in milliseconds to wait for the deletions to complete.
        m.TimeoutMs = int32(binary.BigEndian.Uint32(buff[offset:]))
        offset += 4
    }
    if version >= 4 {
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

func (m *DeleteTopicsRequest) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    if version >= 6 {
        // writing m.Topics: The name or topic ID of the topic
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.Topics) + 1))
        for _, topics := range m.Topics {
            // writing non tagged fields
            // writing topics.Name: The topic name
            // flexible and nullable
            if topics.Name == nil {
                // null
                buff = append(buff, 0)
            } else {
                // not null
                buff = binary.AppendUvarint(buff, uint64(len(*topics.Name) + 1))
            }
            if topics.Name != nil {
                buff = append(buff, *topics.Name...)
            }
            // writing topics.TopicId: The unique topic ID
            if topics.TopicId != nil {
                buff = append(buff, topics.TopicId...)
            }
            numTaggedFields3 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields3))
        }
    }
    if version <= 5 {
        // writing m.TopicNames: The names of the topics to delete
        if version >= 4 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(m.TopicNames) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.TopicNames)))
        }
        for _, topicNames := range m.TopicNames {
            if version >= 4 {
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(*topicNames) + 1))
            } else {
                // non flexible and non nullable
                buff = binary.BigEndian.AppendUint16(buff, uint16(len(*topicNames)))
            }
            if topicNames != nil {
                buff = append(buff, *topicNames...)
            }
        }
    }
    // writing m.TimeoutMs: The length of time in milliseconds to wait for the deletions to complete.
    buff = binary.BigEndian.AppendUint32(buff, uint32(m.TimeoutMs))
    if version >= 4 {
        numTaggedFields6 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields6))
    }
    return buff
}

func (m *DeleteTopicsRequest) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    if version >= 6 {
        // size for m.Topics: The name or topic ID of the topic
        // flexible and not nullable
        size += sizeofUvarint(len(m.Topics) + 1)
        for _, topics := range m.Topics {
            size += 0 * int(unsafe.Sizeof(topics)) // hack to make sure loop variable is always used
            // calculating size for non tagged fields
            numTaggedFields1:= 0
            numTaggedFields1 += 0
            // size for topics.Name: The topic name
            // flexible and nullable
            if topics.Name == nil {
                // null
                size += 1
            } else {
                // not null
                size += sizeofUvarint(len(*topics.Name) + 1)
            }
            if topics.Name != nil {
                size += len(*topics.Name)
            }
            // size for topics.TopicId: The unique topic ID
            size += 16
            numTaggedFields2:= 0
            numTaggedFields2 += 0
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields2)
        }
    }
    if version <= 5 {
        // size for m.TopicNames: The names of the topics to delete
        if version >= 4 {
            // flexible and not nullable
            size += sizeofUvarint(len(m.TopicNames) + 1)
        } else {
            // non flexible and non nullable
            size += 4
        }
        for _, topicNames := range m.TopicNames {
            size += 0 * int(unsafe.Sizeof(topicNames)) // hack to make sure loop variable is always used
            if version >= 4 {
                // flexible and not nullable
                size += sizeofUvarint(len(*topicNames) + 1)
            } else {
                // non flexible and non nullable
                size += 2
            }
            if topicNames != nil {
                size += len(*topicNames)
            }
        }
    }
    // size for m.TimeoutMs: The length of time in milliseconds to wait for the deletions to complete.
    size += 4
    numTaggedFields3:= 0
    numTaggedFields3 += 0
    if version >= 4 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields3)
    }
    return size, tagSizes
}

func (m *DeleteTopicsRequest) HeaderVersions(version int16) (int16, int16) {
    if version >= 4 {
        return 2, 1
    } else {
        return 1, 0
    }
}

func (m *DeleteTopicsRequest) SupportedApiVersions() (int16, int16) {
    return -1, -1
}
