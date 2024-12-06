// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"
import "fmt"
import "github.com/spirit-labs/tektite/common"
import "github.com/spirit-labs/tektite/debug"
import "unsafe"

type CreateTopicsResponseCreatableTopicConfigs struct {
    // The configuration name.
    Name *string
    // The configuration value.
    Value *string
    // True if the configuration is read-only.
    ReadOnly bool
    // The configuration source.
    ConfigSource int8
    // True if this configuration is sensitive.
    IsSensitive bool
}

type CreateTopicsResponseCreatableTopicResult struct {
    // The topic name.
    Name *string
    // The unique topic ID
    TopicId []byte
    // The error code, or 0 if there was no error.
    ErrorCode int16
    // The error message, or null if there was no error.
    ErrorMessage *string
    // Optional topic config error returned if configs are not returned in the response.
    TopicConfigErrorCode int16
    // Number of partitions of the topic.
    NumPartitions int32
    // Replication factor of the topic.
    ReplicationFactor int16
    // Configuration of the topic.
    Configs []CreateTopicsResponseCreatableTopicConfigs
}

type CreateTopicsResponse struct {
    // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ThrottleTimeMs int32
    // Results for each topic we tried to create.
    Topics []CreateTopicsResponseCreatableTopicResult
}

func (m *CreateTopicsResponse) Read(version int16, buff []byte) (int, error) {
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
        // reading m.Topics: Results for each topic we tried to create.
        var l0 int
        if version >= 5 {
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
            topics := make([]CreateTopicsResponseCreatableTopicResult, l0)
            for i0 := 0; i0 < l0; i0++ {
                // reading non tagged fields
                {
                    // reading topics[i0].Name: The topic name.
                    if version >= 5 {
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
                if version >= 7 {
                    {
                        // reading topics[i0].TopicId: The unique topic ID
                        topics[i0].TopicId = common.ByteSliceCopy(buff[offset: offset + 16])
                        offset += 16
                    }
                }
                {
                    // reading topics[i0].ErrorCode: The error code, or 0 if there was no error.
                    topics[i0].ErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
                    offset += 2
                }
                if version >= 1 {
                    {
                        // reading topics[i0].ErrorMessage: The error message, or null if there was no error.
                        if version >= 5 {
                            // flexible and nullable
                            u, n := binary.Uvarint(buff[offset:])
                            offset += n
                            l2 := int(u - 1)
                            if l2 > 0 {
                                s := string(buff[offset: offset + l2])
                                topics[i0].ErrorMessage = &s
                                offset += l2
                            } else {
                                topics[i0].ErrorMessage = nil
                            }
                        } else {
                            // non flexible and nullable
                            var l2 int
                            l2 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
                            offset += 2
                            if l2 > 0 {
                                s := string(buff[offset: offset + l2])
                                topics[i0].ErrorMessage = &s
                                offset += l2
                            } else {
                                topics[i0].ErrorMessage = nil
                            }
                        }
                    }
                }
                if version >= 5 {
                    {
                        // reading topics[i0].NumPartitions: Number of partitions of the topic.
                        topics[i0].NumPartitions = int32(binary.BigEndian.Uint32(buff[offset:]))
                        offset += 4
                    }
                    {
                        // reading topics[i0].ReplicationFactor: Replication factor of the topic.
                        topics[i0].ReplicationFactor = int16(binary.BigEndian.Uint16(buff[offset:]))
                        offset += 2
                    }
                    {
                        // reading topics[i0].Configs: Configuration of the topic.
                        var l3 int
                        // flexible and nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l3 = int(u - 1)
                        if l3 >= 0 {
                            // length will be -1 if field is null
                            configs := make([]CreateTopicsResponseCreatableTopicConfigs, l3)
                            for i1 := 0; i1 < l3; i1++ {
                                // reading non tagged fields
                                {
                                    // reading configs[i1].Name: The configuration name.
                                    // flexible and not nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l4 := int(u - 1)
                                    s := string(buff[offset: offset + l4])
                                    configs[i1].Name = &s
                                    offset += l4
                                }
                                {
                                    // reading configs[i1].Value: The configuration value.
                                    // flexible and nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l5 := int(u - 1)
                                    if l5 > 0 {
                                        s := string(buff[offset: offset + l5])
                                        configs[i1].Value = &s
                                        offset += l5
                                    } else {
                                        configs[i1].Value = nil
                                    }
                                }
                                {
                                    // reading configs[i1].ReadOnly: True if the configuration is read-only.
                                    configs[i1].ReadOnly = buff[offset] == 1
                                    offset++
                                }
                                {
                                    // reading configs[i1].ConfigSource: The configuration source.
                                    configs[i1].ConfigSource = int8(buff[offset])
                                    offset++
                                }
                                {
                                    // reading configs[i1].IsSensitive: True if this configuration is sensitive.
                                    configs[i1].IsSensitive = buff[offset] == 1
                                    offset++
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
                        topics[i0].Configs = configs
                        }
                    }
                }
                if version >= 5 {
                    // reading tagged fields
                    nt, n := binary.Uvarint(buff[offset:])
                    offset += n
                    for i := 0; i < int(nt); i++ {
                        t, n := binary.Uvarint(buff[offset:])
                        offset += n
                        ts, n := binary.Uvarint(buff[offset:])
                        offset += n
                        switch t {
                            case 0:
                                {
                                    // reading topics[i0].TopicConfigErrorCode: Optional topic config error returned if configs are not returned in the response.
                                    topics[i0].TopicConfigErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
                                    offset += 2
                                }
                            default:
                                offset += int(ts)
                        }
                    }
                }
            }
        m.Topics = topics
        }
    }
    if version >= 5 {
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

func (m *CreateTopicsResponse) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    if version >= 2 {
        // writing m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        buff = binary.BigEndian.AppendUint32(buff, uint32(m.ThrottleTimeMs))
    }
    // writing m.Topics: Results for each topic we tried to create.
    if version >= 5 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.Topics) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.Topics)))
    }
    for _, topics := range m.Topics {
        // writing non tagged fields
        // writing topics.Name: The topic name.
        if version >= 5 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*topics.Name) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*topics.Name)))
        }
        if topics.Name != nil {
            buff = append(buff, *topics.Name...)
        }
        if version >= 7 {
            // writing topics.TopicId: The unique topic ID
            if topics.TopicId != nil {
                buff = append(buff, topics.TopicId...)
            }
        }
        // writing topics.ErrorCode: The error code, or 0 if there was no error.
        buff = binary.BigEndian.AppendUint16(buff, uint16(topics.ErrorCode))
        if version >= 1 {
            // writing topics.ErrorMessage: The error message, or null if there was no error.
            if version >= 5 {
                // flexible and nullable
                if topics.ErrorMessage == nil {
                    // null
                    buff = append(buff, 0)
                } else {
                    // not null
                    buff = binary.AppendUvarint(buff, uint64(len(*topics.ErrorMessage) + 1))
                }
            } else {
                // non flexible and nullable
                if topics.ErrorMessage == nil {
                    // null
                    buff = binary.BigEndian.AppendUint16(buff, 65535)
                } else {
                    // not null
                    buff = binary.BigEndian.AppendUint16(buff, uint16(len(*topics.ErrorMessage)))
                }
            }
            if topics.ErrorMessage != nil {
                buff = append(buff, *topics.ErrorMessage...)
            }
        }
        if version >= 5 {
            // writing topics.NumPartitions: Number of partitions of the topic.
            buff = binary.BigEndian.AppendUint32(buff, uint32(topics.NumPartitions))
            // writing topics.ReplicationFactor: Replication factor of the topic.
            buff = binary.BigEndian.AppendUint16(buff, uint16(topics.ReplicationFactor))
            // writing topics.Configs: Configuration of the topic.
            // flexible and nullable
            if topics.Configs == nil {
                // null
                buff = append(buff, 0)
            } else {
                // not null
                buff = binary.AppendUvarint(buff, uint64(len(topics.Configs) + 1))
            }
            for _, configs := range topics.Configs {
                // writing non tagged fields
                // writing configs.Name: The configuration name.
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(*configs.Name) + 1))
                if configs.Name != nil {
                    buff = append(buff, *configs.Name...)
                }
                // writing configs.Value: The configuration value.
                // flexible and nullable
                if configs.Value == nil {
                    // null
                    buff = append(buff, 0)
                } else {
                    // not null
                    buff = binary.AppendUvarint(buff, uint64(len(*configs.Value) + 1))
                }
                if configs.Value != nil {
                    buff = append(buff, *configs.Value...)
                }
                // writing configs.ReadOnly: True if the configuration is read-only.
                if configs.ReadOnly {
                    buff = append(buff, 1)
                } else {
                    buff = append(buff, 0)
                }
                // writing configs.ConfigSource: The configuration source.
                buff = append(buff, byte(configs.ConfigSource))
                // writing configs.IsSensitive: True if this configuration is sensitive.
                if configs.IsSensitive {
                    buff = append(buff, 1)
                } else {
                    buff = append(buff, 0)
                }
                numTaggedFields14 := 0
                // write number of tagged fields
                buff = binary.AppendUvarint(buff, uint64(numTaggedFields14))
            }
        }
        if version >= 5 {
            numTaggedFields15 := 0
            // writing tagged field increments
            // tagged field - topics.TopicConfigErrorCode: Optional topic config error returned if configs are not returned in the response.
            numTaggedFields15++
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields15))
            // writing tagged fields
            // tag header
            buff = binary.AppendUvarint(buff, uint64(0))
            buff = binary.AppendUvarint(buff, uint64(tagSizes[tagPos]))
            tagPos++
            var tagSizeStart16 int
            if debug.SanityChecks {
                tagSizeStart16 = len(buff)
            }
            // writing topics.TopicConfigErrorCode: Optional topic config error returned if configs are not returned in the response.
            buff = binary.BigEndian.AppendUint16(buff, uint16(topics.TopicConfigErrorCode))
            if debug.SanityChecks && len(buff) - tagSizeStart16 != tagSizes[tagPos - 1] {
                panic(fmt.Sprintf("incorrect calculated tag size for tag %d", 0))
            }
        }
    }
    if version >= 5 {
        numTaggedFields17 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields17))
    }
    return buff
}

func (m *CreateTopicsResponse) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    if version >= 2 {
        // size for m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        size += 4
    }
    // size for m.Topics: Results for each topic we tried to create.
    if version >= 5 {
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
        if version >= 5 {
            // flexible and not nullable
            size += sizeofUvarint(len(*topics.Name) + 1)
        } else {
            // non flexible and non nullable
            size += 2
        }
        if topics.Name != nil {
            size += len(*topics.Name)
        }
        if version >= 7 {
            // size for topics.TopicId: The unique topic ID
            size += 16
        }
        // size for topics.ErrorCode: The error code, or 0 if there was no error.
        size += 2
        if version >= 1 {
            // size for topics.ErrorMessage: The error message, or null if there was no error.
            if version >= 5 {
                // flexible and nullable
                if topics.ErrorMessage == nil {
                    // null
                    size += 1
                } else {
                    // not null
                    size += sizeofUvarint(len(*topics.ErrorMessage) + 1)
                }
            } else {
                // non flexible and nullable
                size += 2
            }
            if topics.ErrorMessage != nil {
                size += len(*topics.ErrorMessage)
            }
        }
        if version >= 5 {
            // size for topics.NumPartitions: Number of partitions of the topic.
            size += 4
            // size for topics.ReplicationFactor: Replication factor of the topic.
            size += 2
            // size for topics.Configs: Configuration of the topic.
            // flexible and nullable
            if topics.Configs == nil {
                // null
                size += 1
            } else {
                // not null
                size += sizeofUvarint(len(topics.Configs) + 1)
            }
            for _, configs := range topics.Configs {
                size += 0 * int(unsafe.Sizeof(configs)) // hack to make sure loop variable is always used
                // calculating size for non tagged fields
                numTaggedFields2:= 0
                numTaggedFields2 += 0
                // size for configs.Name: The configuration name.
                // flexible and not nullable
                size += sizeofUvarint(len(*configs.Name) + 1)
                if configs.Name != nil {
                    size += len(*configs.Name)
                }
                // size for configs.Value: The configuration value.
                // flexible and nullable
                if configs.Value == nil {
                    // null
                    size += 1
                } else {
                    // not null
                    size += sizeofUvarint(len(*configs.Value) + 1)
                }
                if configs.Value != nil {
                    size += len(*configs.Value)
                }
                // size for configs.ReadOnly: True if the configuration is read-only.
                size += 1
                // size for configs.ConfigSource: The configuration source.
                size += 1
                // size for configs.IsSensitive: True if this configuration is sensitive.
                size += 1
                numTaggedFields3:= 0
                numTaggedFields3 += 0
                // writing size of num tagged fields field
                size += sizeofUvarint(numTaggedFields3)
            }
        }
        numTaggedFields4:= 0
        numTaggedFields4 += 0
        taggedFieldStart := 0
        taggedFieldSize := 0
        if version >= 5 {
            // size for topics.TopicConfigErrorCode: Optional topic config error returned if configs are not returned in the response.
            numTaggedFields4++
            taggedFieldStart = size
            size += 2
            taggedFieldSize = size - taggedFieldStart
            tagSizes = append(tagSizes, taggedFieldSize)
            // size = <tag id contrib> + <field size>
            size += sizeofUvarint(0) + sizeofUvarint(taggedFieldSize)
        }
        if version >= 5 {
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields4)
        }
    }
    numTaggedFields5:= 0
    numTaggedFields5 += 0
    if version >= 5 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields5)
    }
    return size, tagSizes
}


