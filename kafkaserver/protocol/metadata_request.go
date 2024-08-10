// Package protocol - This is a generated file, please do not edit

package protocol

import "encoding/binary"
import "github.com/spirit-labs/tektite/common"
import "unsafe"

type MetadataRequestMetadataRequestTopic struct {
    // The topic id.
    TopicId []byte
    // The topic name.
    Name *string
}

type MetadataRequest struct {
    // The topics to fetch metadata for.
    Topics []MetadataRequestMetadataRequestTopic
    // If this is true, the broker may auto-create topics that we requested which do not already exist, if it is configured to do so.
    AllowAutoTopicCreation bool
    // Whether to include cluster authorized operations.
    IncludeClusterAuthorizedOperations bool
    // Whether to include topic authorized operations.
    IncludeTopicAuthorizedOperations bool
}

func (m *MetadataRequest) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.Topics: The topics to fetch metadata for.
        var l0 int
        if version >= 9 {
            // flexible and nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l0 = int(u - 1)
        } else {
            // non flexible and nullable
            l0 = int(int32(binary.BigEndian.Uint32(buff[offset:])))
            offset += 4
        }
        if l0 >= 0 {
            // length will be -1 if field is null
            topics := make([]MetadataRequestMetadataRequestTopic, l0)
            for i0 := 0; i0 < l0; i0++ {
                // reading non tagged fields
                if version >= 10 {
                    {
                        // reading topics[i0].TopicId: The topic id.
                        topics[i0].TopicId = common.ByteSliceCopy(buff[offset: offset + 16])
                        offset += 16
                    }
                }
                {
                    // reading topics[i0].Name: The topic name.
                    if version >= 9 {
                        if version >= 10 {
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
                        } else {
                            // flexible and not nullable
                            u, n := binary.Uvarint(buff[offset:])
                            offset += n
                            l1 := int(u - 1)
                            s := string(buff[offset: offset + l1])
                            topics[i0].Name = &s
                            offset += l1
                        }
                    } else {
                        if version >= 10 {
                            // non flexible and nullable
                            var l1 int
                            l1 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
                            offset += 2
                            if l1 > 0 {
                                s := string(buff[offset: offset + l1])
                                topics[i0].Name = &s
                                offset += l1
                            } else {
                                topics[i0].Name = nil
                            }
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
        m.Topics = topics
        }
    }
    if version >= 4 {
        {
            // reading m.AllowAutoTopicCreation: If this is true, the broker may auto-create topics that we requested which do not already exist, if it is configured to do so.
            m.AllowAutoTopicCreation = buff[offset] == 1
            offset++
        }
    }
    if version >= 8 && version <= 10 {
        {
            // reading m.IncludeClusterAuthorizedOperations: Whether to include cluster authorized operations.
            m.IncludeClusterAuthorizedOperations = buff[offset] == 1
            offset++
        }
    }
    if version >= 8 {
        {
            // reading m.IncludeTopicAuthorizedOperations: Whether to include topic authorized operations.
            m.IncludeTopicAuthorizedOperations = buff[offset] == 1
            offset++
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

func (m *MetadataRequest) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.Topics: The topics to fetch metadata for.
    if version >= 9 {
        // flexible and nullable
        if m.Topics == nil {
            // null
            buff = append(buff, 0)
        } else {
            // not null
            buff = binary.AppendUvarint(buff, uint64(len(m.Topics) + 1))
        }
    } else {
        // non flexible and nullable
        if m.Topics == nil {
            // null
            buff = binary.BigEndian.AppendUint32(buff, 4294967295)
        } else {
            // not null
            buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.Topics)))
        }
    }
    for _, topics := range m.Topics {
        // writing non tagged fields
        if version >= 10 {
            // writing topics.TopicId: The topic id.
            if topics.TopicId != nil {
                buff = append(buff, topics.TopicId...)
            }
        }
        // writing topics.Name: The topic name.
        if version >= 9 {
            if version >= 10 {
                // flexible and nullable
                if topics.Name == nil {
                    // null
                    buff = append(buff, 0)
                } else {
                    // not null
                    buff = binary.AppendUvarint(buff, uint64(len(*topics.Name) + 1))
                }
            } else {
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(*topics.Name) + 1))
            }
        } else {
            if version >= 10 {
                // non flexible and nullable
                if topics.Name == nil {
                    // null
                    buff = binary.BigEndian.AppendUint16(buff, 65535)
                } else {
                    // not null
                    buff = binary.BigEndian.AppendUint16(buff, uint16(len(*topics.Name)))
                }
            } else {
                // non flexible and non nullable
                buff = binary.BigEndian.AppendUint16(buff, uint16(len(*topics.Name)))
            }
        }
        if topics.Name != nil {
            buff = append(buff, *topics.Name...)
        }
        if version >= 9 {
            numTaggedFields3 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields3))
        }
    }
    if version >= 4 {
        // writing m.AllowAutoTopicCreation: If this is true, the broker may auto-create topics that we requested which do not already exist, if it is configured to do so.
        if m.AllowAutoTopicCreation {
            buff = append(buff, 1)
        } else {
            buff = append(buff, 0)
        }
    }
    if version >= 8 && version <= 10 {
        // writing m.IncludeClusterAuthorizedOperations: Whether to include cluster authorized operations.
        if m.IncludeClusterAuthorizedOperations {
            buff = append(buff, 1)
        } else {
            buff = append(buff, 0)
        }
    }
    if version >= 8 {
        // writing m.IncludeTopicAuthorizedOperations: Whether to include topic authorized operations.
        if m.IncludeTopicAuthorizedOperations {
            buff = append(buff, 1)
        } else {
            buff = append(buff, 0)
        }
    }
    if version >= 9 {
        numTaggedFields7 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields7))
    }
    return buff
}

func (m *MetadataRequest) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.Topics: The topics to fetch metadata for.
    if version >= 9 {
        // flexible and nullable
        if m.Topics == nil {
            // null
            size += 1
        } else {
            // not null
            size += sizeofUvarint(len(m.Topics) + 1)
        }
    } else {
        // non flexible and nullable
        size += 4
    }
    for _, topics := range m.Topics {
        size += 0 * int(unsafe.Sizeof(topics)) // hack to make sure loop variable is always used
        // calculating size for non tagged fields
        numTaggedFields1:= 0
        numTaggedFields1 += 0
        if version >= 10 {
            // size for topics.TopicId: The topic id.
            size += 16
        }
        // size for topics.Name: The topic name.
        if version >= 9 {
            if version >= 10 {
                // flexible and nullable
                if topics.Name == nil {
                    // null
                    size += 1
                } else {
                    // not null
                    size += sizeofUvarint(len(*topics.Name) + 1)
                }
            } else {
                // flexible and not nullable
                size += sizeofUvarint(len(*topics.Name) + 1)
            }
        } else {
            if version >= 10 {
                // non flexible and nullable
                size += 2
            } else {
                // non flexible and non nullable
                size += 2
            }
        }
        if topics.Name != nil {
            size += len(*topics.Name)
        }
        numTaggedFields2:= 0
        numTaggedFields2 += 0
        if version >= 9 {
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields2)
        }
    }
    if version >= 4 {
        // size for m.AllowAutoTopicCreation: If this is true, the broker may auto-create topics that we requested which do not already exist, if it is configured to do so.
        size += 1
    }
    if version >= 8 && version <= 10 {
        // size for m.IncludeClusterAuthorizedOperations: Whether to include cluster authorized operations.
        size += 1
    }
    if version >= 8 {
        // size for m.IncludeTopicAuthorizedOperations: Whether to include topic authorized operations.
        size += 1
    }
    numTaggedFields3:= 0
    numTaggedFields3 += 0
    if version >= 9 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields3)
    }
    return size, tagSizes
}

func (m *MetadataRequest) HeaderVersions(version int16) (int16, int16) {
    if version >= 9 {
        return 2, 1
    } else {
        return 1, 0
    }
}

func (m *MetadataRequest) SupportedApiVersions() (int16, int16) {
    return 1, 4
}
