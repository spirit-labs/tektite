// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"
import "github.com/spirit-labs/tektite/common"
import "unsafe"

type DeleteTopicsResponseDeletableTopicResult struct {
    // The topic name
    Name *string
    // the unique topic ID
    TopicId []byte
    // The deletion error, or 0 if the deletion succeeded.
    ErrorCode int16
    // The error message, or null if there was no error.
    ErrorMessage *string
}

type DeleteTopicsResponse struct {
    // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ThrottleTimeMs int32
    // The results for each topic we tried to delete.
    Responses []DeleteTopicsResponseDeletableTopicResult
}

func (m *DeleteTopicsResponse) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    if version >= 1 {
        {
            // reading m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
            m.ThrottleTimeMs = int32(binary.BigEndian.Uint32(buff[offset:]))
            offset += 4
        }
    }
    {
        // reading m.Responses: The results for each topic we tried to delete.
        var l0 int
        if version >= 4 {
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
            responses := make([]DeleteTopicsResponseDeletableTopicResult, l0)
            for i0 := 0; i0 < l0; i0++ {
                // reading non tagged fields
                {
                    // reading responses[i0].Name: The topic name
                    if version >= 4 {
                        if version >= 6 {
                            // flexible and nullable
                            u, n := binary.Uvarint(buff[offset:])
                            offset += n
                            l1 := int(u - 1)
                            if l1 > 0 {
                                s := string(buff[offset: offset + l1])
                                responses[i0].Name = &s
                                offset += l1
                            } else {
                                responses[i0].Name = nil
                            }
                        } else {
                            // flexible and not nullable
                            u, n := binary.Uvarint(buff[offset:])
                            offset += n
                            l1 := int(u - 1)
                            s := string(buff[offset: offset + l1])
                            responses[i0].Name = &s
                            offset += l1
                        }
                    } else {
                        if version >= 6 {
                            // non flexible and nullable
                            var l1 int
                            l1 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
                            offset += 2
                            if l1 > 0 {
                                s := string(buff[offset: offset + l1])
                                responses[i0].Name = &s
                                offset += l1
                            } else {
                                responses[i0].Name = nil
                            }
                        } else {
                            // non flexible and non nullable
                            var l1 int
                            l1 = int(binary.BigEndian.Uint16(buff[offset:]))
                            offset += 2
                            s := string(buff[offset: offset + l1])
                            responses[i0].Name = &s
                            offset += l1
                        }
                    }
                }
                if version >= 6 {
                    {
                        // reading responses[i0].TopicId: the unique topic ID
                        responses[i0].TopicId = common.ByteSliceCopy(buff[offset: offset + 16])
                        offset += 16
                    }
                }
                {
                    // reading responses[i0].ErrorCode: The deletion error, or 0 if the deletion succeeded.
                    responses[i0].ErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
                    offset += 2
                }
                if version >= 5 {
                    {
                        // reading responses[i0].ErrorMessage: The error message, or null if there was no error.
                        // flexible and nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l2 := int(u - 1)
                        if l2 > 0 {
                            s := string(buff[offset: offset + l2])
                            responses[i0].ErrorMessage = &s
                            offset += l2
                        } else {
                            responses[i0].ErrorMessage = nil
                        }
                    }
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
            }
        m.Responses = responses
        }
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

func (m *DeleteTopicsResponse) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    if version >= 1 {
        // writing m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        buff = binary.BigEndian.AppendUint32(buff, uint32(m.ThrottleTimeMs))
    }
    // writing m.Responses: The results for each topic we tried to delete.
    if version >= 4 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.Responses) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.Responses)))
    }
    for _, responses := range m.Responses {
        // writing non tagged fields
        // writing responses.Name: The topic name
        if version >= 4 {
            if version >= 6 {
                // flexible and nullable
                if responses.Name == nil {
                    // null
                    buff = append(buff, 0)
                } else {
                    // not null
                    buff = binary.AppendUvarint(buff, uint64(len(*responses.Name) + 1))
                }
            } else {
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(*responses.Name) + 1))
            }
        } else {
            if version >= 6 {
                // non flexible and nullable
                if responses.Name == nil {
                    // null
                    buff = binary.BigEndian.AppendUint16(buff, 65535)
                } else {
                    // not null
                    buff = binary.BigEndian.AppendUint16(buff, uint16(len(*responses.Name)))
                }
            } else {
                // non flexible and non nullable
                buff = binary.BigEndian.AppendUint16(buff, uint16(len(*responses.Name)))
            }
        }
        if responses.Name != nil {
            buff = append(buff, *responses.Name...)
        }
        if version >= 6 {
            // writing responses.TopicId: the unique topic ID
            if responses.TopicId != nil {
                buff = append(buff, responses.TopicId...)
            }
        }
        // writing responses.ErrorCode: The deletion error, or 0 if the deletion succeeded.
        buff = binary.BigEndian.AppendUint16(buff, uint16(responses.ErrorCode))
        if version >= 5 {
            // writing responses.ErrorMessage: The error message, or null if there was no error.
            // flexible and nullable
            if responses.ErrorMessage == nil {
                // null
                buff = append(buff, 0)
            } else {
                // not null
                buff = binary.AppendUvarint(buff, uint64(len(*responses.ErrorMessage) + 1))
            }
            if responses.ErrorMessage != nil {
                buff = append(buff, *responses.ErrorMessage...)
            }
        }
        if version >= 4 {
            numTaggedFields6 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields6))
        }
    }
    if version >= 4 {
        numTaggedFields7 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields7))
    }
    return buff
}

func (m *DeleteTopicsResponse) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    if version >= 1 {
        // size for m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        size += 4
    }
    // size for m.Responses: The results for each topic we tried to delete.
    if version >= 4 {
        // flexible and not nullable
        size += sizeofUvarint(len(m.Responses) + 1)
    } else {
        // non flexible and non nullable
        size += 4
    }
    for _, responses := range m.Responses {
        size += 0 * int(unsafe.Sizeof(responses)) // hack to make sure loop variable is always used
        // calculating size for non tagged fields
        numTaggedFields1:= 0
        numTaggedFields1 += 0
        // size for responses.Name: The topic name
        if version >= 4 {
            if version >= 6 {
                // flexible and nullable
                if responses.Name == nil {
                    // null
                    size += 1
                } else {
                    // not null
                    size += sizeofUvarint(len(*responses.Name) + 1)
                }
            } else {
                // flexible and not nullable
                size += sizeofUvarint(len(*responses.Name) + 1)
            }
        } else {
            if version >= 6 {
                // non flexible and nullable
                size += 2
            } else {
                // non flexible and non nullable
                size += 2
            }
        }
        if responses.Name != nil {
            size += len(*responses.Name)
        }
        if version >= 6 {
            // size for responses.TopicId: the unique topic ID
            size += 16
        }
        // size for responses.ErrorCode: The deletion error, or 0 if the deletion succeeded.
        size += 2
        if version >= 5 {
            // size for responses.ErrorMessage: The error message, or null if there was no error.
            // flexible and nullable
            if responses.ErrorMessage == nil {
                // null
                size += 1
            } else {
                // not null
                size += sizeofUvarint(len(*responses.ErrorMessage) + 1)
            }
            if responses.ErrorMessage != nil {
                size += len(*responses.ErrorMessage)
            }
        }
        numTaggedFields2:= 0
        numTaggedFields2 += 0
        if version >= 4 {
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields2)
        }
    }
    numTaggedFields3:= 0
    numTaggedFields3 += 0
    if version >= 4 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields3)
    }
    return size, tagSizes
}


