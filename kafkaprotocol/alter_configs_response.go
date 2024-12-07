// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"
import "unsafe"

type AlterConfigsResponseAlterConfigsResourceResponse struct {
    // The resource error code.
    ErrorCode int16
    // The resource error message, or null if there was no error.
    ErrorMessage *string
    // The resource type.
    ResourceType int8
    // The resource name.
    ResourceName *string
}

type AlterConfigsResponse struct {
    // Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ThrottleTimeMs int32
    // The responses for each resource.
    Responses []AlterConfigsResponseAlterConfigsResourceResponse
}

func (m *AlterConfigsResponse) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.ThrottleTimeMs: Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        m.ThrottleTimeMs = int32(binary.BigEndian.Uint32(buff[offset:]))
        offset += 4
    }
    {
        // reading m.Responses: The responses for each resource.
        var l0 int
        if version >= 2 {
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
            responses := make([]AlterConfigsResponseAlterConfigsResourceResponse, l0)
            for i0 := 0; i0 < l0; i0++ {
                // reading non tagged fields
                {
                    // reading responses[i0].ErrorCode: The resource error code.
                    responses[i0].ErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
                    offset += 2
                }
                {
                    // reading responses[i0].ErrorMessage: The resource error message, or null if there was no error.
                    if version >= 2 {
                        // flexible and nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l1 := int(u - 1)
                        if l1 > 0 {
                            s := string(buff[offset: offset + l1])
                            responses[i0].ErrorMessage = &s
                            offset += l1
                        } else {
                            responses[i0].ErrorMessage = nil
                        }
                    } else {
                        // non flexible and nullable
                        var l1 int
                        l1 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
                        offset += 2
                        if l1 > 0 {
                            s := string(buff[offset: offset + l1])
                            responses[i0].ErrorMessage = &s
                            offset += l1
                        } else {
                            responses[i0].ErrorMessage = nil
                        }
                    }
                }
                {
                    // reading responses[i0].ResourceType: The resource type.
                    responses[i0].ResourceType = int8(buff[offset])
                    offset++
                }
                {
                    // reading responses[i0].ResourceName: The resource name.
                    if version >= 2 {
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l2 := int(u - 1)
                        s := string(buff[offset: offset + l2])
                        responses[i0].ResourceName = &s
                        offset += l2
                    } else {
                        // non flexible and non nullable
                        var l2 int
                        l2 = int(binary.BigEndian.Uint16(buff[offset:]))
                        offset += 2
                        s := string(buff[offset: offset + l2])
                        responses[i0].ResourceName = &s
                        offset += l2
                    }
                }
                if version >= 2 {
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
    if version >= 2 {
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

func (m *AlterConfigsResponse) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.ThrottleTimeMs: Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    buff = binary.BigEndian.AppendUint32(buff, uint32(m.ThrottleTimeMs))
    // writing m.Responses: The responses for each resource.
    if version >= 2 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.Responses) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.Responses)))
    }
    for _, responses := range m.Responses {
        // writing non tagged fields
        // writing responses.ErrorCode: The resource error code.
        buff = binary.BigEndian.AppendUint16(buff, uint16(responses.ErrorCode))
        // writing responses.ErrorMessage: The resource error message, or null if there was no error.
        if version >= 2 {
            // flexible and nullable
            if responses.ErrorMessage == nil {
                // null
                buff = append(buff, 0)
            } else {
                // not null
                buff = binary.AppendUvarint(buff, uint64(len(*responses.ErrorMessage) + 1))
            }
        } else {
            // non flexible and nullable
            if responses.ErrorMessage == nil {
                // null
                buff = binary.BigEndian.AppendUint16(buff, 65535)
            } else {
                // not null
                buff = binary.BigEndian.AppendUint16(buff, uint16(len(*responses.ErrorMessage)))
            }
        }
        if responses.ErrorMessage != nil {
            buff = append(buff, *responses.ErrorMessage...)
        }
        // writing responses.ResourceType: The resource type.
        buff = append(buff, byte(responses.ResourceType))
        // writing responses.ResourceName: The resource name.
        if version >= 2 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*responses.ResourceName) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*responses.ResourceName)))
        }
        if responses.ResourceName != nil {
            buff = append(buff, *responses.ResourceName...)
        }
        if version >= 2 {
            numTaggedFields6 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields6))
        }
    }
    if version >= 2 {
        numTaggedFields7 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields7))
    }
    return buff
}

func (m *AlterConfigsResponse) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.ThrottleTimeMs: Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    size += 4
    // size for m.Responses: The responses for each resource.
    if version >= 2 {
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
        // size for responses.ErrorCode: The resource error code.
        size += 2
        // size for responses.ErrorMessage: The resource error message, or null if there was no error.
        if version >= 2 {
            // flexible and nullable
            if responses.ErrorMessage == nil {
                // null
                size += 1
            } else {
                // not null
                size += sizeofUvarint(len(*responses.ErrorMessage) + 1)
            }
        } else {
            // non flexible and nullable
            size += 2
        }
        if responses.ErrorMessage != nil {
            size += len(*responses.ErrorMessage)
        }
        // size for responses.ResourceType: The resource type.
        size += 1
        // size for responses.ResourceName: The resource name.
        if version >= 2 {
            // flexible and not nullable
            size += sizeofUvarint(len(*responses.ResourceName) + 1)
        } else {
            // non flexible and non nullable
            size += 2
        }
        if responses.ResourceName != nil {
            size += len(*responses.ResourceName)
        }
        numTaggedFields2:= 0
        numTaggedFields2 += 0
        if version >= 2 {
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields2)
        }
    }
    numTaggedFields3:= 0
    numTaggedFields3 += 0
    if version >= 2 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields3)
    }
    return size, tagSizes
}


