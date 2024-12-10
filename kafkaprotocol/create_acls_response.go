// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"
import "unsafe"

type CreateAclsResponseAclCreationResult struct {
    // The result error, or zero if there was no error.
    ErrorCode int16
    // The result message, or null if there was no error.
    ErrorMessage *string
}

type CreateAclsResponse struct {
    // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ThrottleTimeMs int32
    // The results for each ACL creation.
    Results []CreateAclsResponseAclCreationResult
}

func (m *CreateAclsResponse) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        m.ThrottleTimeMs = int32(binary.BigEndian.Uint32(buff[offset:]))
        offset += 4
    }
    {
        // reading m.Results: The results for each ACL creation.
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
            results := make([]CreateAclsResponseAclCreationResult, l0)
            for i0 := 0; i0 < l0; i0++ {
                // reading non tagged fields
                {
                    // reading results[i0].ErrorCode: The result error, or zero if there was no error.
                    results[i0].ErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
                    offset += 2
                }
                {
                    // reading results[i0].ErrorMessage: The result message, or null if there was no error.
                    if version >= 2 {
                        // flexible and nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l1 := int(u - 1)
                        if l1 > 0 {
                            s := string(buff[offset: offset + l1])
                            results[i0].ErrorMessage = &s
                            offset += l1
                        } else {
                            results[i0].ErrorMessage = nil
                        }
                    } else {
                        // non flexible and nullable
                        var l1 int
                        l1 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
                        offset += 2
                        if l1 > 0 {
                            s := string(buff[offset: offset + l1])
                            results[i0].ErrorMessage = &s
                            offset += l1
                        } else {
                            results[i0].ErrorMessage = nil
                        }
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
        m.Results = results
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

func (m *CreateAclsResponse) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    buff = binary.BigEndian.AppendUint32(buff, uint32(m.ThrottleTimeMs))
    // writing m.Results: The results for each ACL creation.
    if version >= 2 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.Results) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.Results)))
    }
    for _, results := range m.Results {
        // writing non tagged fields
        // writing results.ErrorCode: The result error, or zero if there was no error.
        buff = binary.BigEndian.AppendUint16(buff, uint16(results.ErrorCode))
        // writing results.ErrorMessage: The result message, or null if there was no error.
        if version >= 2 {
            // flexible and nullable
            if results.ErrorMessage == nil {
                // null
                buff = append(buff, 0)
            } else {
                // not null
                buff = binary.AppendUvarint(buff, uint64(len(*results.ErrorMessage) + 1))
            }
        } else {
            // non flexible and nullable
            if results.ErrorMessage == nil {
                // null
                buff = binary.BigEndian.AppendUint16(buff, 65535)
            } else {
                // not null
                buff = binary.BigEndian.AppendUint16(buff, uint16(len(*results.ErrorMessage)))
            }
        }
        if results.ErrorMessage != nil {
            buff = append(buff, *results.ErrorMessage...)
        }
        if version >= 2 {
            numTaggedFields4 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields4))
        }
    }
    if version >= 2 {
        numTaggedFields5 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields5))
    }
    return buff
}

func (m *CreateAclsResponse) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    size += 4
    // size for m.Results: The results for each ACL creation.
    if version >= 2 {
        // flexible and not nullable
        size += sizeofUvarint(len(m.Results) + 1)
    } else {
        // non flexible and non nullable
        size += 4
    }
    for _, results := range m.Results {
        size += 0 * int(unsafe.Sizeof(results)) // hack to make sure loop variable is always used
        // calculating size for non tagged fields
        numTaggedFields1:= 0
        numTaggedFields1 += 0
        // size for results.ErrorCode: The result error, or zero if there was no error.
        size += 2
        // size for results.ErrorMessage: The result message, or null if there was no error.
        if version >= 2 {
            // flexible and nullable
            if results.ErrorMessage == nil {
                // null
                size += 1
            } else {
                // not null
                size += sizeofUvarint(len(*results.ErrorMessage) + 1)
            }
        } else {
            // non flexible and nullable
            size += 2
        }
        if results.ErrorMessage != nil {
            size += len(*results.ErrorMessage)
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


