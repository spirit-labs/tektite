// Package protocol - This is a generated file, please do not edit

package protocol

import "encoding/binary"
import "github.com/spirit-labs/tektite/common"

type SyncGroupResponse struct {
    // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ThrottleTimeMs int32
    // The error code, or 0 if there was no error.
    ErrorCode int16
    // The group protocol type.
    ProtocolType *string
    // The group protocol name.
    ProtocolName *string
    // The member assignment.
    Assignment []byte
}

func (m *SyncGroupResponse) Read(version int16, buff []byte) (int, error) {
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
        // reading m.ErrorCode: The error code, or 0 if there was no error.
        m.ErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
        offset += 2
    }
    if version >= 5 {
        {
            // reading m.ProtocolType: The group protocol type.
            // flexible and nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l0 := int(u - 1)
            if l0 > 0 {
                s := string(buff[offset: offset + l0])
                m.ProtocolType = &s
                offset += l0
            } else {
                m.ProtocolType = nil
            }
        }
        {
            // reading m.ProtocolName: The group protocol name.
            // flexible and nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l1 := int(u - 1)
            if l1 > 0 {
                s := string(buff[offset: offset + l1])
                m.ProtocolName = &s
                offset += l1
            } else {
                m.ProtocolName = nil
            }
        }
    }
    {
        // reading m.Assignment: The member assignment.
        if version >= 4 {
            // flexible and not nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l2 := int(u - 1)
            m.Assignment = common.CopyByteSlice(buff[offset: offset + l2])
            offset += l2
        } else {
            // non flexible and non nullable
            var l2 int
            l2 = int(binary.BigEndian.Uint32(buff[offset:]))
            offset += 4
            m.Assignment = common.CopyByteSlice(buff[offset: offset + l2])
            offset += l2
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

func (m *SyncGroupResponse) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    if version >= 1 {
        // writing m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        buff = binary.BigEndian.AppendUint32(buff, uint32(m.ThrottleTimeMs))
    }
    // writing m.ErrorCode: The error code, or 0 if there was no error.
    buff = binary.BigEndian.AppendUint16(buff, uint16(m.ErrorCode))
    if version >= 5 {
        // writing m.ProtocolType: The group protocol type.
        // flexible and nullable
        if m.ProtocolType == nil {
            // null
            buff = append(buff, 0)
        } else {
            // not null
            buff = binary.AppendUvarint(buff, uint64(len(*m.ProtocolType) + 1))
        }
        if m.ProtocolType != nil {
            buff = append(buff, *m.ProtocolType...)
        }
        // writing m.ProtocolName: The group protocol name.
        // flexible and nullable
        if m.ProtocolName == nil {
            // null
            buff = append(buff, 0)
        } else {
            // not null
            buff = binary.AppendUvarint(buff, uint64(len(*m.ProtocolName) + 1))
        }
        if m.ProtocolName != nil {
            buff = append(buff, *m.ProtocolName...)
        }
    }
    // writing m.Assignment: The member assignment.
    if version >= 4 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.Assignment) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.Assignment)))
    }
    if m.Assignment != nil {
        buff = append(buff, m.Assignment...)
    }
    if version >= 4 {
        numTaggedFields5 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields5))
    }
    return buff
}

func (m *SyncGroupResponse) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    if version >= 1 {
        // size for m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        size += 4
    }
    // size for m.ErrorCode: The error code, or 0 if there was no error.
    size += 2
    if version >= 5 {
        // size for m.ProtocolType: The group protocol type.
        // flexible and nullable
        if m.ProtocolType == nil {
            // null
            size += 1
        } else {
            // not null
            size += sizeofUvarint(len(*m.ProtocolType) + 1)
        }
        if m.ProtocolType != nil {
            size += len(*m.ProtocolType)
        }
        // size for m.ProtocolName: The group protocol name.
        // flexible and nullable
        if m.ProtocolName == nil {
            // null
            size += 1
        } else {
            // not null
            size += sizeofUvarint(len(*m.ProtocolName) + 1)
        }
        if m.ProtocolName != nil {
            size += len(*m.ProtocolName)
        }
    }
    // size for m.Assignment: The member assignment.
    if version >= 4 {
        // flexible and not nullable
        size += sizeofUvarint(len(m.Assignment) + 1)
    } else {
        // non flexible and non nullable
        size += 4
    }
    if m.Assignment != nil {
        size += len(m.Assignment)
    }
    numTaggedFields1:= 0
    numTaggedFields1 += 0
    if version >= 4 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields1)
    }
    return size, tagSizes
}

