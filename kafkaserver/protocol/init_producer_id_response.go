// Package protocol - This is a generated file, please do not edit

package protocol

import "encoding/binary"

type InitProducerIdResponse struct {
    // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ThrottleTimeMs int32
    // The error code, or 0 if there was no error.
    ErrorCode int16
    // The current producer id.
    ProducerId int64
    // The current epoch associated with the producer id.
    ProducerEpoch int16
}

func (m *InitProducerIdResponse) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        m.ThrottleTimeMs = int32(binary.BigEndian.Uint32(buff[offset:]))
        offset += 4
    }
    {
        // reading m.ErrorCode: The error code, or 0 if there was no error.
        m.ErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
        offset += 2
    }
    {
        // reading m.ProducerId: The current producer id.
        m.ProducerId = int64(binary.BigEndian.Uint64(buff[offset:]))
        offset += 8
    }
    {
        // reading m.ProducerEpoch: The current epoch associated with the producer id.
        m.ProducerEpoch = int16(binary.BigEndian.Uint16(buff[offset:]))
        offset += 2
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

func (m *InitProducerIdResponse) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    buff = binary.BigEndian.AppendUint32(buff, uint32(m.ThrottleTimeMs))
    // writing m.ErrorCode: The error code, or 0 if there was no error.
    buff = binary.BigEndian.AppendUint16(buff, uint16(m.ErrorCode))
    // writing m.ProducerId: The current producer id.
    buff = binary.BigEndian.AppendUint64(buff, uint64(m.ProducerId))
    // writing m.ProducerEpoch: The current epoch associated with the producer id.
    buff = binary.BigEndian.AppendUint16(buff, uint16(m.ProducerEpoch))
    if version >= 2 {
        numTaggedFields4 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields4))
    }
    return buff
}

func (m *InitProducerIdResponse) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    size += 4
    // size for m.ErrorCode: The error code, or 0 if there was no error.
    size += 2
    // size for m.ProducerId: The current producer id.
    size += 8
    // size for m.ProducerEpoch: The current epoch associated with the producer id.
    size += 2
    numTaggedFields1:= 0
    numTaggedFields1 += 0
    if version >= 2 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields1)
    }
    return size, tagSizes
}


