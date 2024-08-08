// Package protocol - This is a generated file, please do not edit

package protocol

import "encoding/binary"

type ResponseHeader struct {
    // The correlation ID of this response.
    CorrelationId int32
}

func (m *ResponseHeader) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.CorrelationId: The correlation ID of this response.
        m.CorrelationId = int32(binary.BigEndian.Uint32(buff[offset:]))
        offset += 4
    }
    if version >= 1 {
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

func (m *ResponseHeader) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.CorrelationId: The correlation ID of this response.
    buff = binary.BigEndian.AppendUint32(buff, uint32(m.CorrelationId))
    if version >= 1 {
        numTaggedFields1 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields1))
    }
    return buff
}

func (m *ResponseHeader) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.CorrelationId: The correlation ID of this response.
    size += 4
    numTaggedFields1:= 0
    numTaggedFields1 += 0
    if version >= 1 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields1)
    }
    return size, tagSizes
}

