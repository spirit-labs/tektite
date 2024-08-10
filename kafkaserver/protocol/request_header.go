// Package protocol - This is a generated file, please do not edit

package protocol

import "encoding/binary"

type RequestHeader struct {
    // The API key of this request.
    RequestApiKey int16
    // The API version of this request.
    RequestApiVersion int16
    // The correlation ID of this request.
    CorrelationId int32
    // The client ID string.
    ClientId *string
}

func (m *RequestHeader) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.RequestApiKey: The API key of this request.
        m.RequestApiKey = int16(binary.BigEndian.Uint16(buff[offset:]))
        offset += 2
    }
    {
        // reading m.RequestApiVersion: The API version of this request.
        m.RequestApiVersion = int16(binary.BigEndian.Uint16(buff[offset:]))
        offset += 2
    }
    {
        // reading m.CorrelationId: The correlation ID of this request.
        m.CorrelationId = int32(binary.BigEndian.Uint32(buff[offset:]))
        offset += 4
    }
    if version >= 1 {
        {
            // reading m.ClientId: The client ID string.
            // non flexible and nullable
            var l0 int
            l0 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
            offset += 2
            if l0 > 0 {
                s := string(buff[offset: offset + l0])
                m.ClientId = &s
                offset += l0
            } else {
                m.ClientId = nil
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
    return offset, nil
}

func (m *RequestHeader) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.RequestApiKey: The API key of this request.
    buff = binary.BigEndian.AppendUint16(buff, uint16(m.RequestApiKey))
    // writing m.RequestApiVersion: The API version of this request.
    buff = binary.BigEndian.AppendUint16(buff, uint16(m.RequestApiVersion))
    // writing m.CorrelationId: The correlation ID of this request.
    buff = binary.BigEndian.AppendUint32(buff, uint32(m.CorrelationId))
    if version >= 1 {
        // writing m.ClientId: The client ID string.
        // non flexible and nullable
        if m.ClientId == nil {
            // null
            buff = binary.BigEndian.AppendUint16(buff, 65535)
        } else {
            // not null
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.ClientId)))
        }
        if m.ClientId != nil {
            buff = append(buff, *m.ClientId...)
        }
    }
    if version >= 2 {
        numTaggedFields4 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields4))
    }
    return buff
}

func (m *RequestHeader) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.RequestApiKey: The API key of this request.
    size += 2
    // size for m.RequestApiVersion: The API version of this request.
    size += 2
    // size for m.CorrelationId: The correlation ID of this request.
    size += 4
    if version >= 1 {
        // size for m.ClientId: The client ID string.
        // non flexible and nullable
        size += 2
        if m.ClientId != nil {
            size += len(*m.ClientId)
        }
    }
    numTaggedFields1:= 0
    numTaggedFields1 += 0
    if version >= 2 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields1)
    }
    return size, tagSizes
}


