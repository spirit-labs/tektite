// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"
import "unsafe"

type SaslHandshakeResponse struct {
    // The error code, or 0 if there was no error.
    ErrorCode int16
    // The mechanisms enabled in the server.
    Mechanisms []*string
}

func (m *SaslHandshakeResponse) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.ErrorCode: The error code, or 0 if there was no error.
        m.ErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
        offset += 2
    }
    {
        // reading m.Mechanisms: The mechanisms enabled in the server.
        var l0 int
        // non flexible and non nullable
        l0 = int(binary.BigEndian.Uint32(buff[offset:]))
        offset += 4
        if l0 >= 0 {
            // length will be -1 if field is null
            mechanisms := make([]*string, l0)
            for i0 := 0; i0 < l0; i0++ {
                // non flexible and non nullable
                var l1 int
                l1 = int(binary.BigEndian.Uint16(buff[offset:]))
                offset += 2
                s := string(buff[offset: offset + l1])
                mechanisms[i0] = &s
                offset += l1
            }
            m.Mechanisms = mechanisms
        }
    }
    return offset, nil
}

func (m *SaslHandshakeResponse) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.ErrorCode: The error code, or 0 if there was no error.
    buff = binary.BigEndian.AppendUint16(buff, uint16(m.ErrorCode))
    // writing m.Mechanisms: The mechanisms enabled in the server.
    // non flexible and non nullable
    buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.Mechanisms)))
    for _, mechanisms := range m.Mechanisms {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint16(buff, uint16(len(*mechanisms)))
        if mechanisms != nil {
            buff = append(buff, *mechanisms...)
        }
    }
    return buff
}

func (m *SaslHandshakeResponse) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    // size for m.ErrorCode: The error code, or 0 if there was no error.
    size += 2
    // size for m.Mechanisms: The mechanisms enabled in the server.
    // non flexible and non nullable
    size += 4
    for _, mechanisms := range m.Mechanisms {
        size += 0 * int(unsafe.Sizeof(mechanisms)) // hack to make sure loop variable is always used
        // non flexible and non nullable
        size += 2
        if mechanisms != nil {
            size += len(*mechanisms)
        }
    }
    return size, tagSizes
}


