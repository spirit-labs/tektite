// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"

type PutUserCredentialsResponse struct {
    // The error code.
    ErrorCode int16
    // The error message, or null if there was no error.
    ErrorMessage *string
}

func (m *PutUserCredentialsResponse) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.ErrorCode: The error code.
        m.ErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
        offset += 2
    }
    {
        // reading m.ErrorMessage: The error message, or null if there was no error.
        // non flexible and nullable
        var l0 int
        l0 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
        offset += 2
        if l0 > 0 {
            s := string(buff[offset: offset + l0])
            m.ErrorMessage = &s
            offset += l0
        } else {
            m.ErrorMessage = nil
        }
    }
    return offset, nil
}

func (m *PutUserCredentialsResponse) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.ErrorCode: The error code.
    buff = binary.BigEndian.AppendUint16(buff, uint16(m.ErrorCode))
    // writing m.ErrorMessage: The error message, or null if there was no error.
    // non flexible and nullable
    if m.ErrorMessage == nil {
        // null
        buff = binary.BigEndian.AppendUint16(buff, 65535)
    } else {
        // not null
        buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.ErrorMessage)))
    }
    if m.ErrorMessage != nil {
        buff = append(buff, *m.ErrorMessage...)
    }
    return buff
}

func (m *PutUserCredentialsResponse) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    // size for m.ErrorCode: The error code.
    size += 2
    // size for m.ErrorMessage: The error message, or null if there was no error.
    // non flexible and nullable
    size += 2
    if m.ErrorMessage != nil {
        size += len(*m.ErrorMessage)
    }
    return size, tagSizes
}


