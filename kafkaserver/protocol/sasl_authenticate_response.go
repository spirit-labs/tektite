// Package protocol - This is a generated file, please do not edit

package protocol

import "encoding/binary"
import "github.com/spirit-labs/tektite/common"

type SaslAuthenticateResponse struct {
    // The error code, or 0 if there was no error.
    ErrorCode int16
    // The error message, or null if there was no error.
    ErrorMessage *string
    // The SASL authentication bytes from the server, as defined by the SASL mechanism.
    AuthBytes []byte
    // Number of milliseconds after which only re-authentication over the existing connection to create a new session can occur.
    SessionLifetimeMs int64
}

func (m *SaslAuthenticateResponse) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.ErrorCode: The error code, or 0 if there was no error.
        m.ErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
        offset += 2
    }
    {
        // reading m.ErrorMessage: The error message, or null if there was no error.
        if version >= 2 {
            // flexible and nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l0 := int(u - 1)
            if l0 > 0 {
                s := string(buff[offset: offset + l0])
                m.ErrorMessage = &s
                offset += l0
            } else {
                m.ErrorMessage = nil
            }
        } else {
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
    }
    {
        // reading m.AuthBytes: The SASL authentication bytes from the server, as defined by the SASL mechanism.
        if version >= 2 {
            // flexible and not nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l1 := int(u - 1)
            m.AuthBytes = common.ByteSliceCopy(buff[offset: offset + l1])
            offset += l1
        } else {
            // non flexible and non nullable
            var l1 int
            l1 = int(binary.BigEndian.Uint32(buff[offset:]))
            offset += 4
            m.AuthBytes = common.ByteSliceCopy(buff[offset: offset + l1])
            offset += l1
        }
    }
    if version >= 1 {
        {
            // reading m.SessionLifetimeMs: Number of milliseconds after which only re-authentication over the existing connection to create a new session can occur.
            m.SessionLifetimeMs = int64(binary.BigEndian.Uint64(buff[offset:]))
            offset += 8
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

func (m *SaslAuthenticateResponse) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.ErrorCode: The error code, or 0 if there was no error.
    buff = binary.BigEndian.AppendUint16(buff, uint16(m.ErrorCode))
    // writing m.ErrorMessage: The error message, or null if there was no error.
    if version >= 2 {
        // flexible and nullable
        if m.ErrorMessage == nil {
            // null
            buff = append(buff, 0)
        } else {
            // not null
            buff = binary.AppendUvarint(buff, uint64(len(*m.ErrorMessage) + 1))
        }
    } else {
        // non flexible and nullable
        if m.ErrorMessage == nil {
            // null
            buff = binary.BigEndian.AppendUint16(buff, 65535)
        } else {
            // not null
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.ErrorMessage)))
        }
    }
    if m.ErrorMessage != nil {
        buff = append(buff, *m.ErrorMessage...)
    }
    // writing m.AuthBytes: The SASL authentication bytes from the server, as defined by the SASL mechanism.
    if version >= 2 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.AuthBytes) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.AuthBytes)))
    }
    if m.AuthBytes != nil {
        buff = append(buff, m.AuthBytes...)
    }
    if version >= 1 {
        // writing m.SessionLifetimeMs: Number of milliseconds after which only re-authentication over the existing connection to create a new session can occur.
        buff = binary.BigEndian.AppendUint64(buff, uint64(m.SessionLifetimeMs))
    }
    if version >= 2 {
        numTaggedFields4 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields4))
    }
    return buff
}

func (m *SaslAuthenticateResponse) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.ErrorCode: The error code, or 0 if there was no error.
    size += 2
    // size for m.ErrorMessage: The error message, or null if there was no error.
    if version >= 2 {
        // flexible and nullable
        if m.ErrorMessage == nil {
            // null
            size += 1
        } else {
            // not null
            size += sizeofUvarint(len(*m.ErrorMessage) + 1)
        }
    } else {
        // non flexible and nullable
        size += 2
    }
    if m.ErrorMessage != nil {
        size += len(*m.ErrorMessage)
    }
    // size for m.AuthBytes: The SASL authentication bytes from the server, as defined by the SASL mechanism.
    if version >= 2 {
        // flexible and not nullable
        size += sizeofUvarint(len(m.AuthBytes) + 1)
    } else {
        // non flexible and non nullable
        size += 4
    }
    if m.AuthBytes != nil {
        size += len(m.AuthBytes)
    }
    if version >= 1 {
        // size for m.SessionLifetimeMs: Number of milliseconds after which only re-authentication over the existing connection to create a new session can occur.
        size += 8
    }
    numTaggedFields1:= 0
    numTaggedFields1 += 0
    if version >= 2 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields1)
    }
    return size, tagSizes
}


