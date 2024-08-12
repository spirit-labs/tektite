// Package protocol - This is a generated file, please do not edit

package protocol

import "encoding/binary"
import "github.com/spirit-labs/tektite/common"

type SaslAuthenticateRequest struct {
    // The SASL authentication bytes from the client, as defined by the SASL mechanism.
    AuthBytes []byte
}

func (m *SaslAuthenticateRequest) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.AuthBytes: The SASL authentication bytes from the client, as defined by the SASL mechanism.
        if version >= 2 {
            // flexible and not nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l0 := int(u - 1)
            m.AuthBytes = common.ByteSliceCopy(buff[offset: offset + l0])
            offset += l0
        } else {
            // non flexible and non nullable
            var l0 int
            l0 = int(binary.BigEndian.Uint32(buff[offset:]))
            offset += 4
            m.AuthBytes = common.ByteSliceCopy(buff[offset: offset + l0])
            offset += l0
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

func (m *SaslAuthenticateRequest) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.AuthBytes: The SASL authentication bytes from the client, as defined by the SASL mechanism.
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
    if version >= 2 {
        numTaggedFields1 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields1))
    }
    return buff
}

func (m *SaslAuthenticateRequest) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.AuthBytes: The SASL authentication bytes from the client, as defined by the SASL mechanism.
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
    numTaggedFields1:= 0
    numTaggedFields1 += 0
    if version >= 2 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields1)
    }
    return size, tagSizes
}

func (m *SaslAuthenticateRequest) HeaderVersions(version int16) (int16, int16) {
    if version >= 2 {
        return 2, 1
    } else {
        return 1, 0
    }
}

func (m *SaslAuthenticateRequest) SupportedApiVersions() (int16, int16) {
    return 0, 1
}
