// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"

type SaslHandshakeRequest struct {
    // The SASL mechanism chosen by the client.
    Mechanism *string
}

func (m *SaslHandshakeRequest) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.Mechanism: The SASL mechanism chosen by the client.
        // non flexible and non nullable
        var l0 int
        l0 = int(binary.BigEndian.Uint16(buff[offset:]))
        offset += 2
        s := string(buff[offset: offset + l0])
        m.Mechanism = &s
        offset += l0
    }
    return offset, nil
}

func (m *SaslHandshakeRequest) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.Mechanism: The SASL mechanism chosen by the client.
    // non flexible and non nullable
    buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.Mechanism)))
    if m.Mechanism != nil {
        buff = append(buff, *m.Mechanism...)
    }
    return buff
}

func (m *SaslHandshakeRequest) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    // size for m.Mechanism: The SASL mechanism chosen by the client.
    // non flexible and non nullable
    size += 2
    if m.Mechanism != nil {
        size += len(*m.Mechanism)
    }
    return size, tagSizes
}

func (m *SaslHandshakeRequest) HeaderVersions(version int16) (int16, int16) {
    return 1, 0
}

func (m *SaslHandshakeRequest) SupportedApiVersions() (int16, int16) {
    return 0, 1
}
