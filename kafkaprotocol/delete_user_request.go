// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"

type DeleteUserRequest struct {
    // The username of the user to delete.
    Username *string
}

func (m *DeleteUserRequest) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.Username: The username of the user to delete.
        // non flexible and non nullable
        var l0 int
        l0 = int(binary.BigEndian.Uint16(buff[offset:]))
        offset += 2
        s := string(buff[offset: offset + l0])
        m.Username = &s
        offset += l0
    }
    return offset, nil
}

func (m *DeleteUserRequest) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.Username: The username of the user to delete.
    // non flexible and non nullable
    buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.Username)))
    if m.Username != nil {
        buff = append(buff, *m.Username...)
    }
    return buff
}

func (m *DeleteUserRequest) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    // size for m.Username: The username of the user to delete.
    // non flexible and non nullable
    size += 2
    if m.Username != nil {
        size += len(*m.Username)
    }
    return size, tagSizes
}

func (m *DeleteUserRequest) HeaderVersions(version int16) (int16, int16) {
    return 1, 0
}

func (m *DeleteUserRequest) SupportedApiVersions() (int16, int16) {
    return 0, 0
}
