// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"
import "github.com/spirit-labs/tektite/common"

type PutUserCredentialsRequest struct {
    // The username of the user to put.
    Username *string
    // The stored key.
    StoredKey []byte
    // The ServerKey.
    ServerKey []byte
    // The salt.
    Salt *string
    // The number of iterations.
    Iters int64
}

func (m *PutUserCredentialsRequest) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.Username: The username of the user to put.
        // non flexible and non nullable
        var l0 int
        l0 = int(binary.BigEndian.Uint16(buff[offset:]))
        offset += 2
        s := string(buff[offset: offset + l0])
        m.Username = &s
        offset += l0
    }
    {
        // reading m.StoredKey: The stored key.
        // non flexible and non nullable
        var l1 int
        l1 = int(binary.BigEndian.Uint32(buff[offset:]))
        offset += 4
        m.StoredKey = common.ByteSliceCopy(buff[offset: offset + l1])
        offset += l1
    }
    {
        // reading m.ServerKey: The ServerKey.
        // non flexible and non nullable
        var l2 int
        l2 = int(binary.BigEndian.Uint32(buff[offset:]))
        offset += 4
        m.ServerKey = common.ByteSliceCopy(buff[offset: offset + l2])
        offset += l2
    }
    {
        // reading m.Salt: The salt.
        // non flexible and non nullable
        var l3 int
        l3 = int(binary.BigEndian.Uint16(buff[offset:]))
        offset += 2
        s := string(buff[offset: offset + l3])
        m.Salt = &s
        offset += l3
    }
    {
        // reading m.Iters: The number of iterations.
        m.Iters = int64(binary.BigEndian.Uint64(buff[offset:]))
        offset += 8
    }
    return offset, nil
}

func (m *PutUserCredentialsRequest) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.Username: The username of the user to put.
    // non flexible and non nullable
    buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.Username)))
    if m.Username != nil {
        buff = append(buff, *m.Username...)
    }
    // writing m.StoredKey: The stored key.
    // non flexible and non nullable
    buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.StoredKey)))
    if m.StoredKey != nil {
        buff = append(buff, m.StoredKey...)
    }
    // writing m.ServerKey: The ServerKey.
    // non flexible and non nullable
    buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.ServerKey)))
    if m.ServerKey != nil {
        buff = append(buff, m.ServerKey...)
    }
    // writing m.Salt: The salt.
    // non flexible and non nullable
    buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.Salt)))
    if m.Salt != nil {
        buff = append(buff, *m.Salt...)
    }
    // writing m.Iters: The number of iterations.
    buff = binary.BigEndian.AppendUint64(buff, uint64(m.Iters))
    return buff
}

func (m *PutUserCredentialsRequest) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    // size for m.Username: The username of the user to put.
    // non flexible and non nullable
    size += 2
    if m.Username != nil {
        size += len(*m.Username)
    }
    // size for m.StoredKey: The stored key.
    // non flexible and non nullable
    size += 4
    if m.StoredKey != nil {
        size += len(m.StoredKey)
    }
    // size for m.ServerKey: The ServerKey.
    // non flexible and non nullable
    size += 4
    if m.ServerKey != nil {
        size += len(m.ServerKey)
    }
    // size for m.Salt: The salt.
    // non flexible and non nullable
    size += 2
    if m.Salt != nil {
        size += len(*m.Salt)
    }
    // size for m.Iters: The number of iterations.
    size += 8
    return size, tagSizes
}

func (m *PutUserCredentialsRequest) HeaderVersions(version int16) (int16, int16) {
    return 1, 0
}

func (m *PutUserCredentialsRequest) SupportedApiVersions() (int16, int16) {
    return 0, 0
}
