// Package protocol - This is a generated file, please do not edit

package protocol

import "encoding/binary"

type InitProducerIdRequest struct {
    // The transactional id, or null if the producer is not transactional.
    TransactionalId *string
    // The time in ms to wait before aborting idle transactions sent by this producer. This is only relevant if a TransactionalId has been defined.
    TransactionTimeoutMs int32
    // The producer id. This is used to disambiguate requests if a transactional id is reused following its expiration.
    ProducerId int64
    // The producer's current epoch. This will be checked against the producer epoch on the broker, and the request will return an error if they do not match.
    ProducerEpoch int16
}

func (m *InitProducerIdRequest) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.TransactionalId: The transactional id, or null if the producer is not transactional.
        if version >= 2 {
            // flexible and nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l0 := int(u - 1)
            if l0 > 0 {
                s := string(buff[offset: offset + l0])
                m.TransactionalId = &s
                offset += l0
            } else {
                m.TransactionalId = nil
            }
        } else {
            // non flexible and nullable
            var l0 int
            l0 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
            offset += 2
            if l0 > 0 {
                s := string(buff[offset: offset + l0])
                m.TransactionalId = &s
                offset += l0
            } else {
                m.TransactionalId = nil
            }
        }
    }
    {
        // reading m.TransactionTimeoutMs: The time in ms to wait before aborting idle transactions sent by this producer. This is only relevant if a TransactionalId has been defined.
        m.TransactionTimeoutMs = int32(binary.BigEndian.Uint32(buff[offset:]))
        offset += 4
    }
    if version >= 3 {
        {
            // reading m.ProducerId: The producer id. This is used to disambiguate requests if a transactional id is reused following its expiration.
            m.ProducerId = int64(binary.BigEndian.Uint64(buff[offset:]))
            offset += 8
        }
        {
            // reading m.ProducerEpoch: The producer's current epoch. This will be checked against the producer epoch on the broker, and the request will return an error if they do not match.
            m.ProducerEpoch = int16(binary.BigEndian.Uint16(buff[offset:]))
            offset += 2
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

func (m *InitProducerIdRequest) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.TransactionalId: The transactional id, or null if the producer is not transactional.
    if version >= 2 {
        // flexible and nullable
        if m.TransactionalId == nil {
            // null
            buff = append(buff, 0)
        } else {
            // not null
            buff = binary.AppendUvarint(buff, uint64(len(*m.TransactionalId) + 1))
        }
    } else {
        // non flexible and nullable
        if m.TransactionalId == nil {
            // null
            buff = binary.BigEndian.AppendUint16(buff, 65535)
        } else {
            // not null
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.TransactionalId)))
        }
    }
    if m.TransactionalId != nil {
        buff = append(buff, *m.TransactionalId...)
    }
    // writing m.TransactionTimeoutMs: The time in ms to wait before aborting idle transactions sent by this producer. This is only relevant if a TransactionalId has been defined.
    buff = binary.BigEndian.AppendUint32(buff, uint32(m.TransactionTimeoutMs))
    if version >= 3 {
        // writing m.ProducerId: The producer id. This is used to disambiguate requests if a transactional id is reused following its expiration.
        buff = binary.BigEndian.AppendUint64(buff, uint64(m.ProducerId))
        // writing m.ProducerEpoch: The producer's current epoch. This will be checked against the producer epoch on the broker, and the request will return an error if they do not match.
        buff = binary.BigEndian.AppendUint16(buff, uint16(m.ProducerEpoch))
    }
    if version >= 2 {
        numTaggedFields4 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields4))
    }
    return buff
}

func (m *InitProducerIdRequest) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.TransactionalId: The transactional id, or null if the producer is not transactional.
    if version >= 2 {
        // flexible and nullable
        if m.TransactionalId == nil {
            // null
            size += 1
        } else {
            // not null
            size += sizeofUvarint(len(*m.TransactionalId) + 1)
        }
    } else {
        // non flexible and nullable
        size += 2
    }
    if m.TransactionalId != nil {
        size += len(*m.TransactionalId)
    }
    // size for m.TransactionTimeoutMs: The time in ms to wait before aborting idle transactions sent by this producer. This is only relevant if a TransactionalId has been defined.
    size += 4
    if version >= 3 {
        // size for m.ProducerId: The producer id. This is used to disambiguate requests if a transactional id is reused following its expiration.
        size += 8
        // size for m.ProducerEpoch: The producer's current epoch. This will be checked against the producer epoch on the broker, and the request will return an error if they do not match.
        size += 2
    }
    numTaggedFields1:= 0
    numTaggedFields1 += 0
    if version >= 2 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields1)
    }
    return size, tagSizes
}

func (m *InitProducerIdRequest) HeaderVersions(version int16) (int16, int16) {
    if version >= 2 {
        return 2, 1
    } else {
        return 1, 0
    }
}

func (m *InitProducerIdRequest) SupportedApiVersions() (int16, int16) {
    return 0, 0
}
