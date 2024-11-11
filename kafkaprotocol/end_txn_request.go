// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"

type EndTxnRequest struct {
    // The ID of the transaction to end.
    TransactionalId *string
    // The producer ID.
    ProducerId int64
    // The current epoch associated with the producer.
    ProducerEpoch int16
    // True if the transaction was committed, false if it was aborted.
    Committed bool
}

func (m *EndTxnRequest) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.TransactionalId: The ID of the transaction to end.
        if version >= 3 {
            // flexible and not nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l0 := int(u - 1)
            s := string(buff[offset: offset + l0])
            m.TransactionalId = &s
            offset += l0
        } else {
            // non flexible and non nullable
            var l0 int
            l0 = int(binary.BigEndian.Uint16(buff[offset:]))
            offset += 2
            s := string(buff[offset: offset + l0])
            m.TransactionalId = &s
            offset += l0
        }
    }
    {
        // reading m.ProducerId: The producer ID.
        m.ProducerId = int64(binary.BigEndian.Uint64(buff[offset:]))
        offset += 8
    }
    {
        // reading m.ProducerEpoch: The current epoch associated with the producer.
        m.ProducerEpoch = int16(binary.BigEndian.Uint16(buff[offset:]))
        offset += 2
    }
    {
        // reading m.Committed: True if the transaction was committed, false if it was aborted.
        m.Committed = buff[offset] == 1
        offset++
    }
    if version >= 3 {
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

func (m *EndTxnRequest) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.TransactionalId: The ID of the transaction to end.
    if version >= 3 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(*m.TransactionalId) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.TransactionalId)))
    }
    if m.TransactionalId != nil {
        buff = append(buff, *m.TransactionalId...)
    }
    // writing m.ProducerId: The producer ID.
    buff = binary.BigEndian.AppendUint64(buff, uint64(m.ProducerId))
    // writing m.ProducerEpoch: The current epoch associated with the producer.
    buff = binary.BigEndian.AppendUint16(buff, uint16(m.ProducerEpoch))
    // writing m.Committed: True if the transaction was committed, false if it was aborted.
    if m.Committed {
        buff = append(buff, 1)
    } else {
        buff = append(buff, 0)
    }
    if version >= 3 {
        numTaggedFields4 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields4))
    }
    return buff
}

func (m *EndTxnRequest) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.TransactionalId: The ID of the transaction to end.
    if version >= 3 {
        // flexible and not nullable
        size += sizeofUvarint(len(*m.TransactionalId) + 1)
    } else {
        // non flexible and non nullable
        size += 2
    }
    if m.TransactionalId != nil {
        size += len(*m.TransactionalId)
    }
    // size for m.ProducerId: The producer ID.
    size += 8
    // size for m.ProducerEpoch: The current epoch associated with the producer.
    size += 2
    // size for m.Committed: True if the transaction was committed, false if it was aborted.
    size += 1
    numTaggedFields1:= 0
    numTaggedFields1 += 0
    if version >= 3 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields1)
    }
    return size, tagSizes
}

func (m *EndTxnRequest) HeaderVersions(version int16) (int16, int16) {
    if version >= 3 {
        return 2, 1
    } else {
        return 1, 0
    }
}

func (m *EndTxnRequest) SupportedApiVersions() (int16, int16) {
    return -1, -1
}
