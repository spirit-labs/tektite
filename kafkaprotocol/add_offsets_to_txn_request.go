// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"

type AddOffsetsToTxnRequest struct {
    // The transactional id corresponding to the transaction.
    TransactionalId *string
    // Current producer id in use by the transactional id.
    ProducerId int64
    // Current epoch associated with the producer id.
    ProducerEpoch int16
    // The unique group identifier.
    GroupId *string
}

func (m *AddOffsetsToTxnRequest) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.TransactionalId: The transactional id corresponding to the transaction.
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
        // reading m.ProducerId: Current producer id in use by the transactional id.
        m.ProducerId = int64(binary.BigEndian.Uint64(buff[offset:]))
        offset += 8
    }
    {
        // reading m.ProducerEpoch: Current epoch associated with the producer id.
        m.ProducerEpoch = int16(binary.BigEndian.Uint16(buff[offset:]))
        offset += 2
    }
    {
        // reading m.GroupId: The unique group identifier.
        if version >= 3 {
            // flexible and not nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l1 := int(u - 1)
            s := string(buff[offset: offset + l1])
            m.GroupId = &s
            offset += l1
        } else {
            // non flexible and non nullable
            var l1 int
            l1 = int(binary.BigEndian.Uint16(buff[offset:]))
            offset += 2
            s := string(buff[offset: offset + l1])
            m.GroupId = &s
            offset += l1
        }
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

func (m *AddOffsetsToTxnRequest) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.TransactionalId: The transactional id corresponding to the transaction.
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
    // writing m.ProducerId: Current producer id in use by the transactional id.
    buff = binary.BigEndian.AppendUint64(buff, uint64(m.ProducerId))
    // writing m.ProducerEpoch: Current epoch associated with the producer id.
    buff = binary.BigEndian.AppendUint16(buff, uint16(m.ProducerEpoch))
    // writing m.GroupId: The unique group identifier.
    if version >= 3 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(*m.GroupId) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.GroupId)))
    }
    if m.GroupId != nil {
        buff = append(buff, *m.GroupId...)
    }
    if version >= 3 {
        numTaggedFields4 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields4))
    }
    return buff
}

func (m *AddOffsetsToTxnRequest) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.TransactionalId: The transactional id corresponding to the transaction.
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
    // size for m.ProducerId: Current producer id in use by the transactional id.
    size += 8
    // size for m.ProducerEpoch: Current epoch associated with the producer id.
    size += 2
    // size for m.GroupId: The unique group identifier.
    if version >= 3 {
        // flexible and not nullable
        size += sizeofUvarint(len(*m.GroupId) + 1)
    } else {
        // non flexible and non nullable
        size += 2
    }
    if m.GroupId != nil {
        size += len(*m.GroupId)
    }
    numTaggedFields1:= 0
    numTaggedFields1 += 0
    if version >= 3 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields1)
    }
    return size, tagSizes
}

func (m *AddOffsetsToTxnRequest) HeaderVersions(version int16) (int16, int16) {
    if version >= 3 {
        return 2, 1
    } else {
        return 1, 0
    }
}

func (m *AddOffsetsToTxnRequest) SupportedApiVersions() (int16, int16) {
    return -1, -1
}
