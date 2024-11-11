// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"
import "unsafe"

type AddPartitionsToTxnRequestAddPartitionsToTxnTopic struct {
    // The name of the topic.
    Name *string
    // The partition indexes to add to the transaction
    Partitions []int32
}

type AddPartitionsToTxnRequestAddPartitionsToTxnTransaction struct {
    // The transactional id corresponding to the transaction.
    TransactionalId *string
    // Current producer id in use by the transactional id.
    ProducerId int64
    // Current epoch associated with the producer id.
    ProducerEpoch int16
    // Boolean to signify if we want to check if the partition is in the transaction rather than add it.
    VerifyOnly bool
    // The partitions to add to the transaction.
    Topics []AddPartitionsToTxnRequestAddPartitionsToTxnTopic
}

type AddPartitionsToTxnRequest struct {
    // List of transactions to add partitions to.
    Transactions []AddPartitionsToTxnRequestAddPartitionsToTxnTransaction
    // The transactional id corresponding to the transaction.
    V3AndBelowTransactionalId *string
    // Current producer id in use by the transactional id.
    V3AndBelowProducerId int64
    // Current epoch associated with the producer id.
    V3AndBelowProducerEpoch int16
    // The partitions to add to the transaction.
    V3AndBelowTopics []AddPartitionsToTxnRequestAddPartitionsToTxnTopic
}

func (m *AddPartitionsToTxnRequest) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    if version >= 4 {
        {
            // reading m.Transactions: List of transactions to add partitions to.
            var l0 int
            // flexible and not nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l0 = int(u - 1)
            if l0 >= 0 {
                // length will be -1 if field is null
                transactions := make([]AddPartitionsToTxnRequestAddPartitionsToTxnTransaction, l0)
                for i0 := 0; i0 < l0; i0++ {
                    // reading non tagged fields
                    {
                        // reading transactions[i0].TransactionalId: The transactional id corresponding to the transaction.
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l1 := int(u - 1)
                        s := string(buff[offset: offset + l1])
                        transactions[i0].TransactionalId = &s
                        offset += l1
                    }
                    {
                        // reading transactions[i0].ProducerId: Current producer id in use by the transactional id.
                        transactions[i0].ProducerId = int64(binary.BigEndian.Uint64(buff[offset:]))
                        offset += 8
                    }
                    {
                        // reading transactions[i0].ProducerEpoch: Current epoch associated with the producer id.
                        transactions[i0].ProducerEpoch = int16(binary.BigEndian.Uint16(buff[offset:]))
                        offset += 2
                    }
                    {
                        // reading transactions[i0].VerifyOnly: Boolean to signify if we want to check if the partition is in the transaction rather than add it.
                        transactions[i0].VerifyOnly = buff[offset] == 1
                        offset++
                    }
                    {
                        // reading transactions[i0].Topics: The partitions to add to the transaction.
                        var l2 int
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l2 = int(u - 1)
                        if l2 >= 0 {
                            // length will be -1 if field is null
                            topics := make([]AddPartitionsToTxnRequestAddPartitionsToTxnTopic, l2)
                            for i1 := 0; i1 < l2; i1++ {
                                // reading non tagged fields
                                {
                                    // reading topics[i1].Name: The name of the topic.
                                    // flexible and not nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l3 := int(u - 1)
                                    s := string(buff[offset: offset + l3])
                                    topics[i1].Name = &s
                                    offset += l3
                                }
                                {
                                    // reading topics[i1].Partitions: The partition indexes to add to the transaction
                                    var l4 int
                                    // flexible and not nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l4 = int(u - 1)
                                    if l4 >= 0 {
                                        // length will be -1 if field is null
                                        partitions := make([]int32, l4)
                                        for i2 := 0; i2 < l4; i2++ {
                                            partitions[i2] = int32(binary.BigEndian.Uint32(buff[offset:]))
                                            offset += 4
                                        }
                                        topics[i1].Partitions = partitions
                                    }
                                }
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
                        transactions[i0].Topics = topics
                        }
                    }
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
            m.Transactions = transactions
            }
        }
    }
    if version <= 3 {
        {
            // reading m.V3AndBelowTransactionalId: The transactional id corresponding to the transaction.
            if version >= 3 {
                // flexible and not nullable
                u, n := binary.Uvarint(buff[offset:])
                offset += n
                l5 := int(u - 1)
                s := string(buff[offset: offset + l5])
                m.V3AndBelowTransactionalId = &s
                offset += l5
            } else {
                // non flexible and non nullable
                var l5 int
                l5 = int(binary.BigEndian.Uint16(buff[offset:]))
                offset += 2
                s := string(buff[offset: offset + l5])
                m.V3AndBelowTransactionalId = &s
                offset += l5
            }
        }
        {
            // reading m.V3AndBelowProducerId: Current producer id in use by the transactional id.
            m.V3AndBelowProducerId = int64(binary.BigEndian.Uint64(buff[offset:]))
            offset += 8
        }
        {
            // reading m.V3AndBelowProducerEpoch: Current epoch associated with the producer id.
            m.V3AndBelowProducerEpoch = int16(binary.BigEndian.Uint16(buff[offset:]))
            offset += 2
        }
        {
            // reading m.V3AndBelowTopics: The partitions to add to the transaction.
            var l6 int
            if version >= 3 {
                // flexible and not nullable
                u, n := binary.Uvarint(buff[offset:])
                offset += n
                l6 = int(u - 1)
            } else {
                // non flexible and non nullable
                l6 = int(binary.BigEndian.Uint32(buff[offset:]))
                offset += 4
            }
            if l6 >= 0 {
                // length will be -1 if field is null
                v3AndBelowTopics := make([]AddPartitionsToTxnRequestAddPartitionsToTxnTopic, l6)
                for i3 := 0; i3 < l6; i3++ {
                    // reading non tagged fields
                    {
                        // reading v3AndBelowTopics[i3].Name: The name of the topic.
                        if version >= 3 {
                            // flexible and not nullable
                            u, n := binary.Uvarint(buff[offset:])
                            offset += n
                            l7 := int(u - 1)
                            s := string(buff[offset: offset + l7])
                            v3AndBelowTopics[i3].Name = &s
                            offset += l7
                        } else {
                            // non flexible and non nullable
                            var l7 int
                            l7 = int(binary.BigEndian.Uint16(buff[offset:]))
                            offset += 2
                            s := string(buff[offset: offset + l7])
                            v3AndBelowTopics[i3].Name = &s
                            offset += l7
                        }
                    }
                    {
                        // reading v3AndBelowTopics[i3].Partitions: The partition indexes to add to the transaction
                        var l8 int
                        if version >= 3 {
                            // flexible and not nullable
                            u, n := binary.Uvarint(buff[offset:])
                            offset += n
                            l8 = int(u - 1)
                        } else {
                            // non flexible and non nullable
                            l8 = int(binary.BigEndian.Uint32(buff[offset:]))
                            offset += 4
                        }
                        if l8 >= 0 {
                            // length will be -1 if field is null
                            partitions := make([]int32, l8)
                            for i4 := 0; i4 < l8; i4++ {
                                partitions[i4] = int32(binary.BigEndian.Uint32(buff[offset:]))
                                offset += 4
                            }
                            v3AndBelowTopics[i3].Partitions = partitions
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
                }
            m.V3AndBelowTopics = v3AndBelowTopics
            }
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

func (m *AddPartitionsToTxnRequest) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    if version >= 4 {
        // writing m.Transactions: List of transactions to add partitions to.
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.Transactions) + 1))
        for _, transactions := range m.Transactions {
            // writing non tagged fields
            // writing transactions.TransactionalId: The transactional id corresponding to the transaction.
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*transactions.TransactionalId) + 1))
            if transactions.TransactionalId != nil {
                buff = append(buff, *transactions.TransactionalId...)
            }
            // writing transactions.ProducerId: Current producer id in use by the transactional id.
            buff = binary.BigEndian.AppendUint64(buff, uint64(transactions.ProducerId))
            // writing transactions.ProducerEpoch: Current epoch associated with the producer id.
            buff = binary.BigEndian.AppendUint16(buff, uint16(transactions.ProducerEpoch))
            // writing transactions.VerifyOnly: Boolean to signify if we want to check if the partition is in the transaction rather than add it.
            if transactions.VerifyOnly {
                buff = append(buff, 1)
            } else {
                buff = append(buff, 0)
            }
            // writing transactions.Topics: The partitions to add to the transaction.
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(transactions.Topics) + 1))
            for _, topics := range transactions.Topics {
                // writing non tagged fields
                // writing topics.Name: The name of the topic.
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(*topics.Name) + 1))
                if topics.Name != nil {
                    buff = append(buff, *topics.Name...)
                }
                // writing topics.Partitions: The partition indexes to add to the transaction
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(topics.Partitions) + 1))
                for _, partitions := range topics.Partitions {
                    buff = binary.BigEndian.AppendUint32(buff, uint32(partitions))
                }
                numTaggedFields8 := 0
                // write number of tagged fields
                buff = binary.AppendUvarint(buff, uint64(numTaggedFields8))
            }
            numTaggedFields9 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields9))
        }
    }
    if version <= 3 {
        // writing m.V3AndBelowTransactionalId: The transactional id corresponding to the transaction.
        if version >= 3 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*m.V3AndBelowTransactionalId) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.V3AndBelowTransactionalId)))
        }
        if m.V3AndBelowTransactionalId != nil {
            buff = append(buff, *m.V3AndBelowTransactionalId...)
        }
        // writing m.V3AndBelowProducerId: Current producer id in use by the transactional id.
        buff = binary.BigEndian.AppendUint64(buff, uint64(m.V3AndBelowProducerId))
        // writing m.V3AndBelowProducerEpoch: Current epoch associated with the producer id.
        buff = binary.BigEndian.AppendUint16(buff, uint16(m.V3AndBelowProducerEpoch))
        // writing m.V3AndBelowTopics: The partitions to add to the transaction.
        if version >= 3 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(m.V3AndBelowTopics) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.V3AndBelowTopics)))
        }
        for _, v3AndBelowTopics := range m.V3AndBelowTopics {
            // writing non tagged fields
            // writing v3AndBelowTopics.Name: The name of the topic.
            if version >= 3 {
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(*v3AndBelowTopics.Name) + 1))
            } else {
                // non flexible and non nullable
                buff = binary.BigEndian.AppendUint16(buff, uint16(len(*v3AndBelowTopics.Name)))
            }
            if v3AndBelowTopics.Name != nil {
                buff = append(buff, *v3AndBelowTopics.Name...)
            }
            // writing v3AndBelowTopics.Partitions: The partition indexes to add to the transaction
            if version >= 3 {
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(v3AndBelowTopics.Partitions) + 1))
            } else {
                // non flexible and non nullable
                buff = binary.BigEndian.AppendUint32(buff, uint32(len(v3AndBelowTopics.Partitions)))
            }
            for _, partitions := range v3AndBelowTopics.Partitions {
                buff = binary.BigEndian.AppendUint32(buff, uint32(partitions))
            }
            if version >= 3 {
                numTaggedFields16 := 0
                // write number of tagged fields
                buff = binary.AppendUvarint(buff, uint64(numTaggedFields16))
            }
        }
    }
    if version >= 3 {
        numTaggedFields17 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields17))
    }
    return buff
}

func (m *AddPartitionsToTxnRequest) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    if version >= 4 {
        // size for m.Transactions: List of transactions to add partitions to.
        // flexible and not nullable
        size += sizeofUvarint(len(m.Transactions) + 1)
        for _, transactions := range m.Transactions {
            size += 0 * int(unsafe.Sizeof(transactions)) // hack to make sure loop variable is always used
            // calculating size for non tagged fields
            numTaggedFields1:= 0
            numTaggedFields1 += 0
            // size for transactions.TransactionalId: The transactional id corresponding to the transaction.
            // flexible and not nullable
            size += sizeofUvarint(len(*transactions.TransactionalId) + 1)
            if transactions.TransactionalId != nil {
                size += len(*transactions.TransactionalId)
            }
            // size for transactions.ProducerId: Current producer id in use by the transactional id.
            size += 8
            // size for transactions.ProducerEpoch: Current epoch associated with the producer id.
            size += 2
            // size for transactions.VerifyOnly: Boolean to signify if we want to check if the partition is in the transaction rather than add it.
            size += 1
            // size for transactions.Topics: The partitions to add to the transaction.
            // flexible and not nullable
            size += sizeofUvarint(len(transactions.Topics) + 1)
            for _, topics := range transactions.Topics {
                size += 0 * int(unsafe.Sizeof(topics)) // hack to make sure loop variable is always used
                // calculating size for non tagged fields
                numTaggedFields2:= 0
                numTaggedFields2 += 0
                // size for topics.Name: The name of the topic.
                // flexible and not nullable
                size += sizeofUvarint(len(*topics.Name) + 1)
                if topics.Name != nil {
                    size += len(*topics.Name)
                }
                // size for topics.Partitions: The partition indexes to add to the transaction
                // flexible and not nullable
                size += sizeofUvarint(len(topics.Partitions) + 1)
                for _, partitions := range topics.Partitions {
                    size += 0 * int(unsafe.Sizeof(partitions)) // hack to make sure loop variable is always used
                    size += 4
                }
                numTaggedFields3:= 0
                numTaggedFields3 += 0
                // writing size of num tagged fields field
                size += sizeofUvarint(numTaggedFields3)
            }
            numTaggedFields4:= 0
            numTaggedFields4 += 0
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields4)
        }
    }
    if version <= 3 {
        // size for m.V3AndBelowTransactionalId: The transactional id corresponding to the transaction.
        if version >= 3 {
            // flexible and not nullable
            size += sizeofUvarint(len(*m.V3AndBelowTransactionalId) + 1)
        } else {
            // non flexible and non nullable
            size += 2
        }
        if m.V3AndBelowTransactionalId != nil {
            size += len(*m.V3AndBelowTransactionalId)
        }
        // size for m.V3AndBelowProducerId: Current producer id in use by the transactional id.
        size += 8
        // size for m.V3AndBelowProducerEpoch: Current epoch associated with the producer id.
        size += 2
        // size for m.V3AndBelowTopics: The partitions to add to the transaction.
        if version >= 3 {
            // flexible and not nullable
            size += sizeofUvarint(len(m.V3AndBelowTopics) + 1)
        } else {
            // non flexible and non nullable
            size += 4
        }
        for _, v3AndBelowTopics := range m.V3AndBelowTopics {
            size += 0 * int(unsafe.Sizeof(v3AndBelowTopics)) // hack to make sure loop variable is always used
            // calculating size for non tagged fields
            numTaggedFields5:= 0
            numTaggedFields5 += 0
            // size for v3AndBelowTopics.Name: The name of the topic.
            if version >= 3 {
                // flexible and not nullable
                size += sizeofUvarint(len(*v3AndBelowTopics.Name) + 1)
            } else {
                // non flexible and non nullable
                size += 2
            }
            if v3AndBelowTopics.Name != nil {
                size += len(*v3AndBelowTopics.Name)
            }
            // size for v3AndBelowTopics.Partitions: The partition indexes to add to the transaction
            if version >= 3 {
                // flexible and not nullable
                size += sizeofUvarint(len(v3AndBelowTopics.Partitions) + 1)
            } else {
                // non flexible and non nullable
                size += 4
            }
            for _, partitions := range v3AndBelowTopics.Partitions {
                size += 0 * int(unsafe.Sizeof(partitions)) // hack to make sure loop variable is always used
                size += 4
            }
            numTaggedFields6:= 0
            numTaggedFields6 += 0
            if version >= 3 {
                // writing size of num tagged fields field
                size += sizeofUvarint(numTaggedFields6)
            }
        }
    }
    numTaggedFields7:= 0
    numTaggedFields7 += 0
    if version >= 3 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields7)
    }
    return size, tagSizes
}

func (m *AddPartitionsToTxnRequest) HeaderVersions(version int16) (int16, int16) {
    if version >= 3 {
        return 2, 1
    } else {
        return 1, 0
    }
}

func (m *AddPartitionsToTxnRequest) SupportedApiVersions() (int16, int16) {
    return -1, -1
}
