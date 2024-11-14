// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"
import "unsafe"

type AddPartitionsToTxnResponseAddPartitionsToTxnTopicResult struct {
    // The topic name.
    Name *string
    // The results for each partition
    ResultsByPartition []AddPartitionsToTxnResponseAddPartitionsToTxnPartitionResult
}

type AddPartitionsToTxnResponseAddPartitionsToTxnPartitionResult struct {
    // The partition indexes.
    PartitionIndex int32
    // The response error code.
    PartitionErrorCode int16
}

type AddPartitionsToTxnResponseAddPartitionsToTxnResult struct {
    // The transactional id corresponding to the transaction.
    TransactionalId *string
    // The results for each topic.
    TopicResults []AddPartitionsToTxnResponseAddPartitionsToTxnTopicResult
}

type AddPartitionsToTxnResponse struct {
    // Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ThrottleTimeMs int32
    // The response top level error code.
    ErrorCode int16
    // Results categorized by transactional ID.
    ResultsByTransaction []AddPartitionsToTxnResponseAddPartitionsToTxnResult
    // The results for each topic.
    ResultsByTopicV3AndBelow []AddPartitionsToTxnResponseAddPartitionsToTxnTopicResult
}

func (m *AddPartitionsToTxnResponse) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.ThrottleTimeMs: Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        m.ThrottleTimeMs = int32(binary.BigEndian.Uint32(buff[offset:]))
        offset += 4
    }
    if version >= 4 {
        {
            // reading m.ErrorCode: The response top level error code.
            m.ErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
            offset += 2
        }
        {
            // reading m.ResultsByTransaction: Results categorized by transactional ID.
            var l0 int
            // flexible and not nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l0 = int(u - 1)
            if l0 >= 0 {
                // length will be -1 if field is null
                resultsByTransaction := make([]AddPartitionsToTxnResponseAddPartitionsToTxnResult, l0)
                for i0 := 0; i0 < l0; i0++ {
                    // reading non tagged fields
                    {
                        // reading resultsByTransaction[i0].TransactionalId: The transactional id corresponding to the transaction.
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l1 := int(u - 1)
                        s := string(buff[offset: offset + l1])
                        resultsByTransaction[i0].TransactionalId = &s
                        offset += l1
                    }
                    {
                        // reading resultsByTransaction[i0].TopicResults: The results for each topic.
                        var l2 int
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l2 = int(u - 1)
                        if l2 >= 0 {
                            // length will be -1 if field is null
                            topicResults := make([]AddPartitionsToTxnResponseAddPartitionsToTxnTopicResult, l2)
                            for i1 := 0; i1 < l2; i1++ {
                                // reading non tagged fields
                                {
                                    // reading topicResults[i1].Name: The topic name.
                                    // flexible and not nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l3 := int(u - 1)
                                    s := string(buff[offset: offset + l3])
                                    topicResults[i1].Name = &s
                                    offset += l3
                                }
                                {
                                    // reading topicResults[i1].ResultsByPartition: The results for each partition
                                    var l4 int
                                    // flexible and not nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l4 = int(u - 1)
                                    if l4 >= 0 {
                                        // length will be -1 if field is null
                                        resultsByPartition := make([]AddPartitionsToTxnResponseAddPartitionsToTxnPartitionResult, l4)
                                        for i2 := 0; i2 < l4; i2++ {
                                            // reading non tagged fields
                                            {
                                                // reading resultsByPartition[i2].PartitionIndex: The partition indexes.
                                                resultsByPartition[i2].PartitionIndex = int32(binary.BigEndian.Uint32(buff[offset:]))
                                                offset += 4
                                            }
                                            {
                                                // reading resultsByPartition[i2].PartitionErrorCode: The response error code.
                                                resultsByPartition[i2].PartitionErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
                                                offset += 2
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
                                    topicResults[i1].ResultsByPartition = resultsByPartition
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
                        resultsByTransaction[i0].TopicResults = topicResults
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
            m.ResultsByTransaction = resultsByTransaction
            }
        }
    }
    if version <= 3 {
        {
            // reading m.ResultsByTopicV3AndBelow: The results for each topic.
            var l5 int
            if version >= 3 {
                // flexible and not nullable
                u, n := binary.Uvarint(buff[offset:])
                offset += n
                l5 = int(u - 1)
            } else {
                // non flexible and non nullable
                l5 = int(binary.BigEndian.Uint32(buff[offset:]))
                offset += 4
            }
            if l5 >= 0 {
                // length will be -1 if field is null
                resultsByTopicV3AndBelow := make([]AddPartitionsToTxnResponseAddPartitionsToTxnTopicResult, l5)
                for i3 := 0; i3 < l5; i3++ {
                    // reading non tagged fields
                    {
                        // reading resultsByTopicV3AndBelow[i3].Name: The topic name.
                        if version >= 3 {
                            // flexible and not nullable
                            u, n := binary.Uvarint(buff[offset:])
                            offset += n
                            l6 := int(u - 1)
                            s := string(buff[offset: offset + l6])
                            resultsByTopicV3AndBelow[i3].Name = &s
                            offset += l6
                        } else {
                            // non flexible and non nullable
                            var l6 int
                            l6 = int(binary.BigEndian.Uint16(buff[offset:]))
                            offset += 2
                            s := string(buff[offset: offset + l6])
                            resultsByTopicV3AndBelow[i3].Name = &s
                            offset += l6
                        }
                    }
                    {
                        // reading resultsByTopicV3AndBelow[i3].ResultsByPartition: The results for each partition
                        var l7 int
                        if version >= 3 {
                            // flexible and not nullable
                            u, n := binary.Uvarint(buff[offset:])
                            offset += n
                            l7 = int(u - 1)
                        } else {
                            // non flexible and non nullable
                            l7 = int(binary.BigEndian.Uint32(buff[offset:]))
                            offset += 4
                        }
                        if l7 >= 0 {
                            // length will be -1 if field is null
                            resultsByPartition := make([]AddPartitionsToTxnResponseAddPartitionsToTxnPartitionResult, l7)
                            for i4 := 0; i4 < l7; i4++ {
                                // reading non tagged fields
                                {
                                    // reading resultsByPartition[i4].PartitionIndex: The partition indexes.
                                    resultsByPartition[i4].PartitionIndex = int32(binary.BigEndian.Uint32(buff[offset:]))
                                    offset += 4
                                }
                                {
                                    // reading resultsByPartition[i4].PartitionErrorCode: The response error code.
                                    resultsByPartition[i4].PartitionErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
                                    offset += 2
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
                        resultsByTopicV3AndBelow[i3].ResultsByPartition = resultsByPartition
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
            m.ResultsByTopicV3AndBelow = resultsByTopicV3AndBelow
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

func (m *AddPartitionsToTxnResponse) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.ThrottleTimeMs: Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    buff = binary.BigEndian.AppendUint32(buff, uint32(m.ThrottleTimeMs))
    if version >= 4 {
        // writing m.ErrorCode: The response top level error code.
        buff = binary.BigEndian.AppendUint16(buff, uint16(m.ErrorCode))
        // writing m.ResultsByTransaction: Results categorized by transactional ID.
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.ResultsByTransaction) + 1))
        for _, resultsByTransaction := range m.ResultsByTransaction {
            // writing non tagged fields
            // writing resultsByTransaction.TransactionalId: The transactional id corresponding to the transaction.
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*resultsByTransaction.TransactionalId) + 1))
            if resultsByTransaction.TransactionalId != nil {
                buff = append(buff, *resultsByTransaction.TransactionalId...)
            }
            // writing resultsByTransaction.TopicResults: The results for each topic.
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(resultsByTransaction.TopicResults) + 1))
            for _, topicResults := range resultsByTransaction.TopicResults {
                // writing non tagged fields
                // writing topicResults.Name: The topic name.
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(*topicResults.Name) + 1))
                if topicResults.Name != nil {
                    buff = append(buff, *topicResults.Name...)
                }
                // writing topicResults.ResultsByPartition: The results for each partition
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(topicResults.ResultsByPartition) + 1))
                for _, resultsByPartition := range topicResults.ResultsByPartition {
                    // writing non tagged fields
                    // writing resultsByPartition.PartitionIndex: The partition indexes.
                    buff = binary.BigEndian.AppendUint32(buff, uint32(resultsByPartition.PartitionIndex))
                    // writing resultsByPartition.PartitionErrorCode: The response error code.
                    buff = binary.BigEndian.AppendUint16(buff, uint16(resultsByPartition.PartitionErrorCode))
                    numTaggedFields9 := 0
                    // write number of tagged fields
                    buff = binary.AppendUvarint(buff, uint64(numTaggedFields9))
                }
                numTaggedFields10 := 0
                // write number of tagged fields
                buff = binary.AppendUvarint(buff, uint64(numTaggedFields10))
            }
            numTaggedFields11 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields11))
        }
    }
    if version <= 3 {
        // writing m.ResultsByTopicV3AndBelow: The results for each topic.
        if version >= 3 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(m.ResultsByTopicV3AndBelow) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.ResultsByTopicV3AndBelow)))
        }
        for _, resultsByTopicV3AndBelow := range m.ResultsByTopicV3AndBelow {
            // writing non tagged fields
            // writing resultsByTopicV3AndBelow.Name: The topic name.
            if version >= 3 {
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(*resultsByTopicV3AndBelow.Name) + 1))
            } else {
                // non flexible and non nullable
                buff = binary.BigEndian.AppendUint16(buff, uint16(len(*resultsByTopicV3AndBelow.Name)))
            }
            if resultsByTopicV3AndBelow.Name != nil {
                buff = append(buff, *resultsByTopicV3AndBelow.Name...)
            }
            // writing resultsByTopicV3AndBelow.ResultsByPartition: The results for each partition
            if version >= 3 {
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(resultsByTopicV3AndBelow.ResultsByPartition) + 1))
            } else {
                // non flexible and non nullable
                buff = binary.BigEndian.AppendUint32(buff, uint32(len(resultsByTopicV3AndBelow.ResultsByPartition)))
            }
            for _, resultsByPartition := range resultsByTopicV3AndBelow.ResultsByPartition {
                // writing non tagged fields
                // writing resultsByPartition.PartitionIndex: The partition indexes.
                buff = binary.BigEndian.AppendUint32(buff, uint32(resultsByPartition.PartitionIndex))
                // writing resultsByPartition.PartitionErrorCode: The response error code.
                buff = binary.BigEndian.AppendUint16(buff, uint16(resultsByPartition.PartitionErrorCode))
                if version >= 3 {
                    numTaggedFields17 := 0
                    // write number of tagged fields
                    buff = binary.AppendUvarint(buff, uint64(numTaggedFields17))
                }
            }
            if version >= 3 {
                numTaggedFields18 := 0
                // write number of tagged fields
                buff = binary.AppendUvarint(buff, uint64(numTaggedFields18))
            }
        }
    }
    if version >= 3 {
        numTaggedFields19 := 0
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields19))
    }
    return buff
}

func (m *AddPartitionsToTxnResponse) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.ThrottleTimeMs: Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    size += 4
    if version >= 4 {
        // size for m.ErrorCode: The response top level error code.
        size += 2
        // size for m.ResultsByTransaction: Results categorized by transactional ID.
        // flexible and not nullable
        size += sizeofUvarint(len(m.ResultsByTransaction) + 1)
        for _, resultsByTransaction := range m.ResultsByTransaction {
            size += 0 * int(unsafe.Sizeof(resultsByTransaction)) // hack to make sure loop variable is always used
            // calculating size for non tagged fields
            numTaggedFields1:= 0
            numTaggedFields1 += 0
            // size for resultsByTransaction.TransactionalId: The transactional id corresponding to the transaction.
            // flexible and not nullable
            size += sizeofUvarint(len(*resultsByTransaction.TransactionalId) + 1)
            if resultsByTransaction.TransactionalId != nil {
                size += len(*resultsByTransaction.TransactionalId)
            }
            // size for resultsByTransaction.TopicResults: The results for each topic.
            // flexible and not nullable
            size += sizeofUvarint(len(resultsByTransaction.TopicResults) + 1)
            for _, topicResults := range resultsByTransaction.TopicResults {
                size += 0 * int(unsafe.Sizeof(topicResults)) // hack to make sure loop variable is always used
                // calculating size for non tagged fields
                numTaggedFields2:= 0
                numTaggedFields2 += 0
                // size for topicResults.Name: The topic name.
                // flexible and not nullable
                size += sizeofUvarint(len(*topicResults.Name) + 1)
                if topicResults.Name != nil {
                    size += len(*topicResults.Name)
                }
                // size for topicResults.ResultsByPartition: The results for each partition
                // flexible and not nullable
                size += sizeofUvarint(len(topicResults.ResultsByPartition) + 1)
                for _, resultsByPartition := range topicResults.ResultsByPartition {
                    size += 0 * int(unsafe.Sizeof(resultsByPartition)) // hack to make sure loop variable is always used
                    // calculating size for non tagged fields
                    numTaggedFields3:= 0
                    numTaggedFields3 += 0
                    // size for resultsByPartition.PartitionIndex: The partition indexes.
                    size += 4
                    // size for resultsByPartition.PartitionErrorCode: The response error code.
                    size += 2
                    numTaggedFields4:= 0
                    numTaggedFields4 += 0
                    // writing size of num tagged fields field
                    size += sizeofUvarint(numTaggedFields4)
                }
                numTaggedFields5:= 0
                numTaggedFields5 += 0
                // writing size of num tagged fields field
                size += sizeofUvarint(numTaggedFields5)
            }
            numTaggedFields6:= 0
            numTaggedFields6 += 0
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields6)
        }
    }
    if version <= 3 {
        // size for m.ResultsByTopicV3AndBelow: The results for each topic.
        if version >= 3 {
            // flexible and not nullable
            size += sizeofUvarint(len(m.ResultsByTopicV3AndBelow) + 1)
        } else {
            // non flexible and non nullable
            size += 4
        }
        for _, resultsByTopicV3AndBelow := range m.ResultsByTopicV3AndBelow {
            size += 0 * int(unsafe.Sizeof(resultsByTopicV3AndBelow)) // hack to make sure loop variable is always used
            // calculating size for non tagged fields
            numTaggedFields7:= 0
            numTaggedFields7 += 0
            // size for resultsByTopicV3AndBelow.Name: The topic name.
            if version >= 3 {
                // flexible and not nullable
                size += sizeofUvarint(len(*resultsByTopicV3AndBelow.Name) + 1)
            } else {
                // non flexible and non nullable
                size += 2
            }
            if resultsByTopicV3AndBelow.Name != nil {
                size += len(*resultsByTopicV3AndBelow.Name)
            }
            // size for resultsByTopicV3AndBelow.ResultsByPartition: The results for each partition
            if version >= 3 {
                // flexible and not nullable
                size += sizeofUvarint(len(resultsByTopicV3AndBelow.ResultsByPartition) + 1)
            } else {
                // non flexible and non nullable
                size += 4
            }
            for _, resultsByPartition := range resultsByTopicV3AndBelow.ResultsByPartition {
                size += 0 * int(unsafe.Sizeof(resultsByPartition)) // hack to make sure loop variable is always used
                // calculating size for non tagged fields
                numTaggedFields8:= 0
                numTaggedFields8 += 0
                // size for resultsByPartition.PartitionIndex: The partition indexes.
                size += 4
                // size for resultsByPartition.PartitionErrorCode: The response error code.
                size += 2
                numTaggedFields9:= 0
                numTaggedFields9 += 0
                if version >= 3 {
                    // writing size of num tagged fields field
                    size += sizeofUvarint(numTaggedFields9)
                }
            }
            numTaggedFields10:= 0
            numTaggedFields10 += 0
            if version >= 3 {
                // writing size of num tagged fields field
                size += sizeofUvarint(numTaggedFields10)
            }
        }
    }
    numTaggedFields11:= 0
    numTaggedFields11 += 0
    if version >= 3 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields11)
    }
    return size, tagSizes
}


