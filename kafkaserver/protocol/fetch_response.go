// Package protocol - This is a generated file, please do not edit

package protocol

import "encoding/binary"
import "github.com/spirit-labs/tektite/common"
import "github.com/pkg/errors"
import "github.com/spirit-labs/tektite/debug"
import "fmt"
import "unsafe"

type FetchResponseEpochEndOffset struct {
    Epoch int32
    EndOffset int64
}

type FetchResponseLeaderIdAndEpoch struct {
    // The ID of the current leader or -1 if the leader is unknown.
    LeaderId int32
    // The latest known leader epoch
    LeaderEpoch int32
}

type FetchResponseSnapshotId struct {
    EndOffset int64
    Epoch int32
}

type FetchResponseAbortedTransaction struct {
    // The producer id associated with the aborted transaction.
    ProducerId int64
    // The first offset in the aborted transaction.
    FirstOffset int64
}

type FetchResponsePartitionData struct {
    // The partition index.
    PartitionIndex int32
    // The error code, or 0 if there was no fetch error.
    ErrorCode int16
    // The current high water mark.
    HighWatermark int64
    // The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED)
    LastStableOffset int64
    // The current log start offset.
    LogStartOffset int64
    // In case divergence is detected based on the `LastFetchedEpoch` and `FetchOffset` in the request, this field indicates the largest epoch and its end offset such that subsequent records are known to diverge
    DivergingEpoch FetchResponseEpochEndOffset
    CurrentLeader FetchResponseLeaderIdAndEpoch
    // In the case of fetching an offset less than the LogStartOffset, this is the end offset and epoch that should be used in the FetchSnapshot request.
    SnapshotId FetchResponseSnapshotId
    // The aborted transactions.
    AbortedTransactions []FetchResponseAbortedTransaction
    // The preferred read replica for the consumer to use on its next fetch request
    PreferredReadReplica int32
    // The record data.
    Records [][]byte
}

type FetchResponseFetchableTopicResponse struct {
    // The topic name.
    Topic *string
    // The unique topic ID
    TopicId []byte
    // The topic partitions.
    Partitions []FetchResponsePartitionData
}

type FetchResponseNodeEndpoint struct {
    // The ID of the associated node.
    NodeId int32
    // The node's hostname.
    Host *string
    // The node's port.
    Port int32
    // The rack of the node, or null if it has not been assigned to a rack.
    Rack *string
}

type FetchResponse struct {
    // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ThrottleTimeMs int32
    // The top level response error code.
    ErrorCode int16
    // The fetch session ID, or 0 if this is not part of a fetch session.
    SessionId int32
    // The response topics.
    Responses []FetchResponseFetchableTopicResponse
    // Endpoints for all current-leaders enumerated in PartitionData, with errors NOT_LEADER_OR_FOLLOWER & FENCED_LEADER_EPOCH.
    NodeEndpoints []FetchResponseNodeEndpoint
}

func (m *FetchResponse) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    if version >= 1 {
        {
            // reading m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
            m.ThrottleTimeMs = int32(binary.BigEndian.Uint32(buff[offset:]))
            offset += 4
        }
    }
    if version >= 7 {
        {
            // reading m.ErrorCode: The top level response error code.
            m.ErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
            offset += 2
        }
        {
            // reading m.SessionId: The fetch session ID, or 0 if this is not part of a fetch session.
            m.SessionId = int32(binary.BigEndian.Uint32(buff[offset:]))
            offset += 4
        }
    }
    {
        // reading m.Responses: The response topics.
        var l0 int
        if version >= 12 {
            // flexible and not nullable
            u, n := binary.Uvarint(buff[offset:])
            offset += n
            l0 = int(u - 1)
        } else {
            // non flexible and non nullable
            l0 = int(binary.BigEndian.Uint32(buff[offset:]))
            offset += 4
        }
        if l0 >= 0 {
            // length will be -1 if field is null
            responses := make([]FetchResponseFetchableTopicResponse, l0)
            for i0 := 0; i0 < l0; i0++ {
                // reading non tagged fields
                if version <= 12 {
                    {
                        // reading responses[i0].Topic: The topic name.
                        if version >= 12 {
                            // flexible and not nullable
                            u, n := binary.Uvarint(buff[offset:])
                            offset += n
                            l1 := int(u - 1)
                            s := string(buff[offset: offset + l1])
                            responses[i0].Topic = &s
                            offset += l1
                        } else {
                            // non flexible and non nullable
                            var l1 int
                            l1 = int(binary.BigEndian.Uint16(buff[offset:]))
                            offset += 2
                            s := string(buff[offset: offset + l1])
                            responses[i0].Topic = &s
                            offset += l1
                        }
                    }
                }
                if version >= 13 {
                    {
                        // reading responses[i0].TopicId: The unique topic ID
                        responses[i0].TopicId = common.ByteSliceCopy(buff[offset: offset + 16])
                        offset += 16
                    }
                }
                {
                    // reading responses[i0].Partitions: The topic partitions.
                    var l2 int
                    if version >= 12 {
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l2 = int(u - 1)
                    } else {
                        // non flexible and non nullable
                        l2 = int(binary.BigEndian.Uint32(buff[offset:]))
                        offset += 4
                    }
                    if l2 >= 0 {
                        // length will be -1 if field is null
                        partitions := make([]FetchResponsePartitionData, l2)
                        for i1 := 0; i1 < l2; i1++ {
                            // reading non tagged fields
                            {
                                // reading partitions[i1].PartitionIndex: The partition index.
                                partitions[i1].PartitionIndex = int32(binary.BigEndian.Uint32(buff[offset:]))
                                offset += 4
                            }
                            {
                                // reading partitions[i1].ErrorCode: The error code, or 0 if there was no fetch error.
                                partitions[i1].ErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
                                offset += 2
                            }
                            {
                                // reading partitions[i1].HighWatermark: The current high water mark.
                                partitions[i1].HighWatermark = int64(binary.BigEndian.Uint64(buff[offset:]))
                                offset += 8
                            }
                            if version >= 4 {
                                {
                                    // reading partitions[i1].LastStableOffset: The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED)
                                    partitions[i1].LastStableOffset = int64(binary.BigEndian.Uint64(buff[offset:]))
                                    offset += 8
                                }
                            }
                            if version >= 5 {
                                {
                                    // reading partitions[i1].LogStartOffset: The current log start offset.
                                    partitions[i1].LogStartOffset = int64(binary.BigEndian.Uint64(buff[offset:]))
                                    offset += 8
                                }
                            }
                            if version >= 4 {
                                {
                                    // reading partitions[i1].AbortedTransactions: The aborted transactions.
                                    var l3 int
                                    if version >= 12 {
                                        // flexible and nullable
                                        u, n := binary.Uvarint(buff[offset:])
                                        offset += n
                                        l3 = int(u - 1)
                                    } else {
                                        // non flexible and nullable
                                        l3 = int(int32(binary.BigEndian.Uint32(buff[offset:])))
                                        offset += 4
                                    }
                                    if l3 >= 0 {
                                        // length will be -1 if field is null
                                        abortedTransactions := make([]FetchResponseAbortedTransaction, l3)
                                        for i2 := 0; i2 < l3; i2++ {
                                            // reading non tagged fields
                                            {
                                                // reading abortedTransactions[i2].ProducerId: The producer id associated with the aborted transaction.
                                                abortedTransactions[i2].ProducerId = int64(binary.BigEndian.Uint64(buff[offset:]))
                                                offset += 8
                                            }
                                            {
                                                // reading abortedTransactions[i2].FirstOffset: The first offset in the aborted transaction.
                                                abortedTransactions[i2].FirstOffset = int64(binary.BigEndian.Uint64(buff[offset:]))
                                                offset += 8
                                            }
                                            if version >= 12 {
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
                                    partitions[i1].AbortedTransactions = abortedTransactions
                                    }
                                }
                            }
                            if version >= 11 {
                                {
                                    // reading partitions[i1].PreferredReadReplica: The preferred read replica for the consumer to use on its next fetch request
                                    partitions[i1].PreferredReadReplica = int32(binary.BigEndian.Uint32(buff[offset:]))
                                    offset += 4
                                }
                            }
                            {
                                // reading partitions[i1].Records: The record data.
                                if version >= 12 {
                                    // flexible and nullable
                                    u, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    l4 := int(u - 1)
                                    if l4 > 0 {
                                        partitions[i1].Records = [][]byte{common.ByteSliceCopy(buff[offset: offset + l4])}
                                        offset += l4
                                    } else {
                                        partitions[i1].Records = nil
                                    }
                                } else {
                                    // non flexible and nullable
                                    var l4 int
                                    l4 = int(int32(binary.BigEndian.Uint32(buff[offset:])))
                                    offset += 4
                                    if l4 > 0 {
                                        partitions[i1].Records = [][]byte{common.ByteSliceCopy(buff[offset: offset + l4])}
                                        offset += l4
                                    } else {
                                        partitions[i1].Records = nil
                                    }
                                }
                            }
                            if version >= 12 {
                                // reading tagged fields
                                nt, n := binary.Uvarint(buff[offset:])
                                offset += n
                                for i := 0; i < int(nt); i++ {
                                    t, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    ts, n := binary.Uvarint(buff[offset:])
                                    offset += n
                                    switch t {
                                        case 0:
                                            {
                                                // reading partitions[i1].DivergingEpoch: In case divergence is detected based on the `LastFetchedEpoch` and `FetchOffset` in the request, this field indicates the largest epoch and its end offset such that subsequent records are known to diverge
                                                {
                                                    // reading non tagged fields
                                                    {
                                                        // reading partitions[i1].DivergingEpoch.Epoch: 
                                                        partitions[i1].DivergingEpoch.Epoch = int32(binary.BigEndian.Uint32(buff[offset:]))
                                                        offset += 4
                                                    }
                                                    {
                                                        // reading partitions[i1].DivergingEpoch.EndOffset: 
                                                        partitions[i1].DivergingEpoch.EndOffset = int64(binary.BigEndian.Uint64(buff[offset:]))
                                                        offset += 8
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
                                            }
                                        case 1:
                                            {
                                                // reading partitions[i1].CurrentLeader: 
                                                {
                                                    // reading non tagged fields
                                                    {
                                                        // reading partitions[i1].CurrentLeader.LeaderId: The ID of the current leader or -1 if the leader is unknown.
                                                        partitions[i1].CurrentLeader.LeaderId = int32(binary.BigEndian.Uint32(buff[offset:]))
                                                        offset += 4
                                                    }
                                                    {
                                                        // reading partitions[i1].CurrentLeader.LeaderEpoch: The latest known leader epoch
                                                        partitions[i1].CurrentLeader.LeaderEpoch = int32(binary.BigEndian.Uint32(buff[offset:]))
                                                        offset += 4
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
                                            }
                                        case 2:
                                            {
                                                // reading partitions[i1].SnapshotId: In the case of fetching an offset less than the LogStartOffset, this is the end offset and epoch that should be used in the FetchSnapshot request.
                                                {
                                                    // reading non tagged fields
                                                    {
                                                        // reading partitions[i1].SnapshotId.EndOffset: 
                                                        partitions[i1].SnapshotId.EndOffset = int64(binary.BigEndian.Uint64(buff[offset:]))
                                                        offset += 8
                                                    }
                                                    {
                                                        // reading partitions[i1].SnapshotId.Epoch: 
                                                        partitions[i1].SnapshotId.Epoch = int32(binary.BigEndian.Uint32(buff[offset:]))
                                                        offset += 4
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
                                            }
                                        default:
                                            offset += int(ts)
                                    }
                                }
                            }
                        }
                    responses[i0].Partitions = partitions
                    }
                }
                if version >= 12 {
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
        m.Responses = responses
        }
    }
    if version >= 12 {
        // reading tagged fields
        nt, n := binary.Uvarint(buff[offset:])
        offset += n
        for i := 0; i < int(nt); i++ {
            t, n := binary.Uvarint(buff[offset:])
            offset += n
            ts, n := binary.Uvarint(buff[offset:])
            offset += n
            switch t {
                case 0:
                    if version >= 16 {
                        {
                            // reading m.NodeEndpoints: Endpoints for all current-leaders enumerated in PartitionData, with errors NOT_LEADER_OR_FOLLOWER & FENCED_LEADER_EPOCH.
                            var l5 int
                            // flexible and not nullable
                            u, n := binary.Uvarint(buff[offset:])
                            offset += n
                            l5 = int(u - 1)
                            if l5 >= 0 {
                                // length will be -1 if field is null
                                nodeEndpoints := make([]FetchResponseNodeEndpoint, l5)
                                for i3 := 0; i3 < l5; i3++ {
                                    // reading non tagged fields
                                    {
                                        // reading nodeEndpoints[i3].NodeId: The ID of the associated node.
                                        nodeEndpoints[i3].NodeId = int32(binary.BigEndian.Uint32(buff[offset:]))
                                        offset += 4
                                    }
                                    {
                                        // reading nodeEndpoints[i3].Host: The node's hostname.
                                        // flexible and not nullable
                                        u, n := binary.Uvarint(buff[offset:])
                                        offset += n
                                        l6 := int(u - 1)
                                        s := string(buff[offset: offset + l6])
                                        nodeEndpoints[i3].Host = &s
                                        offset += l6
                                    }
                                    {
                                        // reading nodeEndpoints[i3].Port: The node's port.
                                        nodeEndpoints[i3].Port = int32(binary.BigEndian.Uint32(buff[offset:]))
                                        offset += 4
                                    }
                                    {
                                        // reading nodeEndpoints[i3].Rack: The rack of the node, or null if it has not been assigned to a rack.
                                        // flexible and nullable
                                        u, n := binary.Uvarint(buff[offset:])
                                        offset += n
                                        l7 := int(u - 1)
                                        if l7 > 0 {
                                            s := string(buff[offset: offset + l7])
                                            nodeEndpoints[i3].Rack = &s
                                            offset += l7
                                        } else {
                                            nodeEndpoints[i3].Rack = nil
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
                            m.NodeEndpoints = nodeEndpoints
                            }
                        }
                    } else {
                        return 0, errors.Errorf("Tag 0 is not valid at version %d", version)
                    }
                default:
                    offset += int(ts)
            }
        }
    }
    return offset, nil
}

func (m *FetchResponse) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    if version >= 1 {
        // writing m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        buff = binary.BigEndian.AppendUint32(buff, uint32(m.ThrottleTimeMs))
    }
    if version >= 7 {
        // writing m.ErrorCode: The top level response error code.
        buff = binary.BigEndian.AppendUint16(buff, uint16(m.ErrorCode))
        // writing m.SessionId: The fetch session ID, or 0 if this is not part of a fetch session.
        buff = binary.BigEndian.AppendUint32(buff, uint32(m.SessionId))
    }
    // writing m.Responses: The response topics.
    if version >= 12 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.Responses) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.Responses)))
    }
    for _, responses := range m.Responses {
        // writing non tagged fields
        if version <= 12 {
            // writing responses.Topic: The topic name.
            if version >= 12 {
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(*responses.Topic) + 1))
            } else {
                // non flexible and non nullable
                buff = binary.BigEndian.AppendUint16(buff, uint16(len(*responses.Topic)))
            }
            if responses.Topic != nil {
                buff = append(buff, *responses.Topic...)
            }
        }
        if version >= 13 {
            // writing responses.TopicId: The unique topic ID
            if responses.TopicId != nil {
                buff = append(buff, responses.TopicId...)
            }
        }
        // writing responses.Partitions: The topic partitions.
        if version >= 12 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(responses.Partitions) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint32(buff, uint32(len(responses.Partitions)))
        }
        for _, partitions := range responses.Partitions {
            // writing non tagged fields
            // writing partitions.PartitionIndex: The partition index.
            buff = binary.BigEndian.AppendUint32(buff, uint32(partitions.PartitionIndex))
            // writing partitions.ErrorCode: The error code, or 0 if there was no fetch error.
            buff = binary.BigEndian.AppendUint16(buff, uint16(partitions.ErrorCode))
            // writing partitions.HighWatermark: The current high water mark.
            buff = binary.BigEndian.AppendUint64(buff, uint64(partitions.HighWatermark))
            if version >= 4 {
                // writing partitions.LastStableOffset: The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED)
                buff = binary.BigEndian.AppendUint64(buff, uint64(partitions.LastStableOffset))
            }
            if version >= 5 {
                // writing partitions.LogStartOffset: The current log start offset.
                buff = binary.BigEndian.AppendUint64(buff, uint64(partitions.LogStartOffset))
            }
            if version >= 4 {
                // writing partitions.AbortedTransactions: The aborted transactions.
                if version >= 12 {
                    // flexible and nullable
                    if partitions.AbortedTransactions == nil {
                        // null
                        buff = append(buff, 0)
                    } else {
                        // not null
                        buff = binary.AppendUvarint(buff, uint64(len(partitions.AbortedTransactions) + 1))
                    }
                } else {
                    // non flexible and nullable
                    if partitions.AbortedTransactions == nil {
                        // null
                        buff = binary.BigEndian.AppendUint32(buff, 4294967295)
                    } else {
                        // not null
                        buff = binary.BigEndian.AppendUint32(buff, uint32(len(partitions.AbortedTransactions)))
                    }
                }
                for _, abortedTransactions := range partitions.AbortedTransactions {
                    // writing non tagged fields
                    // writing abortedTransactions.ProducerId: The producer id associated with the aborted transaction.
                    buff = binary.BigEndian.AppendUint64(buff, uint64(abortedTransactions.ProducerId))
                    // writing abortedTransactions.FirstOffset: The first offset in the aborted transaction.
                    buff = binary.BigEndian.AppendUint64(buff, uint64(abortedTransactions.FirstOffset))
                    if version >= 12 {
                        numTaggedFields15 := 0
                        // write number of tagged fields
                        buff = binary.AppendUvarint(buff, uint64(numTaggedFields15))
                    }
                }
            }
            if version >= 11 {
                // writing partitions.PreferredReadReplica: The preferred read replica for the consumer to use on its next fetch request
                buff = binary.BigEndian.AppendUint32(buff, uint32(partitions.PreferredReadReplica))
            }
            // writing partitions.Records: The record data.
            if version >= 12 {
                // flexible and nullable
                if partitions.Records == nil {
                    // null
                    buff = append(buff, 0)
                } else {
                    // not null
                    recordsTotSize18 := 0
                    for _, rec := range partitions.Records {
                        recordsTotSize18 += len(rec)
                    }
                    buff = binary.AppendUvarint(buff, uint64(recordsTotSize18 + 1))
                }
            } else {
                // non flexible and nullable
                if partitions.Records == nil {
                    // null
                    buff = binary.BigEndian.AppendUint32(buff, 4294967295)
                } else {
                    // not null
                    recordsTotSize19 := 0
                    for _, rec := range partitions.Records {
                        recordsTotSize19 += len(rec)
                    }
                    buff = binary.BigEndian.AppendUint32(buff, uint32(recordsTotSize19))
                }
            }
            for _, rec := range partitions.Records {
                buff = append(buff, rec...)
            }
            if version >= 12 {
                numTaggedFields20 := 0
                // writing tagged field increments
                // tagged field - partitions.DivergingEpoch: In case divergence is detected based on the `LastFetchedEpoch` and `FetchOffset` in the request, this field indicates the largest epoch and its end offset such that subsequent records are known to diverge
                numTaggedFields20++
                // tagged field - partitions.CurrentLeader: 
                numTaggedFields20++
                // tagged field - partitions.SnapshotId: In the case of fetching an offset less than the LogStartOffset, this is the end offset and epoch that should be used in the FetchSnapshot request.
                numTaggedFields20++
                // write number of tagged fields
                buff = binary.AppendUvarint(buff, uint64(numTaggedFields20))
                // writing tagged fields
                // tag header
                buff = binary.AppendUvarint(buff, uint64(0))
                buff = binary.AppendUvarint(buff, uint64(tagSizes[tagPos]))
                tagPos++
                var tagSizeStart21 int
                if debug.SanityChecks {
                    tagSizeStart21 = len(buff)
                }
                // writing partitions.DivergingEpoch: In case divergence is detected based on the `LastFetchedEpoch` and `FetchOffset` in the request, this field indicates the largest epoch and its end offset such that subsequent records are known to diverge
                {
                    // writing non tagged fields
                    // writing partitions.DivergingEpoch.Epoch: 
                    buff = binary.BigEndian.AppendUint32(buff, uint32(partitions.DivergingEpoch.Epoch))
                    // writing partitions.DivergingEpoch.EndOffset: 
                    buff = binary.BigEndian.AppendUint64(buff, uint64(partitions.DivergingEpoch.EndOffset))
                    numTaggedFields24 := 0
                    // write number of tagged fields
                    buff = binary.AppendUvarint(buff, uint64(numTaggedFields24))
                }
                if debug.SanityChecks && len(buff) - tagSizeStart21 != tagSizes[tagPos - 1] {
                    panic(fmt.Sprintf("incorrect calculated tag size for tag %d", 0))
                }
                // tag header
                buff = binary.AppendUvarint(buff, uint64(1))
                buff = binary.AppendUvarint(buff, uint64(tagSizes[tagPos]))
                tagPos++
                var tagSizeStart25 int
                if debug.SanityChecks {
                    tagSizeStart25 = len(buff)
                }
                // writing partitions.CurrentLeader: 
                {
                    // writing non tagged fields
                    // writing partitions.CurrentLeader.LeaderId: The ID of the current leader or -1 if the leader is unknown.
                    buff = binary.BigEndian.AppendUint32(buff, uint32(partitions.CurrentLeader.LeaderId))
                    // writing partitions.CurrentLeader.LeaderEpoch: The latest known leader epoch
                    buff = binary.BigEndian.AppendUint32(buff, uint32(partitions.CurrentLeader.LeaderEpoch))
                    numTaggedFields28 := 0
                    // write number of tagged fields
                    buff = binary.AppendUvarint(buff, uint64(numTaggedFields28))
                }
                if debug.SanityChecks && len(buff) - tagSizeStart25 != tagSizes[tagPos - 1] {
                    panic(fmt.Sprintf("incorrect calculated tag size for tag %d", 1))
                }
                // tag header
                buff = binary.AppendUvarint(buff, uint64(2))
                buff = binary.AppendUvarint(buff, uint64(tagSizes[tagPos]))
                tagPos++
                var tagSizeStart29 int
                if debug.SanityChecks {
                    tagSizeStart29 = len(buff)
                }
                // writing partitions.SnapshotId: In the case of fetching an offset less than the LogStartOffset, this is the end offset and epoch that should be used in the FetchSnapshot request.
                {
                    // writing non tagged fields
                    // writing partitions.SnapshotId.EndOffset: 
                    buff = binary.BigEndian.AppendUint64(buff, uint64(partitions.SnapshotId.EndOffset))
                    // writing partitions.SnapshotId.Epoch: 
                    buff = binary.BigEndian.AppendUint32(buff, uint32(partitions.SnapshotId.Epoch))
                    numTaggedFields32 := 0
                    // write number of tagged fields
                    buff = binary.AppendUvarint(buff, uint64(numTaggedFields32))
                }
                if debug.SanityChecks && len(buff) - tagSizeStart29 != tagSizes[tagPos - 1] {
                    panic(fmt.Sprintf("incorrect calculated tag size for tag %d", 2))
                }
            }
        }
        if version >= 12 {
            numTaggedFields33 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields33))
        }
    }
    if version >= 12 {
        numTaggedFields34 := 0
        // writing tagged field increments
        if version >= 16 {
            // tagged field - m.NodeEndpoints: Endpoints for all current-leaders enumerated in PartitionData, with errors NOT_LEADER_OR_FOLLOWER & FENCED_LEADER_EPOCH.
            numTaggedFields34++
        }
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields34))
        // writing tagged fields
        if version >= 16 {
            // tag header
            buff = binary.AppendUvarint(buff, uint64(0))
            buff = binary.AppendUvarint(buff, uint64(tagSizes[tagPos]))
            tagPos++
            var tagSizeStart35 int
            if debug.SanityChecks {
                tagSizeStart35 = len(buff)
            }
            // writing m.NodeEndpoints: Endpoints for all current-leaders enumerated in PartitionData, with errors NOT_LEADER_OR_FOLLOWER & FENCED_LEADER_EPOCH.
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(m.NodeEndpoints) + 1))
            for _, nodeEndpoints := range m.NodeEndpoints {
                // writing non tagged fields
                // writing nodeEndpoints.NodeId: The ID of the associated node.
                buff = binary.BigEndian.AppendUint32(buff, uint32(nodeEndpoints.NodeId))
                // writing nodeEndpoints.Host: The node's hostname.
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(*nodeEndpoints.Host) + 1))
                if nodeEndpoints.Host != nil {
                    buff = append(buff, *nodeEndpoints.Host...)
                }
                // writing nodeEndpoints.Port: The node's port.
                buff = binary.BigEndian.AppendUint32(buff, uint32(nodeEndpoints.Port))
                // writing nodeEndpoints.Rack: The rack of the node, or null if it has not been assigned to a rack.
                // flexible and nullable
                if nodeEndpoints.Rack == nil {
                    // null
                    buff = append(buff, 0)
                } else {
                    // not null
                    buff = binary.AppendUvarint(buff, uint64(len(*nodeEndpoints.Rack) + 1))
                }
                if nodeEndpoints.Rack != nil {
                    buff = append(buff, *nodeEndpoints.Rack...)
                }
                numTaggedFields40 := 0
                // write number of tagged fields
                buff = binary.AppendUvarint(buff, uint64(numTaggedFields40))
            }
            if debug.SanityChecks && len(buff) - tagSizeStart35 != tagSizes[tagPos - 1] {
                panic(fmt.Sprintf("incorrect calculated tag size for tag %d", 0))
            }
        }
    }
    return buff
}

func (m *FetchResponse) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    if version >= 1 {
        // size for m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        size += 4
    }
    if version >= 7 {
        // size for m.ErrorCode: The top level response error code.
        size += 2
        // size for m.SessionId: The fetch session ID, or 0 if this is not part of a fetch session.
        size += 4
    }
    // size for m.Responses: The response topics.
    if version >= 12 {
        // flexible and not nullable
        size += sizeofUvarint(len(m.Responses) + 1)
    } else {
        // non flexible and non nullable
        size += 4
    }
    for _, responses := range m.Responses {
        size += 0 * int(unsafe.Sizeof(responses)) // hack to make sure loop variable is always used
        // calculating size for non tagged fields
        numTaggedFields1:= 0
        numTaggedFields1 += 0
        if version <= 12 {
            // size for responses.Topic: The topic name.
            if version >= 12 {
                // flexible and not nullable
                size += sizeofUvarint(len(*responses.Topic) + 1)
            } else {
                // non flexible and non nullable
                size += 2
            }
            if responses.Topic != nil {
                size += len(*responses.Topic)
            }
        }
        if version >= 13 {
            // size for responses.TopicId: The unique topic ID
            size += 16
        }
        // size for responses.Partitions: The topic partitions.
        if version >= 12 {
            // flexible and not nullable
            size += sizeofUvarint(len(responses.Partitions) + 1)
        } else {
            // non flexible and non nullable
            size += 4
        }
        for _, partitions := range responses.Partitions {
            size += 0 * int(unsafe.Sizeof(partitions)) // hack to make sure loop variable is always used
            // calculating size for non tagged fields
            numTaggedFields2:= 0
            numTaggedFields2 += 0
            // size for partitions.PartitionIndex: The partition index.
            size += 4
            // size for partitions.ErrorCode: The error code, or 0 if there was no fetch error.
            size += 2
            // size for partitions.HighWatermark: The current high water mark.
            size += 8
            if version >= 4 {
                // size for partitions.LastStableOffset: The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED)
                size += 8
            }
            if version >= 5 {
                // size for partitions.LogStartOffset: The current log start offset.
                size += 8
            }
            if version >= 4 {
                // size for partitions.AbortedTransactions: The aborted transactions.
                if version >= 12 {
                    // flexible and nullable
                    if partitions.AbortedTransactions == nil {
                        // null
                        size += 1
                    } else {
                        // not null
                        size += sizeofUvarint(len(partitions.AbortedTransactions) + 1)
                    }
                } else {
                    // non flexible and nullable
                    size += 4
                }
                for _, abortedTransactions := range partitions.AbortedTransactions {
                    size += 0 * int(unsafe.Sizeof(abortedTransactions)) // hack to make sure loop variable is always used
                    // calculating size for non tagged fields
                    numTaggedFields3:= 0
                    numTaggedFields3 += 0
                    // size for abortedTransactions.ProducerId: The producer id associated with the aborted transaction.
                    size += 8
                    // size for abortedTransactions.FirstOffset: The first offset in the aborted transaction.
                    size += 8
                    numTaggedFields4:= 0
                    numTaggedFields4 += 0
                    if version >= 12 {
                        // writing size of num tagged fields field
                        size += sizeofUvarint(numTaggedFields4)
                    }
                }
            }
            if version >= 11 {
                // size for partitions.PreferredReadReplica: The preferred read replica for the consumer to use on its next fetch request
                size += 4
            }
            // size for partitions.Records: The record data.
            if version >= 12 {
                // flexible and nullable
                if partitions.Records == nil {
                    // null
                    size += 1
                } else {
                    // not null
                    recordsTotSize5 := 0
                    for _, rec := range partitions.Records {
                        recordsTotSize5 += len(rec)
                    }
                    size += sizeofUvarint(recordsTotSize5 + 1)
                }
            } else {
                // non flexible and nullable
                size += 4
            }
            for _, rec := range partitions.Records {
                size += len(rec)
            }
            numTaggedFields6:= 0
            numTaggedFields6 += 0
            taggedFieldStart := 0
            taggedFieldSize := 0
            if version >= 12 {
                // size for partitions.DivergingEpoch: In case divergence is detected based on the `LastFetchedEpoch` and `FetchOffset` in the request, this field indicates the largest epoch and its end offset such that subsequent records are known to diverge
                numTaggedFields6++
                taggedFieldStart = size
                {
                    // calculating size for non tagged fields
                    numTaggedFields7:= 0
                    numTaggedFields7 += 0
                    // size for partitions.DivergingEpoch.Epoch: 
                    size += 4
                    // size for partitions.DivergingEpoch.EndOffset: 
                    size += 8
                    numTaggedFields8:= 0
                    numTaggedFields8 += 0
                    // writing size of num tagged fields field
                    size += sizeofUvarint(numTaggedFields8)
                }
                taggedFieldSize = size - taggedFieldStart
                tagSizes = append(tagSizes, taggedFieldSize)
                // size = <tag id contrib> + <field size>
                size += sizeofUvarint(0) + sizeofUvarint(taggedFieldSize)
                // size for partitions.CurrentLeader: 
                numTaggedFields6++
                taggedFieldStart = size
                {
                    // calculating size for non tagged fields
                    numTaggedFields9:= 0
                    numTaggedFields9 += 0
                    // size for partitions.CurrentLeader.LeaderId: The ID of the current leader or -1 if the leader is unknown.
                    size += 4
                    // size for partitions.CurrentLeader.LeaderEpoch: The latest known leader epoch
                    size += 4
                    numTaggedFields10:= 0
                    numTaggedFields10 += 0
                    // writing size of num tagged fields field
                    size += sizeofUvarint(numTaggedFields10)
                }
                taggedFieldSize = size - taggedFieldStart
                tagSizes = append(tagSizes, taggedFieldSize)
                // size = <tag id contrib> + <field size>
                size += sizeofUvarint(1) + sizeofUvarint(taggedFieldSize)
                // size for partitions.SnapshotId: In the case of fetching an offset less than the LogStartOffset, this is the end offset and epoch that should be used in the FetchSnapshot request.
                numTaggedFields6++
                taggedFieldStart = size
                {
                    // calculating size for non tagged fields
                    numTaggedFields11:= 0
                    numTaggedFields11 += 0
                    // size for partitions.SnapshotId.EndOffset: 
                    size += 8
                    // size for partitions.SnapshotId.Epoch: 
                    size += 4
                    numTaggedFields12:= 0
                    numTaggedFields12 += 0
                    // writing size of num tagged fields field
                    size += sizeofUvarint(numTaggedFields12)
                }
                taggedFieldSize = size - taggedFieldStart
                tagSizes = append(tagSizes, taggedFieldSize)
                // size = <tag id contrib> + <field size>
                size += sizeofUvarint(2) + sizeofUvarint(taggedFieldSize)
            }
            if version >= 12 {
                // writing size of num tagged fields field
                size += sizeofUvarint(numTaggedFields6)
            }
        }
        numTaggedFields13:= 0
        numTaggedFields13 += 0
        if version >= 12 {
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields13)
        }
    }
    numTaggedFields14:= 0
    numTaggedFields14 += 0
    taggedFieldStart := 0
    taggedFieldSize := 0
    if version >= 16 {
        // size for m.NodeEndpoints: Endpoints for all current-leaders enumerated in PartitionData, with errors NOT_LEADER_OR_FOLLOWER & FENCED_LEADER_EPOCH.
        numTaggedFields14++
        taggedFieldStart = size
        // flexible and not nullable
        size += sizeofUvarint(len(m.NodeEndpoints) + 1)
        for _, nodeEndpoints := range m.NodeEndpoints {
            size += 0 * int(unsafe.Sizeof(nodeEndpoints)) // hack to make sure loop variable is always used
            // calculating size for non tagged fields
            numTaggedFields15:= 0
            numTaggedFields15 += 0
            // size for nodeEndpoints.NodeId: The ID of the associated node.
            size += 4
            // size for nodeEndpoints.Host: The node's hostname.
            // flexible and not nullable
            size += sizeofUvarint(len(*nodeEndpoints.Host) + 1)
            if nodeEndpoints.Host != nil {
                size += len(*nodeEndpoints.Host)
            }
            // size for nodeEndpoints.Port: The node's port.
            size += 4
            // size for nodeEndpoints.Rack: The rack of the node, or null if it has not been assigned to a rack.
            // flexible and nullable
            if nodeEndpoints.Rack == nil {
                // null
                size += 1
            } else {
                // not null
                size += sizeofUvarint(len(*nodeEndpoints.Rack) + 1)
            }
            if nodeEndpoints.Rack != nil {
                size += len(*nodeEndpoints.Rack)
            }
            numTaggedFields16:= 0
            numTaggedFields16 += 0
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields16)
        }
        taggedFieldSize = size - taggedFieldStart
        tagSizes = append(tagSizes, taggedFieldSize)
        // size = <tag id contrib> + <field size>
        size += sizeofUvarint(0) + sizeofUvarint(taggedFieldSize)
    }
    if version >= 12 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields14)
    }
    return size, tagSizes
}


