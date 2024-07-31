// Package protocol - This is a generated file, please do not edit

package protocol

import "encoding/binary"
import "github.com/spirit-labs/tektite/errors"
import "github.com/spirit-labs/tektite/debug"
import "fmt"
import "unsafe"

type ProduceResponseBatchIndexAndErrorMessage struct {
    // The batch index of the record that cause the batch to be dropped
    BatchIndex int32
    // The error message of the record that caused the batch to be dropped
    BatchIndexErrorMessage *string
}

type ProduceResponseLeaderIdAndEpoch struct {
    // The ID of the current leader or -1 if the leader is unknown.
    LeaderId int32
    // The latest known leader epoch
    LeaderEpoch int32
}

type ProduceResponsePartitionProduceResponse struct {
    // The partition index.
    Index int32
    // The error code, or 0 if there was no error.
    ErrorCode int16
    // The base offset.
    BaseOffset int64
    // The timestamp returned by broker after appending the messages. If CreateTime is used for the topic, the timestamp will be -1.  If LogAppendTime is used for the topic, the timestamp will be the broker local time when the messages are appended.
    LogAppendTimeMs int64
    // The log start offset.
    LogStartOffset int64
    // The batch indices of records that caused the batch to be dropped
    RecordErrors []ProduceResponseBatchIndexAndErrorMessage
    // The global error message summarizing the common root cause of the records that caused the batch to be dropped
    ErrorMessage *string
    CurrentLeader ProduceResponseLeaderIdAndEpoch
}

type ProduceResponseTopicProduceResponse struct {
    // The topic name
    Name *string
    // Each partition that we produced to within the topic.
    PartitionResponses []ProduceResponsePartitionProduceResponse
}

type ProduceResponseNodeEndpoint struct {
    // The ID of the associated node.
    NodeId int32
    // The node's hostname.
    Host *string
    // The node's port.
    Port int32
    // The rack of the node, or null if it has not been assigned to a rack.
    Rack *string
}

type ProduceResponse struct {
    // Each produce response
    Responses []ProduceResponseTopicProduceResponse
    // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ThrottleTimeMs int32
    // Endpoints for all current-leaders enumerated in PartitionProduceResponses, with errors NOT_LEADER_OR_FOLLOWER.
    NodeEndpoints []ProduceResponseNodeEndpoint
}

func (m *ProduceResponse) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    {
        // reading m.Responses: Each produce response
        var l0 int
        if version >= 9 {
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
            responses := make([]ProduceResponseTopicProduceResponse, l0)
            for i0 := 0; i0 < l0; i0++ {
                // reading non tagged fields
                {
                    // reading responses[i0].Name: The topic name
                    if version >= 9 {
                        // flexible and not nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l1 := int(u - 1)
                        s := string(buff[offset: offset + l1])
                        responses[i0].Name = &s
                        offset += l1
                    } else {
                        // non flexible and non nullable
                        var l1 int
                        l1 = int(binary.BigEndian.Uint16(buff[offset:]))
                        offset += 2
                        s := string(buff[offset: offset + l1])
                        responses[i0].Name = &s
                        offset += l1
                    }
                }
                {
                    // reading responses[i0].PartitionResponses: Each partition that we produced to within the topic.
                    var l2 int
                    if version >= 9 {
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
                        partitionResponses := make([]ProduceResponsePartitionProduceResponse, l2)
                        for i1 := 0; i1 < l2; i1++ {
                            // reading non tagged fields
                            {
                                // reading partitionResponses[i1].Index: The partition index.
                                partitionResponses[i1].Index = int32(binary.BigEndian.Uint32(buff[offset:]))
                                offset += 4
                            }
                            {
                                // reading partitionResponses[i1].ErrorCode: The error code, or 0 if there was no error.
                                partitionResponses[i1].ErrorCode = int16(binary.BigEndian.Uint16(buff[offset:]))
                                offset += 2
                            }
                            {
                                // reading partitionResponses[i1].BaseOffset: The base offset.
                                partitionResponses[i1].BaseOffset = int64(binary.BigEndian.Uint64(buff[offset:]))
                                offset += 8
                            }
                            if version >= 2 {
                                {
                                    // reading partitionResponses[i1].LogAppendTimeMs: The timestamp returned by broker after appending the messages. If CreateTime is used for the topic, the timestamp will be -1.  If LogAppendTime is used for the topic, the timestamp will be the broker local time when the messages are appended.
                                    partitionResponses[i1].LogAppendTimeMs = int64(binary.BigEndian.Uint64(buff[offset:]))
                                    offset += 8
                                }
                            }
                            if version >= 5 {
                                {
                                    // reading partitionResponses[i1].LogStartOffset: The log start offset.
                                    partitionResponses[i1].LogStartOffset = int64(binary.BigEndian.Uint64(buff[offset:]))
                                    offset += 8
                                }
                            }
                            if version >= 8 {
                                {
                                    // reading partitionResponses[i1].RecordErrors: The batch indices of records that caused the batch to be dropped
                                    var l3 int
                                    if version >= 9 {
                                        // flexible and not nullable
                                        u, n := binary.Uvarint(buff[offset:])
                                        offset += n
                                        l3 = int(u - 1)
                                    } else {
                                        // non flexible and non nullable
                                        l3 = int(binary.BigEndian.Uint32(buff[offset:]))
                                        offset += 4
                                    }
                                    if l3 >= 0 {
                                        // length will be -1 if field is null
                                        recordErrors := make([]ProduceResponseBatchIndexAndErrorMessage, l3)
                                        for i2 := 0; i2 < l3; i2++ {
                                            // reading non tagged fields
                                            {
                                                // reading recordErrors[i2].BatchIndex: The batch index of the record that cause the batch to be dropped
                                                recordErrors[i2].BatchIndex = int32(binary.BigEndian.Uint32(buff[offset:]))
                                                offset += 4
                                            }
                                            {
                                                // reading recordErrors[i2].BatchIndexErrorMessage: The error message of the record that caused the batch to be dropped
                                                if version >= 9 {
                                                    // flexible and nullable
                                                    u, n := binary.Uvarint(buff[offset:])
                                                    offset += n
                                                    l4 := int(u - 1)
                                                    if l4 > 0 {
                                                        s := string(buff[offset: offset + l4])
                                                        recordErrors[i2].BatchIndexErrorMessage = &s
                                                        offset += l4
                                                    } else {
                                                        recordErrors[i2].BatchIndexErrorMessage = nil
                                                    }
                                                } else {
                                                    // non flexible and nullable
                                                    var l4 int
                                                    l4 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
                                                    offset += 2
                                                    if l4 > 0 {
                                                        s := string(buff[offset: offset + l4])
                                                        recordErrors[i2].BatchIndexErrorMessage = &s
                                                        offset += l4
                                                    } else {
                                                        recordErrors[i2].BatchIndexErrorMessage = nil
                                                    }
                                                }
                                            }
                                            if version >= 9 {
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
                                    partitionResponses[i1].RecordErrors = recordErrors
                                    }
                                }
                                {
                                    // reading partitionResponses[i1].ErrorMessage: The global error message summarizing the common root cause of the records that caused the batch to be dropped
                                    if version >= 9 {
                                        // flexible and nullable
                                        u, n := binary.Uvarint(buff[offset:])
                                        offset += n
                                        l5 := int(u - 1)
                                        if l5 > 0 {
                                            s := string(buff[offset: offset + l5])
                                            partitionResponses[i1].ErrorMessage = &s
                                            offset += l5
                                        } else {
                                            partitionResponses[i1].ErrorMessage = nil
                                        }
                                    } else {
                                        // non flexible and nullable
                                        var l5 int
                                        l5 = int(int16(binary.BigEndian.Uint16(buff[offset:])))
                                        offset += 2
                                        if l5 > 0 {
                                            s := string(buff[offset: offset + l5])
                                            partitionResponses[i1].ErrorMessage = &s
                                            offset += l5
                                        } else {
                                            partitionResponses[i1].ErrorMessage = nil
                                        }
                                    }
                                }
                            }
                            if version >= 9 {
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
                                            if version >= 10 {
                                                {
                                                    // reading partitionResponses[i1].CurrentLeader: 
                                                    {
                                                        // reading non tagged fields
                                                        {
                                                            // reading partitionResponses[i1].CurrentLeader.LeaderId: The ID of the current leader or -1 if the leader is unknown.
                                                            partitionResponses[i1].CurrentLeader.LeaderId = int32(binary.BigEndian.Uint32(buff[offset:]))
                                                            offset += 4
                                                        }
                                                        {
                                                            // reading partitionResponses[i1].CurrentLeader.LeaderEpoch: The latest known leader epoch
                                                            partitionResponses[i1].CurrentLeader.LeaderEpoch = int32(binary.BigEndian.Uint32(buff[offset:]))
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
                                            } else {
                                                return 0, errors.Errorf("Tag 0 is not valid at version %d", version)
                                            }
                                        default:
                                            offset += int(ts)
                                    }
                                }
                            }
                        }
                    responses[i0].PartitionResponses = partitionResponses
                    }
                }
                if version >= 9 {
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
    if version >= 1 {
        {
            // reading m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
            m.ThrottleTimeMs = int32(binary.BigEndian.Uint32(buff[offset:]))
            offset += 4
        }
    }
    if version >= 9 {
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
                    if version >= 10 {
                        {
                            // reading m.NodeEndpoints: Endpoints for all current-leaders enumerated in PartitionProduceResponses, with errors NOT_LEADER_OR_FOLLOWER.
                            var l6 int
                            // flexible and not nullable
                            u, n := binary.Uvarint(buff[offset:])
                            offset += n
                            l6 = int(u - 1)
                            if l6 >= 0 {
                                // length will be -1 if field is null
                                nodeEndpoints := make([]ProduceResponseNodeEndpoint, l6)
                                for i3 := 0; i3 < l6; i3++ {
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
                                        l7 := int(u - 1)
                                        s := string(buff[offset: offset + l7])
                                        nodeEndpoints[i3].Host = &s
                                        offset += l7
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
                                        l8 := int(u - 1)
                                        if l8 > 0 {
                                            s := string(buff[offset: offset + l8])
                                            nodeEndpoints[i3].Rack = &s
                                            offset += l8
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

func (m *ProduceResponse) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    // writing m.Responses: Each produce response
    if version >= 9 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.Responses) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.Responses)))
    }
    for _, responses := range m.Responses {
        // writing non tagged fields
        // writing responses.Name: The topic name
        if version >= 9 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*responses.Name) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*responses.Name)))
        }
        if responses.Name != nil {
            buff = append(buff, *responses.Name...)
        }
        // writing responses.PartitionResponses: Each partition that we produced to within the topic.
        if version >= 9 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(responses.PartitionResponses) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint32(buff, uint32(len(responses.PartitionResponses)))
        }
        for _, partitionResponses := range responses.PartitionResponses {
            // writing non tagged fields
            // writing partitionResponses.Index: The partition index.
            buff = binary.BigEndian.AppendUint32(buff, uint32(partitionResponses.Index))
            // writing partitionResponses.ErrorCode: The error code, or 0 if there was no error.
            buff = binary.BigEndian.AppendUint16(buff, uint16(partitionResponses.ErrorCode))
            // writing partitionResponses.BaseOffset: The base offset.
            buff = binary.BigEndian.AppendUint64(buff, uint64(partitionResponses.BaseOffset))
            if version >= 2 {
                // writing partitionResponses.LogAppendTimeMs: The timestamp returned by broker after appending the messages. If CreateTime is used for the topic, the timestamp will be -1.  If LogAppendTime is used for the topic, the timestamp will be the broker local time when the messages are appended.
                buff = binary.BigEndian.AppendUint64(buff, uint64(partitionResponses.LogAppendTimeMs))
            }
            if version >= 5 {
                // writing partitionResponses.LogStartOffset: The log start offset.
                buff = binary.BigEndian.AppendUint64(buff, uint64(partitionResponses.LogStartOffset))
            }
            if version >= 8 {
                // writing partitionResponses.RecordErrors: The batch indices of records that caused the batch to be dropped
                if version >= 9 {
                    // flexible and not nullable
                    buff = binary.AppendUvarint(buff, uint64(len(partitionResponses.RecordErrors) + 1))
                } else {
                    // non flexible and non nullable
                    buff = binary.BigEndian.AppendUint32(buff, uint32(len(partitionResponses.RecordErrors)))
                }
                for _, recordErrors := range partitionResponses.RecordErrors {
                    // writing non tagged fields
                    // writing recordErrors.BatchIndex: The batch index of the record that cause the batch to be dropped
                    buff = binary.BigEndian.AppendUint32(buff, uint32(recordErrors.BatchIndex))
                    // writing recordErrors.BatchIndexErrorMessage: The error message of the record that caused the batch to be dropped
                    if version >= 9 {
                        // flexible and nullable
                        if recordErrors.BatchIndexErrorMessage == nil {
                            // null
                            buff = append(buff, 0)
                        } else {
                            // not null
                            buff = binary.AppendUvarint(buff, uint64(len(*recordErrors.BatchIndexErrorMessage) + 1))
                        }
                    } else {
                        // non flexible and nullable
                        if recordErrors.BatchIndexErrorMessage == nil {
                            // null
                            buff = binary.BigEndian.AppendUint16(buff, 65535)
                        } else {
                            // not null
                            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*recordErrors.BatchIndexErrorMessage)))
                        }
                    }
                    if recordErrors.BatchIndexErrorMessage != nil {
                        buff = append(buff, *recordErrors.BatchIndexErrorMessage...)
                    }
                    if version >= 9 {
                        numTaggedFields11 := 0
                        // write number of tagged fields
                        buff = binary.AppendUvarint(buff, uint64(numTaggedFields11))
                    }
                }
                // writing partitionResponses.ErrorMessage: The global error message summarizing the common root cause of the records that caused the batch to be dropped
                if version >= 9 {
                    // flexible and nullable
                    if partitionResponses.ErrorMessage == nil {
                        // null
                        buff = append(buff, 0)
                    } else {
                        // not null
                        buff = binary.AppendUvarint(buff, uint64(len(*partitionResponses.ErrorMessage) + 1))
                    }
                } else {
                    // non flexible and nullable
                    if partitionResponses.ErrorMessage == nil {
                        // null
                        buff = binary.BigEndian.AppendUint16(buff, 65535)
                    } else {
                        // not null
                        buff = binary.BigEndian.AppendUint16(buff, uint16(len(*partitionResponses.ErrorMessage)))
                    }
                }
                if partitionResponses.ErrorMessage != nil {
                    buff = append(buff, *partitionResponses.ErrorMessage...)
                }
            }
            if version >= 9 {
                numTaggedFields13 := 0
                // writing tagged field increments
                if version >= 10 {
                    // tagged field - partitionResponses.CurrentLeader: 
                    numTaggedFields13++
                }
                // write number of tagged fields
                buff = binary.AppendUvarint(buff, uint64(numTaggedFields13))
                // writing tagged fields
                if version >= 10 {
                    // tag header
                    buff = binary.AppendUvarint(buff, uint64(0))
                    buff = binary.AppendUvarint(buff, uint64(tagSizes[tagPos]))
                    tagPos++
                    var tagSizeStart14 int
                    if debug.SanityChecks {
                        tagSizeStart14 = len(buff)
                    }
                    // writing partitionResponses.CurrentLeader: 
                    {
                        // writing non tagged fields
                        // writing partitionResponses.CurrentLeader.LeaderId: The ID of the current leader or -1 if the leader is unknown.
                        buff = binary.BigEndian.AppendUint32(buff, uint32(partitionResponses.CurrentLeader.LeaderId))
                        // writing partitionResponses.CurrentLeader.LeaderEpoch: The latest known leader epoch
                        buff = binary.BigEndian.AppendUint32(buff, uint32(partitionResponses.CurrentLeader.LeaderEpoch))
                        numTaggedFields17 := 0
                        // write number of tagged fields
                        buff = binary.AppendUvarint(buff, uint64(numTaggedFields17))
                    }
                    if debug.SanityChecks && len(buff) - tagSizeStart14 != tagSizes[tagPos - 1] {
                        panic(fmt.Sprintf("incorrect calculated tag size for tag %d", 0))
                    }
                }
            }
        }
        if version >= 9 {
            numTaggedFields18 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields18))
        }
    }
    if version >= 1 {
        // writing m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        buff = binary.BigEndian.AppendUint32(buff, uint32(m.ThrottleTimeMs))
    }
    if version >= 9 {
        numTaggedFields20 := 0
        // writing tagged field increments
        if version >= 10 {
            // tagged field - m.NodeEndpoints: Endpoints for all current-leaders enumerated in PartitionProduceResponses, with errors NOT_LEADER_OR_FOLLOWER.
            numTaggedFields20++
        }
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields20))
        // writing tagged fields
        if version >= 10 {
            // tag header
            buff = binary.AppendUvarint(buff, uint64(0))
            buff = binary.AppendUvarint(buff, uint64(tagSizes[tagPos]))
            tagPos++
            var tagSizeStart21 int
            if debug.SanityChecks {
                tagSizeStart21 = len(buff)
            }
            // writing m.NodeEndpoints: Endpoints for all current-leaders enumerated in PartitionProduceResponses, with errors NOT_LEADER_OR_FOLLOWER.
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
                numTaggedFields26 := 0
                // write number of tagged fields
                buff = binary.AppendUvarint(buff, uint64(numTaggedFields26))
            }
            if debug.SanityChecks && len(buff) - tagSizeStart21 != tagSizes[tagPos - 1] {
                panic(fmt.Sprintf("incorrect calculated tag size for tag %d", 0))
            }
        }
    }
    return buff
}

func (m *ProduceResponse) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    // size for m.Responses: Each produce response
    if version >= 9 {
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
        // size for responses.Name: The topic name
        if version >= 9 {
            // flexible and not nullable
            size += sizeofUvarint(len(*responses.Name) + 1)
        } else {
            // non flexible and non nullable
            size += 2
        }
        if responses.Name != nil {
            size += len(*responses.Name)
        }
        // size for responses.PartitionResponses: Each partition that we produced to within the topic.
        if version >= 9 {
            // flexible and not nullable
            size += sizeofUvarint(len(responses.PartitionResponses) + 1)
        } else {
            // non flexible and non nullable
            size += 4
        }
        for _, partitionResponses := range responses.PartitionResponses {
            size += 0 * int(unsafe.Sizeof(partitionResponses)) // hack to make sure loop variable is always used
            // calculating size for non tagged fields
            numTaggedFields2:= 0
            numTaggedFields2 += 0
            // size for partitionResponses.Index: The partition index.
            size += 4
            // size for partitionResponses.ErrorCode: The error code, or 0 if there was no error.
            size += 2
            // size for partitionResponses.BaseOffset: The base offset.
            size += 8
            if version >= 2 {
                // size for partitionResponses.LogAppendTimeMs: The timestamp returned by broker after appending the messages. If CreateTime is used for the topic, the timestamp will be -1.  If LogAppendTime is used for the topic, the timestamp will be the broker local time when the messages are appended.
                size += 8
            }
            if version >= 5 {
                // size for partitionResponses.LogStartOffset: The log start offset.
                size += 8
            }
            if version >= 8 {
                // size for partitionResponses.RecordErrors: The batch indices of records that caused the batch to be dropped
                if version >= 9 {
                    // flexible and not nullable
                    size += sizeofUvarint(len(partitionResponses.RecordErrors) + 1)
                } else {
                    // non flexible and non nullable
                    size += 4
                }
                for _, recordErrors := range partitionResponses.RecordErrors {
                    size += 0 * int(unsafe.Sizeof(recordErrors)) // hack to make sure loop variable is always used
                    // calculating size for non tagged fields
                    numTaggedFields3:= 0
                    numTaggedFields3 += 0
                    // size for recordErrors.BatchIndex: The batch index of the record that cause the batch to be dropped
                    size += 4
                    // size for recordErrors.BatchIndexErrorMessage: The error message of the record that caused the batch to be dropped
                    if version >= 9 {
                        // flexible and nullable
                        if recordErrors.BatchIndexErrorMessage == nil {
                            // null
                            size += 1
                        } else {
                            // not null
                            size += sizeofUvarint(len(*recordErrors.BatchIndexErrorMessage) + 1)
                        }
                    } else {
                        // non flexible and nullable
                        size += 2
                    }
                    if recordErrors.BatchIndexErrorMessage != nil {
                        size += len(*recordErrors.BatchIndexErrorMessage)
                    }
                    numTaggedFields4:= 0
                    numTaggedFields4 += 0
                    if version >= 9 {
                        // writing size of num tagged fields field
                        size += sizeofUvarint(numTaggedFields4)
                    }
                }
                // size for partitionResponses.ErrorMessage: The global error message summarizing the common root cause of the records that caused the batch to be dropped
                if version >= 9 {
                    // flexible and nullable
                    if partitionResponses.ErrorMessage == nil {
                        // null
                        size += 1
                    } else {
                        // not null
                        size += sizeofUvarint(len(*partitionResponses.ErrorMessage) + 1)
                    }
                } else {
                    // non flexible and nullable
                    size += 2
                }
                if partitionResponses.ErrorMessage != nil {
                    size += len(*partitionResponses.ErrorMessage)
                }
            }
            numTaggedFields5:= 0
            numTaggedFields5 += 0
            taggedFieldStart := 0
            taggedFieldSize := 0
            if version >= 10 {
                // size for partitionResponses.CurrentLeader: 
                numTaggedFields5++
                taggedFieldStart = size
                {
                    // calculating size for non tagged fields
                    numTaggedFields6:= 0
                    numTaggedFields6 += 0
                    // size for partitionResponses.CurrentLeader.LeaderId: The ID of the current leader or -1 if the leader is unknown.
                    size += 4
                    // size for partitionResponses.CurrentLeader.LeaderEpoch: The latest known leader epoch
                    size += 4
                    numTaggedFields7:= 0
                    numTaggedFields7 += 0
                    // writing size of num tagged fields field
                    size += sizeofUvarint(numTaggedFields7)
                }
                taggedFieldSize = size - taggedFieldStart
                tagSizes = append(tagSizes, taggedFieldSize)
                // size = <tag id contrib> + <field size>
                size += sizeofUvarint(0) + sizeofUvarint(taggedFieldSize)
            }
            if version >= 9 {
                // writing size of num tagged fields field
                size += sizeofUvarint(numTaggedFields5)
            }
        }
        numTaggedFields8:= 0
        numTaggedFields8 += 0
        if version >= 9 {
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields8)
        }
    }
    if version >= 1 {
        // size for m.ThrottleTimeMs: The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
        size += 4
    }
    numTaggedFields9:= 0
    numTaggedFields9 += 0
    taggedFieldStart := 0
    taggedFieldSize := 0
    if version >= 10 {
        // size for m.NodeEndpoints: Endpoints for all current-leaders enumerated in PartitionProduceResponses, with errors NOT_LEADER_OR_FOLLOWER.
        numTaggedFields9++
        taggedFieldStart = size
        // flexible and not nullable
        size += sizeofUvarint(len(m.NodeEndpoints) + 1)
        for _, nodeEndpoints := range m.NodeEndpoints {
            size += 0 * int(unsafe.Sizeof(nodeEndpoints)) // hack to make sure loop variable is always used
            // calculating size for non tagged fields
            numTaggedFields10:= 0
            numTaggedFields10 += 0
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
            numTaggedFields11:= 0
            numTaggedFields11 += 0
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields11)
        }
        taggedFieldSize = size - taggedFieldStart
        tagSizes = append(tagSizes, taggedFieldSize)
        // size = <tag id contrib> + <field size>
        size += sizeofUvarint(0) + sizeofUvarint(taggedFieldSize)
    }
    if version >= 9 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields9)
    }
    return size, tagSizes
}

