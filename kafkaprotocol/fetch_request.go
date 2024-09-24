// Package kafkaprotocol - This is a generated file, please do not edit

package kafkaprotocol

import "encoding/binary"
import "fmt"
import "github.com/pkg/errors"
import "github.com/spirit-labs/tektite/common"
import "github.com/spirit-labs/tektite/debug"
import "unsafe"

type FetchRequestReplicaState struct {
    // The replica ID of the follower, or -1 if this request is from a consumer.
    ReplicaId int32
    // The epoch of this follower, or -1 if not available.
    ReplicaEpoch int64
}

type FetchRequestFetchPartition struct {
    // The partition index.
    Partition int32
    // The current leader epoch of the partition.
    CurrentLeaderEpoch int32
    // The message offset.
    FetchOffset int64
    // The epoch of the last fetched record or -1 if there is none
    LastFetchedEpoch int32
    // The earliest available offset of the follower replica.  The field is only used when the request is sent by the follower.
    LogStartOffset int64
    // The maximum bytes to fetch from this partition.  See KIP-74 for cases where this limit may not be honored.
    PartitionMaxBytes int32
    // The directory id of the follower fetching
    ReplicaDirectoryId []byte
}

type FetchRequestFetchTopic struct {
    // The name of the topic to fetch.
    Topic *string
    // The unique topic ID
    TopicId []byte
    // The partitions to fetch.
    Partitions []FetchRequestFetchPartition
}

type FetchRequestForgottenTopic struct {
    // The topic name.
    Topic *string
    // The unique topic ID
    TopicId []byte
    // The partitions indexes to forget.
    Partitions []int32
}

type FetchRequest struct {
    // The clusterId if known. This is used to validate metadata fetches prior to broker registration.
    ClusterId *string
    // The broker ID of the follower, of -1 if this request is from a consumer.
    ReplicaId int32
    ReplicaState FetchRequestReplicaState
    // The maximum time in milliseconds to wait for the response.
    MaxWaitMs int32
    // The minimum bytes to accumulate in the response.
    MinBytes int32
    // The maximum bytes to fetch.  See KIP-74 for cases where this limit may not be honored.
    MaxBytes int32
    // This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records
    IsolationLevel int8
    // The fetch session ID.
    SessionId int32
    // The fetch session epoch, which is used for ordering requests in a session.
    SessionEpoch int32
    // The topics to fetch.
    Topics []FetchRequestFetchTopic
    // In an incremental fetch request, the partitions to remove.
    ForgottenTopicsData []FetchRequestForgottenTopic
    // Rack ID of the consumer making this request
    RackId *string
}

func (m *FetchRequest) Read(version int16, buff []byte) (int, error) {
    offset := 0
    // reading non tagged fields
    if version <= 14 {
        {
            // reading m.ReplicaId: The broker ID of the follower, of -1 if this request is from a consumer.
            m.ReplicaId = int32(binary.BigEndian.Uint32(buff[offset:]))
            offset += 4
        }
    }
    {
        // reading m.MaxWaitMs: The maximum time in milliseconds to wait for the response.
        m.MaxWaitMs = int32(binary.BigEndian.Uint32(buff[offset:]))
        offset += 4
    }
    {
        // reading m.MinBytes: The minimum bytes to accumulate in the response.
        m.MinBytes = int32(binary.BigEndian.Uint32(buff[offset:]))
        offset += 4
    }
    if version >= 3 {
        {
            // reading m.MaxBytes: The maximum bytes to fetch.  See KIP-74 for cases where this limit may not be honored.
            m.MaxBytes = int32(binary.BigEndian.Uint32(buff[offset:]))
            offset += 4
        }
    }
    if version >= 4 {
        {
            // reading m.IsolationLevel: This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records
            m.IsolationLevel = int8(buff[offset])
            offset++
        }
    }
    if version >= 7 {
        {
            // reading m.SessionId: The fetch session ID.
            m.SessionId = int32(binary.BigEndian.Uint32(buff[offset:]))
            offset += 4
        }
        {
            // reading m.SessionEpoch: The fetch session epoch, which is used for ordering requests in a session.
            m.SessionEpoch = int32(binary.BigEndian.Uint32(buff[offset:]))
            offset += 4
        }
    }
    {
        // reading m.Topics: The topics to fetch.
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
            topics := make([]FetchRequestFetchTopic, l0)
            for i0 := 0; i0 < l0; i0++ {
                // reading non tagged fields
                if version <= 12 {
                    {
                        // reading topics[i0].Topic: The name of the topic to fetch.
                        if version >= 12 {
                            // flexible and not nullable
                            u, n := binary.Uvarint(buff[offset:])
                            offset += n
                            l1 := int(u - 1)
                            s := string(buff[offset: offset + l1])
                            topics[i0].Topic = &s
                            offset += l1
                        } else {
                            // non flexible and non nullable
                            var l1 int
                            l1 = int(binary.BigEndian.Uint16(buff[offset:]))
                            offset += 2
                            s := string(buff[offset: offset + l1])
                            topics[i0].Topic = &s
                            offset += l1
                        }
                    }
                }
                if version >= 13 {
                    {
                        // reading topics[i0].TopicId: The unique topic ID
                        topics[i0].TopicId = common.ByteSliceCopy(buff[offset: offset + 16])
                        offset += 16
                    }
                }
                {
                    // reading topics[i0].Partitions: The partitions to fetch.
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
                        partitions := make([]FetchRequestFetchPartition, l2)
                        for i1 := 0; i1 < l2; i1++ {
                            // reading non tagged fields
                            {
                                // reading partitions[i1].Partition: The partition index.
                                partitions[i1].Partition = int32(binary.BigEndian.Uint32(buff[offset:]))
                                offset += 4
                            }
                            if version >= 9 {
                                {
                                    // reading partitions[i1].CurrentLeaderEpoch: The current leader epoch of the partition.
                                    partitions[i1].CurrentLeaderEpoch = int32(binary.BigEndian.Uint32(buff[offset:]))
                                    offset += 4
                                }
                            }
                            {
                                // reading partitions[i1].FetchOffset: The message offset.
                                partitions[i1].FetchOffset = int64(binary.BigEndian.Uint64(buff[offset:]))
                                offset += 8
                            }
                            if version >= 12 {
                                {
                                    // reading partitions[i1].LastFetchedEpoch: The epoch of the last fetched record or -1 if there is none
                                    partitions[i1].LastFetchedEpoch = int32(binary.BigEndian.Uint32(buff[offset:]))
                                    offset += 4
                                }
                            }
                            if version >= 5 {
                                {
                                    // reading partitions[i1].LogStartOffset: The earliest available offset of the follower replica.  The field is only used when the request is sent by the follower.
                                    partitions[i1].LogStartOffset = int64(binary.BigEndian.Uint64(buff[offset:]))
                                    offset += 8
                                }
                            }
                            {
                                // reading partitions[i1].PartitionMaxBytes: The maximum bytes to fetch from this partition.  See KIP-74 for cases where this limit may not be honored.
                                partitions[i1].PartitionMaxBytes = int32(binary.BigEndian.Uint32(buff[offset:]))
                                offset += 4
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
                                            if version >= 17 {
                                                {
                                                    // reading partitions[i1].ReplicaDirectoryId: The directory id of the follower fetching
                                                    partitions[i1].ReplicaDirectoryId = common.ByteSliceCopy(buff[offset: offset + 16])
                                                    offset += 16
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
                    topics[i0].Partitions = partitions
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
        m.Topics = topics
        }
    }
    if version >= 7 {
        {
            // reading m.ForgottenTopicsData: In an incremental fetch request, the partitions to remove.
            var l3 int
            if version >= 12 {
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
                forgottenTopicsData := make([]FetchRequestForgottenTopic, l3)
                for i2 := 0; i2 < l3; i2++ {
                    // reading non tagged fields
                    if version >= 7 && version <= 12 {
                        {
                            // reading forgottenTopicsData[i2].Topic: The topic name.
                            if version >= 12 {
                                // flexible and not nullable
                                u, n := binary.Uvarint(buff[offset:])
                                offset += n
                                l4 := int(u - 1)
                                s := string(buff[offset: offset + l4])
                                forgottenTopicsData[i2].Topic = &s
                                offset += l4
                            } else {
                                // non flexible and non nullable
                                var l4 int
                                l4 = int(binary.BigEndian.Uint16(buff[offset:]))
                                offset += 2
                                s := string(buff[offset: offset + l4])
                                forgottenTopicsData[i2].Topic = &s
                                offset += l4
                            }
                        }
                    }
                    if version >= 13 {
                        {
                            // reading forgottenTopicsData[i2].TopicId: The unique topic ID
                            forgottenTopicsData[i2].TopicId = common.ByteSliceCopy(buff[offset: offset + 16])
                            offset += 16
                        }
                    }
                    {
                        // reading forgottenTopicsData[i2].Partitions: The partitions indexes to forget.
                        var l5 int
                        if version >= 12 {
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
                            partitions := make([]int32, l5)
                            for i3 := 0; i3 < l5; i3++ {
                                partitions[i3] = int32(binary.BigEndian.Uint32(buff[offset:]))
                                offset += 4
                            }
                            forgottenTopicsData[i2].Partitions = partitions
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
            m.ForgottenTopicsData = forgottenTopicsData
            }
        }
    }
    if version >= 11 {
        {
            // reading m.RackId: Rack ID of the consumer making this request
            if version >= 12 {
                // flexible and not nullable
                u, n := binary.Uvarint(buff[offset:])
                offset += n
                l6 := int(u - 1)
                s := string(buff[offset: offset + l6])
                m.RackId = &s
                offset += l6
            } else {
                // non flexible and non nullable
                var l6 int
                l6 = int(binary.BigEndian.Uint16(buff[offset:]))
                offset += 2
                s := string(buff[offset: offset + l6])
                m.RackId = &s
                offset += l6
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
                        // reading m.ClusterId: The clusterId if known. This is used to validate metadata fetches prior to broker registration.
                        // flexible and nullable
                        u, n := binary.Uvarint(buff[offset:])
                        offset += n
                        l7 := int(u - 1)
                        if l7 > 0 {
                            s := string(buff[offset: offset + l7])
                            m.ClusterId = &s
                            offset += l7
                        } else {
                            m.ClusterId = nil
                        }
                    }
                case 1:
                    if version >= 15 {
                        {
                            // reading m.ReplicaState: 
                            {
                                // reading non tagged fields
                                {
                                    // reading m.ReplicaState.ReplicaId: The replica ID of the follower, or -1 if this request is from a consumer.
                                    m.ReplicaState.ReplicaId = int32(binary.BigEndian.Uint32(buff[offset:]))
                                    offset += 4
                                }
                                {
                                    // reading m.ReplicaState.ReplicaEpoch: The epoch of this follower, or -1 if not available.
                                    m.ReplicaState.ReplicaEpoch = int64(binary.BigEndian.Uint64(buff[offset:]))
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
                    } else {
                        return 0, errors.Errorf("Tag 1 is not valid at version %d", version)
                    }
                default:
                    offset += int(ts)
            }
        }
    }
    return offset, nil
}

func (m *FetchRequest) Write(version int16, buff []byte, tagSizes []int) []byte {
    var tagPos int
    tagPos += 0 // make sure variable is used
    // writing non tagged fields
    if version <= 14 {
        // writing m.ReplicaId: The broker ID of the follower, of -1 if this request is from a consumer.
        buff = binary.BigEndian.AppendUint32(buff, uint32(m.ReplicaId))
    }
    // writing m.MaxWaitMs: The maximum time in milliseconds to wait for the response.
    buff = binary.BigEndian.AppendUint32(buff, uint32(m.MaxWaitMs))
    // writing m.MinBytes: The minimum bytes to accumulate in the response.
    buff = binary.BigEndian.AppendUint32(buff, uint32(m.MinBytes))
    if version >= 3 {
        // writing m.MaxBytes: The maximum bytes to fetch.  See KIP-74 for cases where this limit may not be honored.
        buff = binary.BigEndian.AppendUint32(buff, uint32(m.MaxBytes))
    }
    if version >= 4 {
        // writing m.IsolationLevel: This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records
        buff = append(buff, byte(m.IsolationLevel))
    }
    if version >= 7 {
        // writing m.SessionId: The fetch session ID.
        buff = binary.BigEndian.AppendUint32(buff, uint32(m.SessionId))
        // writing m.SessionEpoch: The fetch session epoch, which is used for ordering requests in a session.
        buff = binary.BigEndian.AppendUint32(buff, uint32(m.SessionEpoch))
    }
    // writing m.Topics: The topics to fetch.
    if version >= 12 {
        // flexible and not nullable
        buff = binary.AppendUvarint(buff, uint64(len(m.Topics) + 1))
    } else {
        // non flexible and non nullable
        buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.Topics)))
    }
    for _, topics := range m.Topics {
        // writing non tagged fields
        if version <= 12 {
            // writing topics.Topic: The name of the topic to fetch.
            if version >= 12 {
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(*topics.Topic) + 1))
            } else {
                // non flexible and non nullable
                buff = binary.BigEndian.AppendUint16(buff, uint16(len(*topics.Topic)))
            }
            if topics.Topic != nil {
                buff = append(buff, *topics.Topic...)
            }
        }
        if version >= 13 {
            // writing topics.TopicId: The unique topic ID
            if topics.TopicId != nil {
                buff = append(buff, topics.TopicId...)
            }
        }
        // writing topics.Partitions: The partitions to fetch.
        if version >= 12 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(topics.Partitions) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint32(buff, uint32(len(topics.Partitions)))
        }
        for _, partitions := range topics.Partitions {
            // writing non tagged fields
            // writing partitions.Partition: The partition index.
            buff = binary.BigEndian.AppendUint32(buff, uint32(partitions.Partition))
            if version >= 9 {
                // writing partitions.CurrentLeaderEpoch: The current leader epoch of the partition.
                buff = binary.BigEndian.AppendUint32(buff, uint32(partitions.CurrentLeaderEpoch))
            }
            // writing partitions.FetchOffset: The message offset.
            buff = binary.BigEndian.AppendUint64(buff, uint64(partitions.FetchOffset))
            if version >= 12 {
                // writing partitions.LastFetchedEpoch: The epoch of the last fetched record or -1 if there is none
                buff = binary.BigEndian.AppendUint32(buff, uint32(partitions.LastFetchedEpoch))
            }
            if version >= 5 {
                // writing partitions.LogStartOffset: The earliest available offset of the follower replica.  The field is only used when the request is sent by the follower.
                buff = binary.BigEndian.AppendUint64(buff, uint64(partitions.LogStartOffset))
            }
            // writing partitions.PartitionMaxBytes: The maximum bytes to fetch from this partition.  See KIP-74 for cases where this limit may not be honored.
            buff = binary.BigEndian.AppendUint32(buff, uint32(partitions.PartitionMaxBytes))
            if version >= 12 {
                numTaggedFields17 := 0
                // writing tagged field increments
                if version >= 17 {
                    // tagged field - partitions.ReplicaDirectoryId: The directory id of the follower fetching
                    numTaggedFields17++
                }
                // write number of tagged fields
                buff = binary.AppendUvarint(buff, uint64(numTaggedFields17))
                // writing tagged fields
                if version >= 17 {
                    // tag header
                    buff = binary.AppendUvarint(buff, uint64(0))
                    buff = binary.AppendUvarint(buff, uint64(tagSizes[tagPos]))
                    tagPos++
                    var tagSizeStart18 int
                    if debug.SanityChecks {
                        tagSizeStart18 = len(buff)
                    }
                    // writing partitions.ReplicaDirectoryId: The directory id of the follower fetching
                    if partitions.ReplicaDirectoryId != nil {
                        buff = append(buff, partitions.ReplicaDirectoryId...)
                    }
                    if debug.SanityChecks && len(buff) - tagSizeStart18 != tagSizes[tagPos - 1] {
                        panic(fmt.Sprintf("incorrect calculated tag size for tag %d", 0))
                    }
                }
            }
        }
        if version >= 12 {
            numTaggedFields19 := 0
            // write number of tagged fields
            buff = binary.AppendUvarint(buff, uint64(numTaggedFields19))
        }
    }
    if version >= 7 {
        // writing m.ForgottenTopicsData: In an incremental fetch request, the partitions to remove.
        if version >= 12 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(m.ForgottenTopicsData) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint32(buff, uint32(len(m.ForgottenTopicsData)))
        }
        for _, forgottenTopicsData := range m.ForgottenTopicsData {
            // writing non tagged fields
            if version >= 7 && version <= 12 {
                // writing forgottenTopicsData.Topic: The topic name.
                if version >= 12 {
                    // flexible and not nullable
                    buff = binary.AppendUvarint(buff, uint64(len(*forgottenTopicsData.Topic) + 1))
                } else {
                    // non flexible and non nullable
                    buff = binary.BigEndian.AppendUint16(buff, uint16(len(*forgottenTopicsData.Topic)))
                }
                if forgottenTopicsData.Topic != nil {
                    buff = append(buff, *forgottenTopicsData.Topic...)
                }
            }
            if version >= 13 {
                // writing forgottenTopicsData.TopicId: The unique topic ID
                if forgottenTopicsData.TopicId != nil {
                    buff = append(buff, forgottenTopicsData.TopicId...)
                }
            }
            // writing forgottenTopicsData.Partitions: The partitions indexes to forget.
            if version >= 12 {
                // flexible and not nullable
                buff = binary.AppendUvarint(buff, uint64(len(forgottenTopicsData.Partitions) + 1))
            } else {
                // non flexible and non nullable
                buff = binary.BigEndian.AppendUint32(buff, uint32(len(forgottenTopicsData.Partitions)))
            }
            for _, partitions := range forgottenTopicsData.Partitions {
                buff = binary.BigEndian.AppendUint32(buff, uint32(partitions))
            }
            if version >= 12 {
                numTaggedFields24 := 0
                // write number of tagged fields
                buff = binary.AppendUvarint(buff, uint64(numTaggedFields24))
            }
        }
    }
    if version >= 11 {
        // writing m.RackId: Rack ID of the consumer making this request
        if version >= 12 {
            // flexible and not nullable
            buff = binary.AppendUvarint(buff, uint64(len(*m.RackId) + 1))
        } else {
            // non flexible and non nullable
            buff = binary.BigEndian.AppendUint16(buff, uint16(len(*m.RackId)))
        }
        if m.RackId != nil {
            buff = append(buff, *m.RackId...)
        }
    }
    if version >= 12 {
        numTaggedFields26 := 0
        // writing tagged field increments
        // tagged field - m.ClusterId: The clusterId if known. This is used to validate metadata fetches prior to broker registration.
        numTaggedFields26++
        if version >= 15 {
            // tagged field - m.ReplicaState: 
            numTaggedFields26++
        }
        // write number of tagged fields
        buff = binary.AppendUvarint(buff, uint64(numTaggedFields26))
        // writing tagged fields
        // tag header
        buff = binary.AppendUvarint(buff, uint64(0))
        buff = binary.AppendUvarint(buff, uint64(tagSizes[tagPos]))
        tagPos++
        var tagSizeStart27 int
        if debug.SanityChecks {
            tagSizeStart27 = len(buff)
        }
        // writing m.ClusterId: The clusterId if known. This is used to validate metadata fetches prior to broker registration.
        // flexible and nullable
        if m.ClusterId == nil {
            // null
            buff = append(buff, 0)
        } else {
            // not null
            buff = binary.AppendUvarint(buff, uint64(len(*m.ClusterId) + 1))
        }
        if m.ClusterId != nil {
            buff = append(buff, *m.ClusterId...)
        }
        if debug.SanityChecks && len(buff) - tagSizeStart27 != tagSizes[tagPos - 1] {
            panic(fmt.Sprintf("incorrect calculated tag size for tag %d", 0))
        }
        if version >= 15 {
            // tag header
            buff = binary.AppendUvarint(buff, uint64(1))
            buff = binary.AppendUvarint(buff, uint64(tagSizes[tagPos]))
            tagPos++
            var tagSizeStart28 int
            if debug.SanityChecks {
                tagSizeStart28 = len(buff)
            }
            // writing m.ReplicaState: 
            {
                // writing non tagged fields
                // writing m.ReplicaState.ReplicaId: The replica ID of the follower, or -1 if this request is from a consumer.
                buff = binary.BigEndian.AppendUint32(buff, uint32(m.ReplicaState.ReplicaId))
                // writing m.ReplicaState.ReplicaEpoch: The epoch of this follower, or -1 if not available.
                buff = binary.BigEndian.AppendUint64(buff, uint64(m.ReplicaState.ReplicaEpoch))
                numTaggedFields31 := 0
                // write number of tagged fields
                buff = binary.AppendUvarint(buff, uint64(numTaggedFields31))
            }
            if debug.SanityChecks && len(buff) - tagSizeStart28 != tagSizes[tagPos - 1] {
                panic(fmt.Sprintf("incorrect calculated tag size for tag %d", 1))
            }
        }
    }
    return buff
}

func (m *FetchRequest) CalcSize(version int16, tagSizes []int) (int, []int) {
    size := 0
    // calculating size for non tagged fields
    numTaggedFields0:= 0
    numTaggedFields0 += 0
    if version <= 14 {
        // size for m.ReplicaId: The broker ID of the follower, of -1 if this request is from a consumer.
        size += 4
    }
    // size for m.MaxWaitMs: The maximum time in milliseconds to wait for the response.
    size += 4
    // size for m.MinBytes: The minimum bytes to accumulate in the response.
    size += 4
    if version >= 3 {
        // size for m.MaxBytes: The maximum bytes to fetch.  See KIP-74 for cases where this limit may not be honored.
        size += 4
    }
    if version >= 4 {
        // size for m.IsolationLevel: This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records
        size += 1
    }
    if version >= 7 {
        // size for m.SessionId: The fetch session ID.
        size += 4
        // size for m.SessionEpoch: The fetch session epoch, which is used for ordering requests in a session.
        size += 4
    }
    // size for m.Topics: The topics to fetch.
    if version >= 12 {
        // flexible and not nullable
        size += sizeofUvarint(len(m.Topics) + 1)
    } else {
        // non flexible and non nullable
        size += 4
    }
    for _, topics := range m.Topics {
        size += 0 * int(unsafe.Sizeof(topics)) // hack to make sure loop variable is always used
        // calculating size for non tagged fields
        numTaggedFields1:= 0
        numTaggedFields1 += 0
        if version <= 12 {
            // size for topics.Topic: The name of the topic to fetch.
            if version >= 12 {
                // flexible and not nullable
                size += sizeofUvarint(len(*topics.Topic) + 1)
            } else {
                // non flexible and non nullable
                size += 2
            }
            if topics.Topic != nil {
                size += len(*topics.Topic)
            }
        }
        if version >= 13 {
            // size for topics.TopicId: The unique topic ID
            size += 16
        }
        // size for topics.Partitions: The partitions to fetch.
        if version >= 12 {
            // flexible and not nullable
            size += sizeofUvarint(len(topics.Partitions) + 1)
        } else {
            // non flexible and non nullable
            size += 4
        }
        for _, partitions := range topics.Partitions {
            size += 0 * int(unsafe.Sizeof(partitions)) // hack to make sure loop variable is always used
            // calculating size for non tagged fields
            numTaggedFields2:= 0
            numTaggedFields2 += 0
            // size for partitions.Partition: The partition index.
            size += 4
            if version >= 9 {
                // size for partitions.CurrentLeaderEpoch: The current leader epoch of the partition.
                size += 4
            }
            // size for partitions.FetchOffset: The message offset.
            size += 8
            if version >= 12 {
                // size for partitions.LastFetchedEpoch: The epoch of the last fetched record or -1 if there is none
                size += 4
            }
            if version >= 5 {
                // size for partitions.LogStartOffset: The earliest available offset of the follower replica.  The field is only used when the request is sent by the follower.
                size += 8
            }
            // size for partitions.PartitionMaxBytes: The maximum bytes to fetch from this partition.  See KIP-74 for cases where this limit may not be honored.
            size += 4
            numTaggedFields3:= 0
            numTaggedFields3 += 0
            taggedFieldStart := 0
            taggedFieldSize := 0
            if version >= 17 {
                // size for partitions.ReplicaDirectoryId: The directory id of the follower fetching
                numTaggedFields3++
                taggedFieldStart = size
                size += 16
                taggedFieldSize = size - taggedFieldStart
                tagSizes = append(tagSizes, taggedFieldSize)
                // size = <tag id contrib> + <field size>
                size += sizeofUvarint(0) + sizeofUvarint(taggedFieldSize)
            }
            if version >= 12 {
                // writing size of num tagged fields field
                size += sizeofUvarint(numTaggedFields3)
            }
        }
        numTaggedFields4:= 0
        numTaggedFields4 += 0
        if version >= 12 {
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields4)
        }
    }
    if version >= 7 {
        // size for m.ForgottenTopicsData: In an incremental fetch request, the partitions to remove.
        if version >= 12 {
            // flexible and not nullable
            size += sizeofUvarint(len(m.ForgottenTopicsData) + 1)
        } else {
            // non flexible and non nullable
            size += 4
        }
        for _, forgottenTopicsData := range m.ForgottenTopicsData {
            size += 0 * int(unsafe.Sizeof(forgottenTopicsData)) // hack to make sure loop variable is always used
            // calculating size for non tagged fields
            numTaggedFields5:= 0
            numTaggedFields5 += 0
            if version >= 7 && version <= 12 {
                // size for forgottenTopicsData.Topic: The topic name.
                if version >= 12 {
                    // flexible and not nullable
                    size += sizeofUvarint(len(*forgottenTopicsData.Topic) + 1)
                } else {
                    // non flexible and non nullable
                    size += 2
                }
                if forgottenTopicsData.Topic != nil {
                    size += len(*forgottenTopicsData.Topic)
                }
            }
            if version >= 13 {
                // size for forgottenTopicsData.TopicId: The unique topic ID
                size += 16
            }
            // size for forgottenTopicsData.Partitions: The partitions indexes to forget.
            if version >= 12 {
                // flexible and not nullable
                size += sizeofUvarint(len(forgottenTopicsData.Partitions) + 1)
            } else {
                // non flexible and non nullable
                size += 4
            }
            for _, partitions := range forgottenTopicsData.Partitions {
                size += 0 * int(unsafe.Sizeof(partitions)) // hack to make sure loop variable is always used
                size += 4
            }
            numTaggedFields6:= 0
            numTaggedFields6 += 0
            if version >= 12 {
                // writing size of num tagged fields field
                size += sizeofUvarint(numTaggedFields6)
            }
        }
    }
    if version >= 11 {
        // size for m.RackId: Rack ID of the consumer making this request
        if version >= 12 {
            // flexible and not nullable
            size += sizeofUvarint(len(*m.RackId) + 1)
        } else {
            // non flexible and non nullable
            size += 2
        }
        if m.RackId != nil {
            size += len(*m.RackId)
        }
    }
    numTaggedFields7:= 0
    numTaggedFields7 += 0
    taggedFieldStart := 0
    taggedFieldSize := 0
    if version >= 12 {
        // size for m.ClusterId: The clusterId if known. This is used to validate metadata fetches prior to broker registration.
        numTaggedFields7++
        taggedFieldStart = size
        // flexible and nullable
        if m.ClusterId == nil {
            // null
            size += 1
        } else {
            // not null
            size += sizeofUvarint(len(*m.ClusterId) + 1)
        }
        if m.ClusterId != nil {
            size += len(*m.ClusterId)
        }
        taggedFieldSize = size - taggedFieldStart
        tagSizes = append(tagSizes, taggedFieldSize)
        // size = <tag id contrib> + <field size>
        size += sizeofUvarint(0) + sizeofUvarint(taggedFieldSize)
    }
    if version >= 15 {
        // size for m.ReplicaState: 
        numTaggedFields7++
        taggedFieldStart = size
        {
            // calculating size for non tagged fields
            numTaggedFields8:= 0
            numTaggedFields8 += 0
            // size for m.ReplicaState.ReplicaId: The replica ID of the follower, or -1 if this request is from a consumer.
            size += 4
            // size for m.ReplicaState.ReplicaEpoch: The epoch of this follower, or -1 if not available.
            size += 8
            numTaggedFields9:= 0
            numTaggedFields9 += 0
            // writing size of num tagged fields field
            size += sizeofUvarint(numTaggedFields9)
        }
        taggedFieldSize = size - taggedFieldStart
        tagSizes = append(tagSizes, taggedFieldSize)
        // size = <tag id contrib> + <field size>
        size += sizeofUvarint(1) + sizeofUvarint(taggedFieldSize)
    }
    if version >= 12 {
        // writing size of num tagged fields field
        size += sizeofUvarint(numTaggedFields7)
    }
    return size, tagSizes
}

func (m *FetchRequest) HeaderVersions(version int16) (int16, int16) {
    if version >= 12 {
        return 2, 1
    } else {
        return 1, 0
    }
}

func (m *FetchRequest) SupportedApiVersions() (int16, int16) {
    return 4, 4
}
