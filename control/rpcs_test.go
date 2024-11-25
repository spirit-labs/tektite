package control

import (
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/offsets"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/topicmeta"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestSerializeDeserializeRegisterL0Request(t *testing.T) {
	req := RegisterL0Request{
		LeaderVersion: 4555,
		Sequence:      345234,
		RegEntry: lsm.RegistrationEntry{
			Level:            0,
			TableID:          sst.SSTableID("sometableid1"),
			MinVersion:       2323,
			MaxVersion:       100001,
			KeyStart:         []byte("keystart1"),
			KeyEnd:           []byte("keyend1"),
			DeleteRatio:      0.12,
			AddedTime:        uint64(time.Now().UnixMilli()),
			NumEntries:       12345,
			TableSize:        13123123,
			NumPrefixDeletes: 23,
		},
	}
	var buff []byte
	buff = append(buff, 1, 2, 3)
	buff = req.Serialize(buff)
	var req2 RegisterL0Request
	off := req2.Deserialize(buff, 3)
	require.Equal(t, req, req2)
	require.Equal(t, off, len(buff))
}

func TestSerializeDeserializeApplyChangesRequest(t *testing.T) {
	req := ApplyChangesRequest{
		LeaderVersion: 4555,
		RegBatch: lsm.RegistrationBatch{
			Compaction: true,
			JobID:      "job-12345",
			Registrations: []lsm.RegistrationEntry{{
				Level:      23,
				TableID:    sst.SSTableID("sometableid1"),
				MaxVersion: 100001,
				KeyStart:   []byte("keystart1"),
				KeyEnd:     []byte("keyend1"),
			}, {
				Level:      12,
				TableID:    sst.SSTableID("sometableid2"),
				MaxVersion: 200002,
				KeyStart:   []byte("keystart2"),
				KeyEnd:     []byte("keyend2"),
			}},
			DeRegistrations: []lsm.RegistrationEntry{{
				Level:    23,
				TableID:  sst.SSTableID("sometableid11"),
				KeyStart: []byte("keystart3"),
				KeyEnd:   []byte("keyend3"),
			}, {
				Level:    27,
				TableID:  sst.SSTableID("sometableid12"),
				KeyStart: []byte("keystart4"),
				KeyEnd:   []byte("keyend4"),
			}},
		},
	}
	var buff []byte
	buff = append(buff, 1, 2, 3)
	buff = req.Serialize(buff)
	var req2 ApplyChangesRequest
	off := req2.Deserialize(buff, 3)
	require.Equal(t, req, req2)
	require.Equal(t, off, len(buff))
}

func TestSerializeDeserializeQueryTablesInRangeRequest(t *testing.T) {
	req := QueryTablesInRangeRequest{
		LeaderVersion: 567456,
		KeyStart:      []byte("keystart1"),
		KeyEnd:        []byte("keyend1"),
	}
	testSerializeDeserializeQueryTablesInRangeRequest(t, req)

	// And with nil ranges
	req = QueryTablesInRangeRequest{
		LeaderVersion: 456456,
		KeyStart:      nil,
		KeyEnd:        []byte("keyend1"),
	}
	testSerializeDeserializeQueryTablesInRangeRequest(t, req)

	req = QueryTablesInRangeRequest{
		LeaderVersion: 23424,
		KeyStart:      nil,
		KeyEnd:        nil,
	}
	testSerializeDeserializeQueryTablesInRangeRequest(t, req)
}

func testSerializeDeserializeQueryTablesInRangeRequest(t *testing.T, req QueryTablesInRangeRequest) {
	var buff []byte
	buff = append(buff, 1, 2, 3)
	buff = req.Serialize(buff)
	var req2 QueryTablesInRangeRequest
	off := req2.Deserialize(buff, 3)
	require.Equal(t, req, req2)
	require.Equal(t, off, len(buff))
}

func TestSerializeDeserializeRegisterTableListenerRequest(t *testing.T) {
	req := RegisterTableListenerRequest{
		LeaderVersion: 567456,
		TopicID:       123123,
		PartitionID:   34546,
		MemberID:      2321,
		ResetSequence: 123456,
	}
	var buff []byte
	buff = append(buff, 1, 2, 3)
	buff = req.Serialize(buff)
	var req2 RegisterTableListenerRequest
	off := req2.Deserialize(buff, 3)
	require.Equal(t, req, req2)
	require.Equal(t, off, len(buff))
}

func TestSerializeDeserializeRegisterTableListenerResponse(t *testing.T) {
	req := RegisterTableListenerResponse{
		LastReadableOffset: 234234,
	}
	var buff []byte
	buff = append(buff, 1, 2, 3)
	buff = req.Serialize(buff)
	var req2 RegisterTableListenerResponse
	off := req2.Deserialize(buff, 3)
	require.Equal(t, req, req2)
	require.Equal(t, off, len(buff))
}

func TestSerializeDeserializeGetOffsetsRequest(t *testing.T) {
	req := PrePushRequest{
		LeaderVersion: 4536,
		Infos: []offsets.GenerateOffsetTopicInfo{
			{
				TopicID: 1234,
				PartitionInfos: []offsets.GenerateOffsetPartitionInfo{
					{PartitionID: 23, NumOffsets: 345},
					{PartitionID: 45, NumOffsets: 455},
					{PartitionID: 567, NumOffsets: 23},
				},
			},
			{
				TopicID: 345,
				PartitionInfos: []offsets.GenerateOffsetPartitionInfo{
					{PartitionID: 76, NumOffsets: 2342},
				},
			},
			{
				TopicID: 45656,
				PartitionInfos: []offsets.GenerateOffsetPartitionInfo{
					{PartitionID: 879, NumOffsets: 12321},
					{PartitionID: 34, NumOffsets: 4536},
				},
			},
		},
		EpochInfos: []EpochInfo{
			{
				Key:   "consumer-group-1",
				Epoch: 123213,
			},
			{
				Key:   "consumer-group-2",
				Epoch: 23423,
			},
			{
				Key:   "consumer-group-3",
				Epoch: 34545,
			},
		},
	}
	var buff []byte
	buff = append(buff, 1, 2, 3)
	buff = req.Serialize(buff)
	var req2 PrePushRequest
	off := req2.Deserialize(buff, 3)
	require.Equal(t, req, req2)
	require.Equal(t, off, len(buff))
}

func TestSerializeDeserializeGetOffsetsResponse(t *testing.T) {
	resp := PrePushResponse{
		Offsets: []offsets.OffsetTopicInfo{
			{
				TopicID: 12323,
				PartitionInfos: []offsets.OffsetPartitionInfo{
					{
						PartitionID: 234,
						Offset:      234234,
					},
					{
						PartitionID: 345,
						Offset:      3453454,
					},
				},
			},
			{
				TopicID: 4356456,
				PartitionInfos: []offsets.OffsetPartitionInfo{
					{
						PartitionID: 678,
						Offset:      23556,
					},
					{
						PartitionID: 4567,
						Offset:      3455,
					},
				},
			},
		},
		Sequence: 2137632,
		EpochsOK: []bool{
			true, false, false, true, false, true, true,
		},
	}
	var buff []byte
	buff = append(buff, 1, 2, 3)
	buff = resp.Serialize(buff)
	var resp2 PrePushResponse
	off := resp2.Deserialize(buff, 3)
	require.Equal(t, resp, resp2)
	require.Equal(t, off, len(buff))
}

func TestSerializeDeserializeGetAllTopicInfosRequest(t *testing.T) {
	req := GetAllTopicInfosRequest{
		LeaderVersion: 123,
	}
	var buff []byte
	buff = append(buff, 1, 2, 3)
	buff = req.Serialize(buff)
	var req2 GetAllTopicInfosRequest
	off := req2.Deserialize(buff, 3)
	require.Equal(t, req, req2)
	require.Equal(t, off, len(buff))
}

func TestSerializeDeserializeGetAllTopicInfosResponse(t *testing.T) {
	req := GetAllTopicInfosResponse{
		TopicInfos: []topicmeta.TopicInfo{
			{
				ID:             1234,
				Name:           "topic1",
				PartitionCount: 12345,
				RetentionTime:  time.Duration(123123),
			},
			{
				ID:             2323,
				Name:           "topic2",
				PartitionCount: 45363546,
				RetentionTime:  time.Duration(234234),
			},
			{
				ID:             657456,
				Name:           "topic3",
				PartitionCount: 123,
				RetentionTime:  time.Duration(457456),
			},
		},
	}
	var buff []byte
	buff = append(buff, 1, 2, 3)
	buff = req.Serialize(buff)
	var req2 GetAllTopicInfosResponse
	off := req2.Deserialize(buff, 3)
	require.Equal(t, req, req2)
	require.Equal(t, off, len(buff))
}

func TestSerializeDeserializeGetTopicInfoRequest(t *testing.T) {
	req := GetTopicInfoRequest{
		LeaderVersion: 123,
		TopicName:     "some-topic",
	}
	var buff []byte
	buff = append(buff, 1, 2, 3)
	buff = req.Serialize(buff)
	var req2 GetTopicInfoRequest
	off := req2.Deserialize(buff, 3)
	require.Equal(t, req, req2)
	require.Equal(t, off, len(buff))
}

func TestSerializeDeserializeGetTopicInfoByIDRequest(t *testing.T) {
	req := GetTopicInfoByIDRequest{
		LeaderVersion: 123,
		TopicID:       123123,
	}
	var buff []byte
	buff = append(buff, 1, 2, 3)
	buff = req.Serialize(buff)
	var req2 GetTopicInfoByIDRequest
	off := req2.Deserialize(buff, 3)
	require.Equal(t, req, req2)
	require.Equal(t, off, len(buff))
}

func TestSerializeDeserializeGetTopicInfoResponseExistsTrue(t *testing.T) {
	resp := GetTopicInfoResponse{
		Sequence: 123,
		Exists:   true,
		Info: topicmeta.TopicInfo{
			ID:             1233,
			Name:           "some-topic",
			PartitionCount: 123123123,
			RetentionTime:  23445346,
		},
	}
	testSerializeDeserializeGetTopicInfoResponse(t, resp)
}

func TestSerializeDeserializeGetTopicInfoResponseExistsFalse(t *testing.T) {
	resp := GetTopicInfoResponse{
		Sequence: 123,
		Exists:   false,
		Info: topicmeta.TopicInfo{
			ID:             1233,
			Name:           "some-topic",
			PartitionCount: 123123123,
			RetentionTime:  23445346,
		},
	}
	testSerializeDeserializeGetTopicInfoResponse(t, resp)
}

func testSerializeDeserializeGetTopicInfoResponse(t *testing.T, resp GetTopicInfoResponse) {
	var buff []byte
	buff = append(buff, 1, 2, 3)
	buff = resp.Serialize(buff)
	var resp2 GetTopicInfoResponse
	off := resp2.Deserialize(buff, 3)
	require.Equal(t, resp, resp2)
	require.Equal(t, off, len(buff))
}

func TestSerializeDeserializeCreateTopicRequest(t *testing.T) {
	req := CreateTopicRequest{
		LeaderVersion: 123,
		Info: topicmeta.TopicInfo{
			ID:             23423,
			Name:           "some-topic",
			PartitionCount: 123123,
			RetentionTime:  1212312123,
		},
	}
	var buff []byte
	buff = append(buff, 1, 2, 3)
	buff = req.Serialize(buff)
	var req2 CreateTopicRequest
	off := req2.Deserialize(buff, 3)
	require.Equal(t, req, req2)
	require.Equal(t, off, len(buff))
}

func TestSerializeDeserializeDeleteTopicRequest(t *testing.T) {
	req := DeleteTopicRequest{
		LeaderVersion: 123,
		TopicName:     "some-topic",
	}
	var buff []byte
	buff = append(buff, 1, 2, 3)
	buff = req.Serialize(buff)
	var req2 DeleteTopicRequest
	off := req2.Deserialize(buff, 3)
	require.Equal(t, req, req2)
	require.Equal(t, off, len(buff))
}

func TestSerializeDeserializeGetGroupCoordinatorInfoRequest(t *testing.T) {
	req := GetGroupCoordinatorInfoRequest{
		LeaderVersion: 123,
		GroupID:       "some-group-id",
	}
	var buff []byte
	buff = append(buff, 1, 2, 3)
	buff = req.Serialize(buff)
	var req2 GetGroupCoordinatorInfoRequest
	off := req2.Deserialize(buff, 3)
	require.Equal(t, req, req2)
	require.Equal(t, off, len(buff))
}

func TestSerializeDeserializeGetGroupCoordinatorInfoResponse(t *testing.T) {
	req := GetGroupCoordinatorInfoResponse{
		MemberID:   2134,
		Address:    "some-address-812721",
		GroupEpoch: 82378248,
	}
	var buff []byte
	buff = append(buff, 1, 2, 3)
	buff = req.Serialize(buff)
	var req2 GetGroupCoordinatorInfoResponse
	off := req2.Deserialize(buff, 3)
	require.Equal(t, req, req2)
	require.Equal(t, off, len(buff))
}

func TestSerializeDeserializeTableRegisteredNotification(t *testing.T) {
	notif := TablesRegisteredNotification{
		LeaderVersion: 23,
		Sequence:      1232343,
		TableIDs:      []sst.SSTableID{[]byte("some_table_id")},
		Infos: []offsets.OffsetTopicInfo{
			{
				TopicID: 1234,
				PartitionInfos: []offsets.OffsetPartitionInfo{
					{
						PartitionID: 234,
						Offset:      66788,
					},
					{
						PartitionID: 56756,
						Offset:      23432,
					},
				},
			},
			{
				TopicID: 345435,
				PartitionInfos: []offsets.OffsetPartitionInfo{
					{
						PartitionID: 5465,
						Offset:      678678,
					},
				},
			},
		},
	}
	var buff []byte
	buff = append(buff, 1, 2, 3)
	buff = notif.Serialize(buff)
	var notif2 TablesRegisteredNotification
	off := notif2.Deserialize(buff, 3)
	require.Equal(t, notif, notif2)
	require.Equal(t, off, len(buff))
}

func TestSerializeDeserializeGenerateSequenceRequest(t *testing.T) {
	req := GenerateSequenceRequest{
		LeaderVersion: 1232,
		SequenceName:  "some_sequence_name",
	}
	var buff []byte
	buff = append(buff, 1, 2, 3)
	buff = req.Serialize(buff)
	var req2 GenerateSequenceRequest
	off := req2.Deserialize(buff, 3)
	require.Equal(t, req, req2)
	require.Equal(t, off, len(buff))
}

func TestSerializeDeserializeGenerateSequenceResponse(t *testing.T) {
	req := GenerateSequenceResponse{
		Sequence: 2342134124,
	}
	var buff []byte
	buff = append(buff, 1, 2, 3)
	buff = req.Serialize(buff)
	var req2 GenerateSequenceResponse
	off := req2.Deserialize(buff, 3)
	require.Equal(t, req, req2)
	require.Equal(t, off, len(buff))
}

func TestSerializeDeserializeGetOffsetInfoRequest(t *testing.T) {
	req := GetOffsetInfoRequest{
		LeaderVersion: 123213,
		GetOffsetTopicInfos: []offsets.GetOffsetTopicInfo{
			{
				TopicID:      1234,
				PartitionIDs: []int{234, 234345, 34534, 23},
			},
		},
	}
	var buff []byte
	buff = append(buff, 1, 2, 3)
	buff = req.Serialize(buff)
	var req2 GetOffsetInfoRequest
	off := req2.Deserialize(buff, 3)
	require.Equal(t, req, req2)
	require.Equal(t, off, len(buff))
}

func TestSerializeDeserializeGetOffsetInfoResponse(t *testing.T) {
	req := GetOffsetInfoResponse{
		OffsetInfos: []offsets.OffsetTopicInfo{
			{
				TopicID: 1234,
				PartitionInfos: []offsets.OffsetPartitionInfo{
					{
						PartitionID: 234,
						Offset:      42354,
					},
					{
						PartitionID: 3232,
						Offset:      424354,
					},
					{
						PartitionID: 23,
						Offset:      34534,
					},
				},
			},
		},
	}
	var buff []byte
	buff = append(buff, 1, 2, 3)
	buff = req.Serialize(buff)
	var req2 GetOffsetInfoResponse
	off := req2.Deserialize(buff, 3)
	require.Equal(t, req, req2)
	require.Equal(t, off, len(buff))
}
