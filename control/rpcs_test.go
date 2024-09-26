package control

import (
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/offsets"
	"github.com/spirit-labs/tektite/sst"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestSerializeDeserializeRegisterL0Request(t *testing.T) {
	req := RegisterL0Request{
		ClusterVersion: 4555,
		OffsetInfos: []offsets.UpdateWrittenOffsetInfo{
			{
				TopicID: 1234,
				PartitionID: 12,
				OffsetStart: 1001,
				NumOffsets:  213,
			},
			{
				TopicID: 1234,
				PartitionID: 0,
				OffsetStart: 23,
				NumOffsets:  456,
			},
			{
				TopicID: 1234,
				PartitionID: 45,
				OffsetStart: 3453,
				NumOffsets:  32334,
			},
			{
				TopicID: 234234,
				PartitionID: 45,
				OffsetStart: 23423,
				NumOffsets:  234,
			},
			{
				TopicID: 123,
				PartitionID: 34534,
				OffsetStart: 433,
				NumOffsets:  444,
			},
		},
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
		ClusterVersion: 4555,
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
		ClusterVersion: 567456,
		KeyStart:       []byte("keystart1"),
		KeyEnd:         []byte("keyend1"),
	}
	testSerializeDeserializeQueryTablesInRangeRequest(t, req)

	// And with nil ranges
	req = QueryTablesInRangeRequest{
		ClusterVersion: 456456,
		KeyStart:       nil,
		KeyEnd:         []byte("keyend1"),
	}
	testSerializeDeserializeQueryTablesInRangeRequest(t, req)

	req = QueryTablesInRangeRequest{
		ClusterVersion: 23424,
		KeyStart:       nil,
		KeyEnd:         nil,
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

	// And with null ranges
}

func TestSerializeDeserializeGetOffsetsRequest(t *testing.T) {
	req := GetOffsetsRequest{
		CacheNum:       123,
		ClusterVersion: 4536,
		Infos: []offsets.GetOffsetTopicInfo{
			{TopicID: 3, PartitionID: 23, NumOffsets: 345},
			{TopicID: 7, PartitionID: 54, NumOffsets: 45},
			{TopicID: 0, PartitionID: 2, NumOffsets: 45645},
		},
	}
	var buff []byte
	buff = append(buff, 1, 2, 3)
	buff = req.Serialize(buff)
	var req2 GetOffsetsRequest
	off := req2.Deserialize(buff, 3)
	require.Equal(t, req, req2)
	require.Equal(t, off, len(buff))
}

func TestSerializeDeserializeGetOffsetsResponse(t *testing.T) {
	resp := GetOffsetsResponse{Offsets: []int64{3423423, 234234, 343453, 456456, 768678, 6789769}}
	var buff []byte
	buff = append(buff, 1, 2, 3)
	buff = resp.Serialize(buff)
	var resp2 GetOffsetsResponse
	off := resp2.Deserialize(buff, 3)
	require.Equal(t, resp, resp2)
	require.Equal(t, off, len(buff))
}
