package shard

import (
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/sst"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSerializeDeserializeApplyChangesRequest(t *testing.T) {
	req := ApplyChangesRequest{
		ShardID: 12345,
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
		ShardID:  123345,
		ClusterVersion: 567456,
		KeyStart: []byte("keystart1"),
		KeyEnd:   []byte("keyend1"),
	}
	testSerializeDeserializeQueryTablesInRangeRequest(t, req)

	// And with nil ranges
	req = QueryTablesInRangeRequest{
		ShardID:  123345,
		ClusterVersion: 456456,
		KeyStart: nil,
		KeyEnd:   []byte("keyend1"),
	}
	testSerializeDeserializeQueryTablesInRangeRequest(t, req)

	req = QueryTablesInRangeRequest{
		ShardID:  123345,
		ClusterVersion: 23424,
		KeyStart: nil,
		KeyEnd:   nil,
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
