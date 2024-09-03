package opers

import (
	"encoding/binary"
	"github.com/spirit-labs/tektite/common"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMaybeHandleIdempotentProducerBatch(t *testing.T) {
	testCases := []struct {
		name                   string
		partitionID            int
		processorID            int
		producerID             int
		baseSequence           int
		lastOffsetDelta        int
		initialSequenceNumber  int
		expectedSequenceNumber int
		expectedError          common.ErrCode
		expectMappingCreation  bool
	}{
		{
			name:                   "valid",
			partitionID:            0,
			processorID:            0,
			producerID:             1234,
			baseSequence:           10,
			lastOffsetDelta:        5,
			expectedSequenceNumber: 15, // baseSequence + lastOffsetDelta
			expectedError:          -1,
			expectMappingCreation:  true,
		},
		{
			name:                   "duplicate sequence",
			partitionID:            0,
			processorID:            0,
			producerID:             1234,
			baseSequence:           10,
			lastOffsetDelta:        5,
			initialSequenceNumber:  15,
			expectedSequenceNumber: 15,
			expectedError:          common.DuplicateSequence,
			expectMappingCreation:  true,
		},
		{
			name:                   "out of order sequence",
			partitionID:            0,
			processorID:            0,
			producerID:             1234,
			baseSequence:           90,
			lastOffsetDelta:        5,
			initialSequenceNumber:  15,
			expectedSequenceNumber: 15,
			expectedError:          common.OutOfOrderSequence,
			expectMappingCreation:  true,
		},
		{
			name:                   "not idempotent",
			partitionID:            0,
			processorID:            0,
			producerID:             -1, // No idempotency
			expectedSequenceNumber: 0,
			expectedError:          -1,
			expectMappingCreation:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			k := &KafkaInOperator{
				partitionProducerMapping: make([]map[int]map[int]int, 1),
			}

			if tc.initialSequenceNumber > 0 {
				k.partitionProducerMapping[tc.processorID] = make(map[int]map[int]int)
				k.partitionProducerMapping[tc.processorID][tc.partitionID] = map[int]int{
					tc.producerID: tc.initialSequenceNumber,
				}
			}

			bytes := make([]byte, 58)
			binary.BigEndian.PutUint64(bytes[43:51], uint64(tc.producerID))
			binary.BigEndian.PutUint32(bytes[53:57], uint32(tc.baseSequence))
			binary.BigEndian.PutUint32(bytes[23:27], uint32(tc.lastOffsetDelta))

			err := k.maybeHandleIdempotentProducerBatch(tc.partitionID, tc.processorID, bytes)

			if tc.expectedError > 0 {
				require.Error(t, err, "expected error, got nil")
				require.True(t, common.IsTektiteErrorWithCode(err, tc.expectedError), "expected specific error")
			} else {
				require.NoError(t, err, "unexpected error")
			}

			if tc.expectMappingCreation {
				require.NotNil(t, k.partitionProducerMapping[tc.processorID][tc.partitionID], "expected mapping to be created")
				if tc.producerID > -1 {
					require.Equal(t, tc.expectedSequenceNumber, k.partitionProducerMapping[tc.processorID][tc.partitionID][tc.producerID], "sequence number mismatch")
				}
			} else {
				require.Nil(t, k.partitionProducerMapping[tc.processorID][tc.partitionID], "expected no mapping modification")
			}
		})
	}
}
