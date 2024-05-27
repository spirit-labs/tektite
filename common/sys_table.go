package common

// Reserved SlabIDs
const (
	StreamOffsetSequenceSlabID = 1
	BackfillOffsetSlabID       = 2
	CommandsSlabID             = 3
	KafkaOffsetsSlabID         = 5
	ReplSeqSlabID              = 6
	StreamMetaSlabID           = 7
	UserSlabIDBase             = 1000
)

// Reserved ReceiverIDs
const (
	CommandsReceiverID       = 1
	CommandsDeleteReceiverID = 2
	LevelManagerReceiverID   = 3
	DummyReceiverID          = 4
	KafkaOffsetsReceiverID   = 5
	UserReceiverIDBase       = 1000
)
