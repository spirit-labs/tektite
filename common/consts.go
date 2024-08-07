package common

// Reserved SlabIDs
const (
	CommandsSlabID     = 1
	KafkaOffsetsSlabID = 2
	ReplSeqSlabID      = 3
	StreamMetaSlabID   = 4
	UserSlabIDBase     = 1000
)

// Reserved ReceiverIDs
const (
	CommandsReceiverID       = 1
	CommandsDeleteReceiverID = 2
	LevelManagerReceiverID   = 3
	DummyReceiverID          = 4
	KafkaOffsetsReceiverID   = 5
	DeleteSlabReceiverID     = 6
	UserReceiverIDBase       = 1000
)
