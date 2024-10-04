package common

// Reserved SlabIDs
const (
	CommandsSlabID     = 1
	KafkaOffsetsSlabID = 2
	ReplSeqSlabID      = 3
	StreamMetaSlabID   = 4
	UserCredsSlabID    = 5
	TopicMetadataSlabID = 6
	UserSlabIDBase     = 1000
)

// Reserved ReceiverIDs
const (
	CommandsReceiverID        = 1
	CommandsDeleteReceiverID  = 2
	LevelManagerReceiverID    = 3
	DummyReceiverID           = 4
	KafkaOffsetsReceiverID    = 5
	DeleteSlabReceiverID      = 6
	UserCredsReceiverID       = 7
	UserCredsDeleteReceiverID = 8
	UserReceiverIDBase        = 1000
)
