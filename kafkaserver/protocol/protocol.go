package protocol

const (
	APIKeyProduce          = 0
	APIKeyFetch            = 1
	APIKeyListOffsets      = 2
	APIKeyMetadata         = 3
	APIKeyOffsetCommit     = 8
	APIKeyOffsetFetch      = 9
	APIKeyFindCoordinator  = 10
	ApiKeyJoinGroup        = 11
	ApiKeyHeartbeat        = 12
	ApiKeyLeaveGroup       = 13
	ApiKeySyncGroup        = 14
	APIKeySaslHandshake    = 17
	APIKeyAPIVersions      = 18
	APIKeySaslAuthenticate = 36
)

const (
	ErrorCodeUnknownServerError          = -1
	ErrorCodeNone                        = 0
	ErrorCodeUnknownTopicOrPartition     = 3
	ErrorCodeLeaderNotAvailable          = 5
	ErrorCodeNotLeaderOrFollower         = 6
	ErrorCodeCoordinatorNotAvailable     = 15
	ErrorCodeNotCoordinator              = 16
	ErrorCodeIllegalGeneration           = 22
	ErrorCodeInconsistentGroupProtocol   = 23
	ErrorCodeUnknownMemberID             = 25
	ErrorCodeInvalidSessionTimeout       = 26
	ErrorCodeRebalanceInProgress         = 27
	ErrorCodeUnsupportedForMessageFormat = 43
	ErrorCodeGroupIDNotFound             = 69
)

var SupportedAPIKeys = []ApiVersionsResponseApiVersion{
	{ApiKey: APIKeyProduce, MinVersion: 3, MaxVersion: 3},
	{ApiKey: APIKeyFetch, MinVersion: 4, MaxVersion: 4},
	{ApiKey: APIKeyAPIVersions, MinVersion: 0, MaxVersion: 3},
	{ApiKey: APIKeySaslAuthenticate, MinVersion: 0, MaxVersion: 0},
	{ApiKey: APIKeyMetadata, MinVersion: 3, MaxVersion: 3},
	{ApiKey: APIKeyFindCoordinator, MinVersion: 0, MaxVersion: 0},
	{ApiKey: ApiKeyJoinGroup, MinVersion: 0, MaxVersion: 0},
	{ApiKey: ApiKeySyncGroup, MinVersion: 0, MaxVersion: 0},
	{ApiKey: ApiKeyHeartbeat, MinVersion: 0, MaxVersion: 0},
	{ApiKey: APIKeyListOffsets, MinVersion: 1, MaxVersion: 1},
	{ApiKey: APIKeyOffsetCommit, MinVersion: 2, MaxVersion: 2},
	{ApiKey: APIKeyOffsetFetch, MinVersion: 1, MaxVersion: 1},
	{ApiKey: ApiKeyLeaveGroup, MinVersion: 0, MaxVersion: 0},
}

type Records struct {
	Data [][]byte
}
