package transport

const (
	HandlerIDControllerRegisterL0Table = iota + 10
	HandlerIDControllerApplyChanges
	HandlerIDControllerQueryTablesInRange
	HandlerIDControllerGetOffsets
	HandlerIDControllerPollForJob
	HandlerIDControllerGetTopicInfo
	HandlerIDControllerCreateTopic
	HandlerIDControllerDeleteTopic
	HandlerIDMetaLocalCacheTopicAdded
	HandlerIDMetaLocalCacheTopicDeleted
)
