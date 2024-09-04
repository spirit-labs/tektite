package controller

type API interface {

	GetOffsets(topicName string, partition int) (int, error)
}
