package streammeta

type TopicInfo struct {
	TopicID        int
	PartitionCount int
}

type TopicInfoProvider interface {
	GetTopicInfo(topicName string) (TopicInfo, bool)
	GetAllTopics() ([]TopicInfo, error)
}

type SimpleTopicInfoProvider struct {
	Infos map[string]TopicInfo
}

func (t *SimpleTopicInfoProvider) GetTopicInfo(topicName string) (TopicInfo, bool) {
	info, ok := t.Infos[topicName]
	return info, ok
}

func (t *SimpleTopicInfoProvider) GetAllTopics() ([]TopicInfo, error) {
	var allTopics []TopicInfo
	for _, info := range t.Infos {
		allTopics = append(allTopics, info)
	}
	return allTopics, nil
}
