package topicmeta

import (
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/transport"
	"sync"
)

/*
LocalCache is active on all agents and caches topic metadata so each call to get topic info doesn't result in a
potentially remote call to the controller.
Local caches also receive notifications from the manager whenever topics are created or deleted, so they can add
or remove cached topic metadata. A sequence number is also passed in every notification and returned from each
call to GetTopicInfo, and maintained in the cache. If the received sequence number isn't expected, then it means
a notification was missed and the cache is invalidated.
*/
type LocalCache struct {
	lock                 sync.RWMutex
	topicInfos           map[string]TopicInfo
	controlClientFactory ControllerClientFactory
	cl                   ControllerClient
	lastSequence         int
}

func NewLocalCache(controlClientFactory ControllerClientFactory) *LocalCache {
	return &LocalCache{
		topicInfos:           make(map[string]TopicInfo),
		controlClientFactory: controlClientFactory,
		lastSequence:         -1,
	}
}

type ControllerClientFactory func() (ControllerClient, error)

type ControllerClient interface {
	GetTopicInfo(topicName string) (TopicInfo, int, bool, error)
	Close() error
}

func (l *LocalCache) getClient() (ControllerClient, error) {
	if l.cl != nil {
		return l.cl, nil
	}
	cl, err := l.controlClientFactory()
	if err != nil {
		return nil, err
	}
	l.cl = cl
	return cl, nil
}

func (l *LocalCache) GetTopicInfo(topicName string) (TopicInfo, bool, error) {
	info, ok := l.getCachedTopicInfo(topicName)
	if ok {
		return info, true, nil
	}
	return l.getTopicInfoFromController(topicName)
}

func (l *LocalCache) getCachedTopicInfo(topicName string) (TopicInfo, bool) {
	l.lock.RLock()
	defer l.lock.RUnlock()
	info, ok := l.topicInfos[topicName]
	return info, ok
}

func (l *LocalCache) getTopicInfoFromController(topicName string) (TopicInfo, bool, error) {
	l.lock.Lock()
	defer l.lock.Unlock()
	info, ok := l.topicInfos[topicName]
	if ok {
		return info, true, nil
	}
	cl, err := l.getClient()
	if err != nil {
		return TopicInfo{}, false, err
	}
	var sequence int
	var exists bool
	info, sequence, exists, err = cl.GetTopicInfo(topicName)
	if err != nil {
		// We always close on error if retries occur, new connection will be created
		if err := l.cl.Close(); err != nil {
			log.Warnf("failed to close client: %v", err)
		}
		l.cl = nil
		return TopicInfo{}, false, err
	}
	if exists {
		l.topicInfos[topicName] = info
		l.lastSequence = sequence
	}
	return info, exists, nil
}

func (l *LocalCache) HandleTopicAdded(_ *transport.ConnectionContext, buff []byte, responseBuff []byte,
	responseWriter transport.ResponseWriter) error {
	l.lock.Lock()
	defer l.lock.Unlock()
	var notif TopicNotification
	notif.Deserialize(buff, 0)
	if l.lastSequence != -1 && notif.Sequence != l.lastSequence+1 {
		// Invalidate - we missed a sequence
		l.topicInfos = map[string]TopicInfo{}
	} else {
		l.topicInfos[notif.Info.Name] = notif.Info
	}
	l.lastSequence = notif.Sequence
	return responseWriter(responseBuff, nil)
}

func (l *LocalCache) HandleTopicDeleted(_ *transport.ConnectionContext, buff []byte, responseBuff []byte,
	responseWriter transport.ResponseWriter) error {
	l.lock.Lock()
	defer l.lock.Unlock()
	var notif TopicNotification
	notif.Deserialize(buff, 0)
	if notif.Sequence > l.lastSequence+1 {
		// Invalidate - we missed a sequence
		l.topicInfos = map[string]TopicInfo{}
	} else {
		delete(l.topicInfos, notif.Info.Name)
	}
	l.lastSequence = notif.Sequence
	return responseWriter(responseBuff, nil)
}

func (l *LocalCache) getTopicInfos() map[string]TopicInfo {
	l.lock.RLock()
	defer l.lock.RUnlock()
	copied := make(map[string]TopicInfo)
	for k, v := range l.topicInfos {
		copied[k] = v
	}
	return copied
}
