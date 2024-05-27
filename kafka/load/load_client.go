package load

import (
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/kafka"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/msggen"
	"math"
	"math/rand"
	"sync"
	"time"
)

var _ kafka.MessageClient = &MessageProviderFactory{}

var _ kafka.ClientFactory = NewMessageProviderFactory

type MessageProviderFactory struct {
	bufferSize             int
	properties             map[string]string
	maxMessagesPerConsumer int64
	uniqueIDsPerPartition  int64
	messageGeneratorName   string
	committedOffsets       map[int32]int64
	committedOffsetsLock   sync.Mutex
	messageProviders       []*MessageProvider
}

const (
	produceTimeout                 = 100 * time.Millisecond
	bufferSizePropName             = "tektite.loadclient.buffersize"
	uniqueIDsPerPartitionPropName  = "tektite.loadclient.uniqueidsperpartition"
	maxMessagesPerConsumerPropName = "tektite.loadclient.maxmessagesperconsumer"
	messageGeneratorPropName       = "tektite.loadclient.messagegenerator"
	defaultMessageGeneratorName    = "simple"
)

var factoriesLock sync.Mutex

var factories []*MessageProviderFactory

func Factories() []*MessageProviderFactory {
	factoriesLock.Lock()
	defer factoriesLock.Unlock()
	return factories
}

func NewMessageProviderFactory(_ string, properties map[string]string) (kafka.MessageClient, error) {
	bufferSize, err := common.GetOrDefaultIntProperty(bufferSizePropName, properties, math.MaxInt64)
	if err != nil {
		return nil, err
	}
	uniqueIDsPerPartition, err := common.GetOrDefaultIntProperty(uniqueIDsPerPartitionPropName, properties, math.MaxInt64)
	if err != nil {
		return nil, err
	}
	maxMessagesPerConsumer, err := common.GetOrDefaultIntProperty(maxMessagesPerConsumerPropName, properties, math.MaxInt64)
	if err != nil {
		return nil, err
	}
	msgGeneratorName, ok := properties[messageGeneratorPropName]
	if !ok {
		msgGeneratorName = defaultMessageGeneratorName
	}
	fact := &MessageProviderFactory{
		bufferSize:             bufferSize,
		properties:             properties,
		uniqueIDsPerPartition:  int64(uniqueIDsPerPartition),
		maxMessagesPerConsumer: int64(maxMessagesPerConsumer),
		messageGeneratorName:   msgGeneratorName,
		committedOffsets:       map[int32]int64{},
	}
	factoriesLock.Lock()
	defer factoriesLock.Unlock()
	factories = append(factories, fact)
	return fact, nil
}

func (l *MessageProviderFactory) NewMessageProvider(partitions []int, _ []int64) (kafka.MessageProvider, error) {
	l.committedOffsetsLock.Lock()
	defer l.committedOffsetsLock.Unlock()
	msgs := make(chan *kafka.Message, l.bufferSize)
	offsets := make([]int64, len(partitions))
	for i, partitionID := range partitions {
		offsets[i] = l.committedOffsets[int32(partitionID)] + 1
	}
	rnd := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	msgGen, err := l.getMessageGenerator(l.messageGeneratorName)
	if err != nil {
		return nil, err
	}
	msgGen.Init()
	mp := &MessageProvider{
		factory:               l,
		msgs:                  msgs,
		partitions:            partitions,
		numPartitions:         len(partitions),
		offsets:               offsets,
		uniqueIDsPerPartition: l.uniqueIDsPerPartition,
		maxMessages:           l.maxMessagesPerConsumer,
		rnd:                   rnd,
		msgGenerator:          msgGen,
		deliveredOffsets:      map[int32]int64{},
	}
	l.messageProviders = append(l.messageProviders, mp)
	return mp, nil
}

func (l *MessageProviderFactory) NewMessageProducer(int, time.Duration, time.Duration) (kafka.MessageProducer, error) {
	panic("not implemented")
}

func (l *MessageProviderFactory) getMessageGenerator(name string) (msggen.MessageGenerator, error) {
	switch name {
	case "simple":
		return &simpleGenerator{uniqueIDsPerPartition: l.uniqueIDsPerPartition}, nil
	case "payments":
		return &paymentsGenerator{uniqueIDsPerPartition: l.uniqueIDsPerPartition}, nil
	default:
		return nil, errors.Errorf("unknown message generator name %s", name)
	}
}

type MessageProvider struct {
	factory               *MessageProviderFactory
	msgs                  chan *kafka.Message
	running               common.AtomicBool
	numPartitions         int
	partitions            []int
	offsets               []int64
	sequence              int64
	uniqueIDsPerPartition int64
	maxMessages           int64
	msgGenerator          msggen.MessageGenerator
	rnd                   *rand.Rand
	msgLock               sync.Mutex
	deliveredOffsets      map[int32]int64
}

func (l *MessageProvider) GetMessage(pollTimeout time.Duration) (*kafka.Message, error) {
	select {
	case msg := <-l.msgs:
		if msg == nil {
			// Messages channel was closed - probably max number of configured messages was exceeded
			// In this case we don't want to busy loop, so we introduce a delay
			time.Sleep(pollTimeout)
		} else {
			l.msgLock.Lock()
			l.deliveredOffsets[msg.PartInfo.PartitionID] = msg.PartInfo.Offset
			l.msgLock.Unlock()
		}
		return msg, nil
	case <-time.After(pollTimeout):
		return nil, nil
	}
}

func (l *MessageProvider) Stop() error {
	return nil
}

func (l *MessageProvider) Start() error {
	l.running.Set(true)
	common.Go(l.genLoop)
	return nil
}

func (l *MessageProvider) Close() error {
	l.msgLock.Lock()
	defer l.msgLock.Unlock()
	l.running.Set(false)
	return nil
}

func (l *MessageProvider) genLoop() {
	var msgCount int64
	var msg *kafka.Message
	for l.running.Get() && msgCount < l.maxMessages {
		if msg == nil {
			var err error
			msg, err = l.genMessage()
			if err != nil {
				log.Errorf("failed to generate message %+v", err)
				return
			}
		}
		select {
		case l.msgs <- msg:
			msgCount++
			msg = nil
		case <-time.After(produceTimeout):
		}
	}
	close(l.msgs)
}

func (l *MessageProvider) genMessage() (*kafka.Message, error) {
	index := l.sequence % int64(l.numPartitions)
	partition := l.partitions[index]
	offset := l.offsets[index]

	msg, err := l.msgGenerator.GenerateMessage(int32(partition), offset, l.rnd)
	if err != nil {
		return nil, err
	}
	l.offsets[index]++
	l.sequence++

	return msg, nil
}
