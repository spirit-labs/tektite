package msggen

import (
	"context"
	"fmt"
	"github.com/spirit-labs/tektite/common"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spirit-labs/tektite/errors"

	kafkaclient "github.com/segmentio/kafka-go"
	"github.com/spirit-labs/tektite/kafka"
	log "github.com/spirit-labs/tektite/logger"
)

// MessageGenerator - quick and dirty Kafka message generator for demos, tests etc
type MessageGenerator interface {
	GenerateMessage(scope int32, index int64, rnd *rand.Rand) (*kafka.Message, error)
	Name() string
	Init()
}

type GenManager struct {
	lock       sync.Mutex
	generators map[string]MessageGenerator
}

func NewGenManager() (*GenManager, error) {
	gm := &GenManager{generators: make(map[string]MessageGenerator)}
	if err := gm.RegisterGenerators(); err != nil {
		return nil, errors.WithStack(err)
	}
	return gm, nil
}

func (gm *GenManager) RegisterGenerator(gen MessageGenerator) error {
	gm.lock.Lock()
	defer gm.lock.Unlock()
	if _, ok := gm.generators[gen.Name()]; ok {
		return errors.Errorf("generator already registered with name %s", gen.Name())
	}
	gm.generators[gen.Name()] = gen
	return nil
}

func (gm *GenManager) RegisterGenerators() error {
	return gm.RegisterGenerator(&PaymentGenerator{})
}

func (gm *GenManager) ProduceMessages(genName string, topicName string, partitions int, delay time.Duration,
	numMessages int64, indexStart int64, kafkaProps map[string]string) error {
	gm.lock.Lock()
	defer gm.lock.Unlock()

	gen, ok := gm.generators[genName]
	if !ok {
		return errors.Errorf("no generator with registered with name %s", genName)
	}

	msgsSent := int64(0)
	errChan := make(chan error)
	doneChan := make(chan struct{})
	producer := &kafkaclient.Writer{
		Async: true,
		Completion: func(messages []kafkaclient.Message, err error) {
			sent := atomic.AddInt64(&msgsSent, int64(len(messages)))
			if err != nil {
				errChan <- err
			}
			if sent >= numMessages {
				close(errChan)
				log.Infof("%d/%d messages sent to topic %s", sent, numMessages, topicName)
				close(doneChan)
			}
		},
	}
	for k, v := range kafkaProps {
		if err := setProperty(producer, k, v); err != nil {
			return errors.WithStack(err)
		}
	}
	common.Go(func() {
		for {
			select {
			case <-time.After(time.Second):
				sent := atomic.LoadInt64(&msgsSent)
				log.Infof("%d/%d messages sent to topic %s", sent, numMessages, topicName)
			case <-doneChan:
				return
			}
		}
	})
	rnd := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	for i := indexStart; i < indexStart+numMessages; i++ {
		msg, err := gen.GenerateMessage(0, i, rnd)
		if err != nil {
			return errors.WithStack(err)
		}
		hash := common.DefaultHash(msg.Key)
		part := common.CalcPartition(hash, partitions)
		kheaders := make([]kafkaclient.Header, len(msg.Headers))
		for i, hdr := range msg.Headers {
			kheaders[i] = kafkaclient.Header{
				Key:   hdr.Key,
				Value: hdr.Value,
			}
		}
		kmsg := kafkaclient.Message{
			Partition: int(part),
			Topic:     topicName,
			Value:     msg.Value,
			Key:       msg.Key,
			Time:      msg.TimeStamp,
			Headers:   kheaders,
		}

		if e := producer.WriteMessages(context.Background(), kmsg); e != nil {
			return errors.WithStack(e)
		}

		if delay != 0 {
			time.Sleep(delay)
		}
	}
	if err := producer.Close(); err != nil {
		return errors.WithStack(err)
	}
	failed := false
	for err := range errChan {
		log.Errorf("error producing messages: %+v", err)
		failed = true
	}
	if failed {
		return errors.Error("failed to send all messages")
	}
	return nil
}

func setProperty(cfg *kafkaclient.Writer, k, v string) error {
	switch k {
	case "bootstrap.servers":
		cfg.Addr = kafkaclient.TCP(strings.Split(v, ",")...)
	default:
		return errors.NewInvalidConfigurationError(fmt.Sprintf("unsupported segmentio/kafka-go client option: %s:%s", k, v))
	}
	return nil
}
