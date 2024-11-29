package integ

import (
	kafkago "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/spirit-labs/tektite/kafka"
	"strconv"
	"time"
)

type KafkaGoProducer struct {
	producer *kafkago.Producer
}

func NewKafkaGoProducer(address string, tlsEnabled bool, serverCertFile string, clientCertFile string, clientPrivateKeyFile string) (Producer, error) {
	cm := kafkago.ConfigMap{
		"partitioner":        "murmur2_random", // This matches the default hash algorithm we use, and same as Java client
		"bootstrap.servers":  address,
		"acks":               "all",
		"enable.idempotence": strconv.FormatBool(true),
	}
	if tlsEnabled {
		cm = configureConfigureForTls(cm, serverCertFile, clientCertFile, clientPrivateKeyFile)
	}
	producer, err := kafkago.NewProducer(&cm)
	if err != nil {
		return nil, err
	}
	return &KafkaGoProducer{producer}, nil
}

func (k *KafkaGoProducer) Produce(topicName string, messages []kafka.Message) error {
	deliveryChan := make(chan kafkago.Event, len(messages))
	for _, km := range messages {
		msg := &kafkago.Message{
			TopicPartition: kafkago.TopicPartition{Topic: &topicName, Partition: kafkago.PartitionAny},
			Key:            km.Key,
			Value:          km.Value,
			Timestamp:      km.TimeStamp,
		}
		for _, hdr := range km.Headers {
			msg.Headers = append(msg.Headers, kafkago.Header{Key: hdr.Key, Value: hdr.Value})
		}
		err := k.producer.Produce(msg, deliveryChan)
		if err != nil {
			return err
		}
	}
	for i := 0; i < len(messages); i++ {
		e := <-deliveryChan
		m := e.(*kafkago.Message)
		if m.TopicPartition.Error != nil {
			return m.TopicPartition.Error
		}
	}
	return nil
}

func (k *KafkaGoProducer) Close() error {
	k.producer.Close()
	return nil
}

type KafkaGoConsumer struct {
	consumer *kafkago.Consumer
}

func NewKafkaGoConsumer(address string, topicName string, groupID string, tlsEnabled bool, serverCertFile string, clientCertFile string, clientPrivateKeyFile string) (Consumer, error) {
	cm := kafkago.ConfigMap{
		"bootstrap.servers":  address,
		"group.id":           groupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
		//"debug":              "all",
	}
	if tlsEnabled {
		cm = configureConfigureForTls(cm, serverCertFile, clientCertFile, clientPrivateKeyFile)
	}
	consumer, err := kafkago.NewConsumer(&cm)
	if err != nil {
		return nil, err
	}
	if err := consumer.Subscribe(topicName, nil); err != nil {
		return nil, err
	}
	return &KafkaGoConsumer{consumer}, nil
}

func (k *KafkaGoConsumer) Fetch(timeout time.Duration) (*kafka.Message, error) {
	msg, err := k.consumer.ReadMessage(timeout)
	if err != nil {
		if err.(kafkago.Error).Code() == kafkago.ErrTimedOut {
			// not an error
			return nil, nil
		}
		return nil, err
	}
	km := &kafka.Message{
		PartInfo: kafka.PartInfo{
			PartitionID: msg.TopicPartition.Partition,
			Offset:      int64(msg.TopicPartition.Offset),
		},
		TimeStamp: msg.Timestamp,
		Key:       msg.Key,
		Value:     msg.Value,
	}
	for _, hdr := range msg.Headers {
		km.Headers = append(km.Headers, kafka.MessageHeader{
			Key:   hdr.Key,
			Value: hdr.Value,
		})
	}
	return km, nil
}

func (k *KafkaGoConsumer) Close() error {
	return k.consumer.Close()
}

func configureConfigureForTls(cm kafkago.ConfigMap, serverCertFile string, clientCertFile string, clientPrivateKeyFile string) kafkago.ConfigMap {
	cm["security.protocol"] = "ssl"
	cm["ssl.ca.location"] = serverCertFile
	if clientCertFile != "" {
		cm["ssl.certificate.location"] = clientCertFile
	}
	if clientPrivateKeyFile != "" {
		cm["ssl.key.location"] = clientPrivateKeyFile
	}
	return cm
}
