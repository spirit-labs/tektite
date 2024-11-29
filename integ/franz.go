package integ

import (
	"context"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"time"
)

type FranzProducer struct {
	client *kgo.Client
}

func NewFranzProducer(address string, tlsEnabled bool, serverCertFile string, clientCertFile string,
	clientPrivateKeyFile string) (Producer, error) {
	var err error
	var client *kgo.Client
	if tlsEnabled {
		tlsC := conf.ClientTlsConf{
			Enabled:              true,
			ServerCertFile:       serverCertFile,
			ClientPrivateKeyFile: clientPrivateKeyFile,
			ClientCertFile:       clientCertFile,
		}
		goTls, err2 := tlsC.ToGoTlsConf()
		if err2 != nil {
			return nil, err
		}
		client, err = kgo.NewClient(
			kgo.SeedBrokers(address),
			kgo.DialTLSConfig(goTls),
		)
	} else {
		client, err = kgo.NewClient(
			kgo.SeedBrokers(address),
		)
	}
	if err != nil {
		return nil, err
	}
	return &FranzProducer{client: client}, nil
}

func (f *FranzProducer) Produce(topicName string, messages []kafka.Message) error {
	msgs := make([]*kgo.Record, len(messages))
	for i, m := range messages {
		msg := &kgo.Record{
			Key:       m.Key,
			Value:     m.Value,
			Timestamp: m.TimeStamp,
			Topic:     topicName,
		}
		for _, hdr := range m.Headers {
			msg.Headers = append(msg.Headers, kgo.RecordHeader{
				Key:   hdr.Key,
				Value: hdr.Value,
			})
		}
		msgs[i] = msg
	}
	return f.client.ProduceSync(context.Background(), msgs...).FirstErr()
}

func (f *FranzProducer) Close() error {
	f.client.Close()
	return nil
}

type FranzConsumer struct {
	client *kgo.Client
}

func NewFranzConsumer(address string, topicName string, groupID string, tlsEnabled bool, serverCertFile string,
	clientCertFile string, clientPrivateKeyFile string) (Consumer, error) {
	var err error
	var client *kgo.Client
	if tlsEnabled {
		tlsC := conf.ClientTlsConf{
			Enabled:              true,
			ServerCertFile:       serverCertFile,
			ClientPrivateKeyFile: clientPrivateKeyFile,
			ClientCertFile:       clientCertFile,
		}
		goTls, err2 := tlsC.ToGoTlsConf()
		if err2 != nil {
			return nil, err
		}
		client, err = kgo.NewClient(
			kgo.SeedBrokers(address),
			kgo.DialTLSConfig(goTls),
			kgo.ConsumerGroup(groupID),
			kgo.ConsumeTopics(topicName),
		)
	} else {
		client, err = kgo.NewClient(
			kgo.SeedBrokers(address),
			kgo.ConsumerGroup(groupID),
			kgo.ConsumeTopics(topicName),
		)
	}
	if err != nil {
		return nil, err
	}
	return &FranzConsumer{client: client}, nil
}

func (f *FranzConsumer) Fetch(timeout time.Duration) (*kafka.Message, error) {
	start := time.Now()
	for {
		fetches := f.client.PollRecords(context.Background(), 1)
		if errs := fetches.Errors(); len(errs) > 0 {
			return nil, errs[0].Err
		}
		var rec *kgo.Record
		fetches.EachRecord(func(record *kgo.Record) {
			rec = record
		})
		if rec == nil {
			if time.Now().Sub(start) >= timeout {
				return nil, nil
			} else {
				time.Sleep(1 * time.Millisecond)
				continue
			}
		}
		kMsg := &kafka.Message{
			PartInfo: kafka.PartInfo{
				PartitionID: rec.Partition,
				Offset:      rec.Offset,
			},
			Key:       rec.Key,
			Value:     rec.Value,
			TimeStamp: rec.Timestamp,
		}
		for _, hdr := range rec.Headers {
			kMsg.Headers = append(kMsg.Headers, kafka.MessageHeader{
				Key:   hdr.Key,
				Value: hdr.Value,
			})
		}
		return kMsg, nil
	}
}

func (f *FranzConsumer) Close() error {
	f.client.Close()
	return nil
}
