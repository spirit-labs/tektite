// Copyright 2024 The Tektite Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package perf

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/errors"
	log "github.com/spirit-labs/tektite/logger"
	"time"
)

type ConsumerArgs struct {
	Topic           string
	NumMessages     int
	BootstrapServer string
}

type Consumer struct {
	Args *ConsumerArgs
}

const displayInterval = uint64(2 * time.Second)

func (p *Consumer) Run() error {
	cm := &kafka.ConfigMap{
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": true,
		"group.id":           "tektite.perfgroup2",
		"bootstrap.servers":  p.Args.BootstrapServer,
	}
	consumer, err := kafka.NewConsumer(cm)
	if err != nil {
		return err
	}
	//goland:noinspection GoUnhandledErrorResult
	defer consumer.Close()
	if err := consumer.Subscribe(p.Args.Topic, nil); err != nil {
		return err
	}
	start := common.NanoTime()
	totBytes := 0
	lastDisplayBytes := 0
	totMessages := 0
	lastDisplay := start
	lastDisplayMessages := 0
	for {
		ev := consumer.Poll(100)
		if ev == nil {
			continue
		}
		switch e := ev.(type) {
		case *kafka.Message:
			size := len(e.Key) + len(e.Value)
			totBytes += size
			lastDisplayBytes += size
		case kafka.Error:
			log.Errorf("kafka client returned error: %v", e)
			continue
		case kafka.OffsetsCommitted:
			continue
		default:
			return errors.Errorf("unexpected result from poll %+v", e)
		}
		now := common.NanoTime()
		if now-lastDisplay >= displayInterval {
			displayRate(lastDisplayBytes, now-lastDisplay)
			lastDisplay = now
			lastDisplayMessages = 0
			lastDisplayBytes = 0
		}
		totMessages++
		lastDisplayMessages++
		if totMessages == p.Args.NumMessages {
			log.Info("consumed all messages")
			return nil
		}
	}
}

func displayRate(bytes int, dur uint64) {
	rate := float64(int(time.Second)*bytes) / float64(dur)
	//totRate := (float64(int(time.Second)*totBytes) / float64(totDur))
	log.Infof("rate: %.2f bytes/sec", rate)
}
