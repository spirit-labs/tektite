package opers

import (
	log "github.com/spirit-labs/tektite/logger"
	"sync"
	"time"

	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/kafka"
)

type MessageConsumer struct {
	receiver     BatchReceiver
	msgProvider  kafka.MessageProvider
	pollTimeout  time.Duration
	maxMessages  int
	running      common.AtomicBool
	msgBatch     []*kafka.Message
	stopWg       sync.WaitGroup
	errorHandler func(error)
}

type BatchReceiver interface {
	HandleMessages(messages []*kafka.Message) error
}

func NewMessageConsumer(receiver BatchReceiver, msgProvider kafka.MessageProvider, pollTimeout time.Duration,
	maxMessages int, errorHandler func(error)) (*MessageConsumer, error) {
	mc := &MessageConsumer{
		receiver:     receiver,
		msgProvider:  msgProvider,
		pollTimeout:  pollTimeout,
		maxMessages:  maxMessages,
		errorHandler: errorHandler,
	}
	mc.stopWg.Add(1)
	if err := msgProvider.Start(); err != nil {
		return nil, errors.WithStack(err)
	}
	mc.start()
	return mc, nil
}

func (m *MessageConsumer) start() {
	m.running.Set(true)
	common.Go(m.pollLoop)
}

func (m *MessageConsumer) Stop(fromLoop bool) error {
	// If not called from the pollLoop then we must wait for the pollLoop to exit
	if !fromLoop && m.running.CompareAndSet(true, false) {
		m.stopWg.Wait()
	}
	return m.msgProvider.Stop()
}

func (m *MessageConsumer) pollLoop() {
	defer common.PanicHandler()
	defer func() {
		m.running.Set(false)
		m.stopWg.Done()
	}()
	for m.running.Get() {
		messages, err := m.getBatch(m.pollTimeout)
		if err != nil {
			m.handleError(err, false)
			return
		}

		if len(messages) > 0 {
			// This blocks until messages were actually ingested
			if err := m.receiver.HandleMessages(messages); err != nil {
				m.handleError(err, true)
				return
			}
		}
	}
}

func (m *MessageConsumer) getBatch(pollTimeout time.Duration) ([]*kafka.Message, error) {
	start := common.NanoTime()
	remaining := int64(pollTimeout)

	m.msgBatch = m.msgBatch[:0]

	// The golang Kafka consumer API returns single messages, not batches, but it's more efficient for us to
	// process in batches. So we attempt to return more than one message at a time.

	for len(m.msgBatch) < m.maxMessages {
		msg, err := m.msgProvider.GetMessage(time.Duration(remaining))
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if msg == nil {
			break
		}
		m.msgBatch = append(m.msgBatch, msg)
		remaining -= int64(common.NanoTime() - start)
		if remaining <= 0 {
			break
		}
	}
	return m.msgBatch, nil
}

func (m *MessageConsumer) handleError(err error, processingError bool) {
	if processingError {
		log.Errorf("failed to process batch: %v", err)
	} else {
		log.Errorf("failed to ingest kafka messages: %v", err)
	}
	m.errorHandler(err)
}
