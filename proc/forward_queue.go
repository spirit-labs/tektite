package proc

import (
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
)

type forwardQueue struct {
	queue              []*enqueuedForward
	forwardResendTimer *common.TimerHandle
	forwardInProgress  bool
	forwardCancelled   bool
	forwardSequences   []int
}

type enqueuedForward struct {
	remoteBatches []*ProcessBatch
}

func (p *processor) enqueueForwardBatches(remoteBatches []*ProcessBatch) {
	p.CheckInProcessorLoop()
	for _, remoteBatch := range remoteBatches {
		if !remoteBatch.Barrier {
			// Assign a sequence to each batch
			procID := remoteBatch.ProcessorID
			forwardSeq := p.forwardSequences[procID]
			p.forwardSequences[procID] = forwardSeq + 1
			if forwardSeq == 0 {
				// The first one is always -1 to signify a reset
				forwardSeq = -1
			}
			remoteBatch.ForwardSequence = forwardSeq
		}
	}
	entry := &enqueuedForward{
		remoteBatches: remoteBatches,
	}
	p.queue = append(p.queue, entry)
	if !p.forwardInProgress && len(p.queue) != 1 {
		panic("no forwards in progress but forwards in queue")
	}
	if p.forwardInProgress {
		return
	}
	p.forwardNextEntry()
}

func (p *processor) forwardNextEntry() {
	p.CheckInProcessorLoop()
	entry := p.queue[0]
	fut := common.NewCountDownFuture(len(entry.remoteBatches), func(err error) {
		if err != nil && !common.IsUnavailableError(err) {
			panic(err)
		}
		p.SubmitAction(func() error {
			p.handleForwardCompletion(err, entry)
			return nil
		})
	})
	for _, remoteBatch := range entry.remoteBatches {
		theBatch := remoteBatch
		if remoteBatch.ForwardSequence == -1 {
			// The first one we send has sequence -1 - this tells the destination to reset its expected sequence
			// If the same entry is resent, it should have sequence 0, so it can be rejected by duplicated detection
			// (and not reset again), so we set sequence to zero on the batch that remains in the queue.
			// We need to copy as don't want to change ForwardSequence on the actual batch we sent, only on the one
			// in the queue, in case it gets resent
			theBatch = remoteBatch.Copy()
			remoteBatch.ForwardSequence = 0
		}
		p.batchForwarder.ForwardBatch(theBatch, false, fut.CountDown)
	}
	p.forwardInProgress = true
}

func (p *processor) handleForwardCompletion(err error, entry *enqueuedForward) {
	p.CheckInProcessorLoop()
	if p.forwardCancelled {
		p.forwardCancelled = false
		return
	}
	if entry != p.queue[0] {
		panic("forward completion in wrong order")
	}
	if err == nil {
		p.queue = p.queue[1:]
		if len(p.queue) == 0 {
			p.forwardInProgress = false
		} else {
			p.forwardNextEntry()
		}
		return
	}
	log.Warnf("failed to forward message %v", err)
	lpf := len(p.queue)
	if p.forwardResendTimer != nil {
		panic("forward resend timer should be nil")
	}
	p.forwardResendTimer = common.ScheduleTimer(p.cfg.ForwardResendDelay, true, func() {
		ok := p.SubmitAction(func() error {
			if p.IsStopped() {
				return nil
			}
			p.forwardResendTimer = nil
			p.forwardNextEntry()
			return nil
		})
		if !ok {
			log.Warnf("processor %d couldn't submit action to resend as processor stopped queue size %d",
				p.id, lpf)
		}
	})
}
