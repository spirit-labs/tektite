package tx

import (
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	"math"
	"sync"
)

type Coordinator struct {
	lock               sync.Mutex
	producerIDSequence int64
	txInfos map[int64]*txInfo
}

type txInfo struct {
	pid           int64
	producerEpoch int16
	tektiteEpoch  int64
}

func NewCoordinator() *Coordinator {
	return &Coordinator{
		txInfos: make(map[int64]*txInfo),
	}
}

func (c *Coordinator) HandleInitProducerID(req *kafkaprotocol.InitProducerIdRequest) (*kafkaprotocol.InitProducerIdResponse, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// First look up any existing pid already mapped to the transactional id
	transactionalID := common.SafeDerefStringPtr(req.TransactionalId)

	var info *txInfo
	if transactionalID != "" {
		info = c.loadInfoForTransactionalID(transactionalID)
		if info != nil {

			// TODO verify existing producer epoch?

			// TODO resolve any prepared but not committed/aborted transaction state

			// bump the kafka epoch
			if info.producerEpoch == math.MaxInt16 {
				info.producerEpoch = 0
			} else {
				info.producerEpoch++
			}
		}
	}

	if info == nil {
		// First time transactionalID was used or no transactional id - generate a pid
		pid := c.producerIDSequence
		c.producerIDSequence++
		info = &txInfo{
			pid:           pid,
			producerEpoch: 0,
		}
	}

	// Store the tx state
	c.storeTxInfo(info)

	resp := &kafkaprotocol.InitProducerIdResponse{
		ProducerId:    info.pid,
		ProducerEpoch: info.producerEpoch,
	}
	return resp, nil
}


func (c *Coordinator) loadInfoForTransactionalID(transactionalID string) *txInfo {

	// TODO load from storage with query

	// Next: call controller to get tektite epoch and set it on the info

	return nil
}

func (c *Coordinator) storeTxInfo(txInfo *txInfo) {

	// Send to table pusher with tektite epoch

}

func createCoordinatorKey(transactionalID string) string {
	// prefix with 't.' to disambiguate with group coordinator keys
	return "t." + transactionalID
}