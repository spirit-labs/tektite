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

package opers

import (
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/proc"
)

type StreamExecContext interface {
	StoreEntry(kv common.KV, noCache bool)
	ForwardEntry(processorID int, receiverID int, remotePartitionID int, rowIndex int, batch *evbatch.Batch, schema *evbatch.EventSchema)
	ForwardBarrier(processorID int, receiverID int)
	CheckInProcessorLoop()
	WriteVersion() int
	PartitionID() int
	ForwardingProcessorID() int
	ForwardSequence() int
	Processor() proc.Processor
	Get(key []byte) ([]byte, error)
	BackFill() bool
	WaterMark() int
	SetWaterMark(waterMark int)
	EventBatchBytes() []byte
	ReceiverID() int
}

type QueryExecContext interface {
	ExecID() string
	ResultAddress() string
	Last() bool
	ExecState() any
}
