package controller

import (
	"encoding/binary"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/replicator"
	"sync"
)

/*
TODO
We will add a Flush() method on command handler that is called on the leader, this will store the entries in storage
and push to s3, it can be done async
In updateOffsets we will store command id against offset entry
In HandleFollower if the last flushed has changed we will remove any entries from the map, as already changed
If new node joins, then it won't initially be eligible to be leader, it will have to wait until it sees last flushed changing.
We can stub out store and load of offsets with an interface
We will need to make sure that replication groups are chosen based on partition hash! so we don't push overlapping. we can use partition hash cache
for this
*/

func (c *Controller) GetOffsets(topicName string, partition int, numOffsets int) (int, error) {
	command := &OffsetsCommand{
		topicName:  topicName,
		partition:  partition,
		numOffsets: numOffsets,
	}
	// TODO maybe make generic to avoid cast
	r, err := c.replicator.ApplyCommand(command)
	if err != nil {
		return 0, err
	}
	baseOffset := r.(int)
	return baseOffset, nil
}

const getOffsetsCommandType replicator.CommandType = 1

type OffsetsCommandHandler struct {
	lock        sync.Mutex
	nextOffsets map[string]map[int]int
}

func newOffsetsCommandHandler() *OffsetsCommandHandler {
	return &OffsetsCommandHandler{
		nextOffsets: make(map[string]map[int]int),
	}
}

func (o *OffsetsCommandHandler) NewCommand() (replicator.Command, error) {
	return &OffsetsCommand{}, nil
}

func (o *OffsetsCommandHandler) HandleLeader(command replicator.Command) (any, error) {
	return o.updateOffsets(command)
}

func (o *OffsetsCommandHandler) HandleFollower(command replicator.Command, commandSequence int64, lastFlushedSequence int64) error {
	_, err := o.updateOffsets(command)
	return err
}

func (o *OffsetsCommandHandler) updateOffsets(command replicator.Command) (any, error) {
	o.lock.Lock()
	defer o.lock.Unlock()
	oc := command.(*OffsetsCommand)
	topicOffsets, ok := o.nextOffsets[oc.topicName]
	if !ok {
		topicOffsets = make(map[int]int)
		o.nextOffsets[oc.topicName] = topicOffsets
	}
	nextOffset := topicOffsets[oc.partition]
	topicOffsets[oc.partition] = nextOffset + oc.numOffsets
	return nextOffset, nil
}

type OffsetsCommand struct {
	topicName  string
	partition  int
	numOffsets int
}

func (oc *OffsetsCommand) Type() replicator.CommandType {
	return getOffsetsCommandType
}

func (oc *OffsetsCommand) GroupID(numGroups int) int64 {
	// TODO use sha? Cache mapping?
	buff := make([]byte, len(oc.topicName)+8)
	copy(buff, oc.topicName)
	binary.BigEndian.PutUint64(buff[len(oc.topicName)+8:], uint64(oc.partition))
	hash := common.HashFnv(buff)
	return int64(hash % uint32(numGroups))
}

func (oc *OffsetsCommand) Serialize(buff []byte) ([]byte, error) {
	buff = binary.BigEndian.AppendUint16(buff, uint16(len(oc.topicName)))
	buff = append(buff, oc.topicName...)
	buff = binary.BigEndian.AppendUint64(buff, uint64(oc.partition))
	buff = binary.BigEndian.AppendUint64(buff, uint64(oc.numOffsets))
	return buff, nil
}

func (oc *OffsetsCommand) Deserialize(buff []byte) ([]byte, error) {
	lt := binary.BigEndian.Uint16(buff)
	end := 2 + int(lt)
	oc.topicName = string(buff[2:end])
	oc.partition = int(binary.BigEndian.Uint64(buff[end:]))
	end += 8
	oc.numOffsets = int(binary.BigEndian.Uint64(buff[end:]))
	return buff[end:], nil
}
