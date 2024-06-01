package command

import (
	"fmt"
	"github.com/spirit-labs/tektite/clustmgr"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/lock"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/opers"
	"github.com/spirit-labs/tektite/parser"
	"github.com/spirit-labs/tektite/proc"
	"github.com/spirit-labs/tektite/query"
	"github.com/spirit-labs/tektite/sequence"
	"github.com/spirit-labs/tektite/types"
	"github.com/spirit-labs/tektite/vmgr"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const (
	CommandsSlabName                  = "sys.command"
	LoadCommandsQueryName             = "sys.load_commands"
	ExecuteCommandLockName            = "exec_command"
	CommandsVersion                   = 1
	commandManagerProcessorID         = 1
	slabAndReceiverSequencesBatchSize = 10
)

var CommandsColumnNames = []string{"id", "command", "version", "extra_data"}
var CommandsColumnTypes = []types.ColumnType{types.ColumnTypeInt, types.ColumnTypeString, types.ColumnTypeInt, types.ColumnTypeBytes}

type Manager interface {
	ExecuteCommand(command string) error
	SetCommandSignaller(signaller Signaller)
	HandleClusterState(cs clustmgr.ClusterState) error
	Start() error
	Stop() error
}

type manager struct {
	lock                   sync.Mutex
	cfg                    *conf.Config
	streamManager          opers.StreamManager
	queryManager           query.Manager
	seqManager             sequence.Manager
	vmgrClient             vmgr.Client
	lockManager            lock.Manager
	commandSignaller       Signaller
	batchForwarder         batchForwarder
	parser                 *parser.Parser
	lastProcessedCommandID int64
	commandsOpSchema       *opers.OperatorSchema
	compactionTimer        *common.TimerHandle
	commandIDsToClear      []int64
	clusterCompactor       atomic.Bool
	testSource             bool
	stopped                atomic.Bool
	loadCommandsTimer      *common.TimerHandle
}

type batchForwarder interface {
	ForwardBatch(batch *proc.ProcessBatch, replicate bool, completionFunc func(error))
}

type Signaller interface {
	CommandAvailable()
}

func NewCommandManager(streamManager opers.StreamManager, queryManager query.Manager,
	seqManager sequence.Manager, lockManager lock.Manager, batchForwarder batchForwarder,
	vmgrClient vmgr.Client, parser *parser.Parser, cfg *conf.Config) Manager {
	return &manager{
		cfg:                    cfg,
		streamManager:          streamManager,
		queryManager:           queryManager,
		seqManager:             seqManager,
		lockManager:            lockManager,
		batchForwarder:         batchForwarder,
		vmgrClient:             vmgrClient,
		parser:                 parser,
		lastProcessedCommandID: -1,
	}
}

func (m *manager) SetCommandSignaller(signaller Signaller) {
	m.commandSignaller = signaller
}

func (m *manager) Start() error {
	m.lock.Lock()
	defer m.lock.Unlock()
	// Register the command slab and receiver
	commandsSchema := evbatch.NewEventSchema(CommandsColumnNames, CommandsColumnTypes)
	m.commandsOpSchema = &opers.OperatorSchema{
		EventSchema:     commandsSchema,
		PartitionScheme: opers.NewPartitionScheme("_default_", 1, false, m.cfg.ProcessorCount),
	}
	if err := m.streamManager.RegisterSystemSlab(CommandsSlabName, common.CommandsReceiverID, common.CommandsDeleteReceiverID,
		common.CommandsSlabID, m.commandsOpSchema, []string{"id"}, true); err != nil {
		return err
	}

	// Prepare the load command query
	prepare := parser.NewPrepareQueryDesc()
	if err := m.parser.Parse(fmt.Sprintf("prepare %s := (scan $id_start:int to end from %s) -> (sort by id)",
		LoadCommandsQueryName, CommandsSlabName), prepare); err != nil {
		return err
	}
	if err := m.queryManager.PrepareQuery(*prepare); err != nil {
		return err
	}
	// Now that we have prepared sys queries the query manager is ready to accept remote queries
	m.queryManager.Activate()

	// Load commands
	if err := m.loadAndProcessCommands(); err != nil {
		return err
	}
	m.streamManager.Loaded()
	// Force a compaction if necessary
	if err := m.maybeCompact(); err != nil {
		return err
	}
	m.scheduleCheckCompaction(true)
	return nil
}

func (m *manager) scheduleCheckCompaction(first bool) {
	m.compactionTimer = common.ScheduleTimer(m.cfg.CommandCompactionInterval, first, func() {
		m.lock.Lock()
		defer m.lock.Unlock()
		if err := m.maybeCompact(); err != nil {
			log.Errorf("failed to create snapshot %v", err)
		}
		m.scheduleCheckCompaction(false)
	})
}

const loadCommandsInterval = 5 * time.Second

func (m *manager) scheduleLoadCommands(first bool) {
	// We periodically load commands in case we missed a broadcast notification as those are best effort
	m.loadCommandsTimer = common.ScheduleTimer(loadCommandsInterval, first, func() {
		m.lock.Lock()
		defer m.lock.Unlock()
		if err := m.CheckProcessCommands(); err != nil {
			log.Errorf("failed to load commands %v", err)
		}
		m.scheduleCheckCompaction(false)
	})
}

func (m *manager) loadAndProcessCommands() error {
	batch, err := m.loadCommands(m.lastProcessedCommandID + 1)
	if err != nil {
		return err
	}
	if batch.RowCount > 0 {
		commandID, err := m.processCommands(batch)
		if err != nil {
			return err
		}
		m.lastProcessedCommandID = commandID
	}
	return nil
}

func (m *manager) loadCommands(fromCommandID int64) (*evbatch.Batch, error) {
	return common.CallWithRetryOnUnavailableWithTimeout[*evbatch.Batch](func() (*evbatch.Batch, error) {
		return m.executeQuerySingleResultBatch(LoadCommandsQueryName, []any{fromCommandID})
	}, func() bool {
		return m.stopped.Load()
	}, 10*time.Millisecond, time.Duration(math.MaxInt64), "")
}

func (m *manager) processCommands(batch *evbatch.Batch) (int64, error) {
	var commandID int64
	for i := 0; i < batch.RowCount; i++ {
		commandID = batch.GetIntColumn(0).Get(i)
		command := batch.GetStringColumn(1).Get(i)
		vers := batch.GetIntColumn(2).Get(i)
		if vers != CommandsVersion {
			return 0, errors.Errorf("unexpected command version %d, expected %d", vers, CommandsVersion)
		}

		ast, err := m.parser.ParseTSL(command)
		if err != nil {
			// This can occur, e.g. if a wasm function is not found because the module has been unregistered
			// in this case we want to ignore and carry on
			log.Debugf("command manager failed to parse command: %v", err)
			continue
		}
		if ast.CreateStream != nil {
			extraData := batch.GetBytesColumn(3).Get(i)
			receiverSequences, slabSequences := deserializeExtraData(extraData)
			ast.CreateStream.TestSource = m.testSource
			err = m.streamManager.DeployStream(*ast.CreateStream, receiverSequences, slabSequences, command, commandID)
		} else if ast.DeleteStream != nil {
			pi := m.streamManager.GetStream(ast.DeleteStream.StreamName)
			err = m.streamManager.UndeployStream(*ast.DeleteStream, commandID)
			if err == nil {
				m.commandIDsToClear = append(m.commandIDsToClear, pi.CommandID, commandID)
			}
		} else if ast.PrepareQuery != nil {
			if err == nil {
				err = m.queryManager.PrepareQuery(*ast.PrepareQuery)
			}
		} else {
			panic("invalid command")
		}
		if err != nil {
			var perr errors.TektiteError
			if errors.As(err, &perr) {
				// User error - this is OK, we record commands before they are processed, so they can error too on reprocessing.
				// We ignore
				if log.DebugEnabled {
					log.Debugf("error on processing command %v", err)
				}
				m.commandIDsToClear = append(m.commandIDsToClear, commandID)
				continue
			}
			return 0, err
		}
	}
	return commandID, nil
}

func (m *manager) executeQuerySingleResultBatch(queryName string, args []any) (*evbatch.Batch, error) {
	ch := make(chan *evbatch.Batch, 1)
	_, err := m.queryManager.ExecutePreparedQueryWithHighestVersion(queryName, args, math.MaxInt64,
		func(last bool, numLastBatches int, batch *evbatch.Batch) error {
			if numLastBatches != 1 {
				panic("sys query must have 1 partition")
			}
			if !last {
				panic("must be one result batch as queries have sort")
			}
			ch <- batch
			return nil
		})
	if err != nil {
		return nil, err
	}
	return <-ch, nil
}

func (m *manager) LastProcessedCommandID() int64 {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.lastProcessedCommandID
}

func (m *manager) ExecuteCommand(command string) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	ast, err := m.parser.ParseTSL(command)
	if err != nil {
		return err
	}
	var extraData []byte
	var receiverSequences, slabSequences []int
	if ast.CreateStream != nil {
		receiverCount, slabCount := calcSequencesCountForStream(ast.CreateStream)
		receiverSequences, err = m.getSequences(receiverCount, opers.ReceiverSequenceName, common.UserReceiverIDBase)
		if err != nil {
			return err
		}
		slabSequences, err = m.getSequences(slabCount, opers.SlabSequenceName, common.UserSlabIDBase)
		if err != nil {
			return err
		}
		extraData = serializeExtraData(receiverSequences, slabSequences)
	}

	// Get cluster wide exclusive lock
	err = m.getClusterLock()
	if err != nil {
		return err
	}
	defer m.releaseClusterLock()

	// Load the commands to get the last command id - note that what is in store is the source of truth
	batch, err := m.loadCommands(m.lastProcessedCommandID + 1)
	if err != nil {
		return err
	}
	commandID := m.lastProcessedCommandID
	if batch.RowCount > 0 {
		for i := 0; i < batch.RowCount; i++ {
			commandID = batch.GetIntColumn(0).Get(i)
		}
	}
	commandID++

	// Store the command reliably
	// Encode as evbatch, create process batch and send to processor for the system shard id
	// The sys receiver for commands will then pick that up
	colBuilders := evbatch.CreateColBuilders(CommandsColumnTypes)
	colBuilders[0].(*evbatch.IntColBuilder).Append(commandID)
	colBuilders[1].(*evbatch.StringColBuilder).Append(command)
	colBuilders[2].(*evbatch.IntColBuilder).Append(CommandsVersion)
	if extraData == nil {
		colBuilders[3].AppendNull()
	} else {
		colBuilders[3].(*evbatch.BytesColBuilder).Append(extraData)
	}
	commandBatch := evbatch.NewBatchFromBuilders(m.commandsOpSchema.EventSchema, colBuilders...)
	processorID := m.commandsOpSchema.ProcessorIDs[0]
	pBatch := proc.NewProcessBatch(processorID, commandBatch, common.CommandsReceiverID, 0, -1)
	if err := m.ingestCommandBatch(pBatch); err != nil {
		return err
	}

	// If we get this far then the command is persisted and officially processed, even if it will later error
	// when it is deployed to the stream manager
	m.lastProcessedCommandID = commandID

	// Now we can process the command locally
	if ast.CreateStream != nil {
		err = m.streamManager.DeployStream(*ast.CreateStream, receiverSequences, slabSequences, command, commandID)
	} else if ast.DeleteStream != nil {
		pi := m.streamManager.GetStream(ast.DeleteStream.StreamName)
		err = m.streamManager.UndeployStream(*ast.DeleteStream, commandID)
		if err == nil {
			m.commandIDsToClear = append(m.commandIDsToClear, commandID, pi.CommandID)
		}
	} else if ast.PrepareQuery != nil {
		if err == nil {
			err = m.queryManager.PrepareQuery(*ast.PrepareQuery)
		}
	} else {
		panic("invalid command")
	}
	if err != nil {
		var perr errors.TektiteError
		if !errors.As(err, &perr) {
			// Not a user error, return now
			return err
		} else {
			if perr.Code == errors.ShutdownError {
				return err
			}
		}
	}

	if err != nil {
		m.commandIDsToClear = append(m.commandIDsToClear, commandID)
	}

	if m.commandSignaller != nil {
		// Now we can broadcast that a new command is available - we do this on stream manager user error too
		m.commandSignaller.CommandAvailable()
	}

	return err
}

func (m *manager) maybeCompact() error {
	if !m.isClusterCompactor() {
		return nil
	}
	if len(m.commandIDsToClear) == 0 {
		return nil
	}

	// We simply delete all commands which failed with user error or refer to streams which were successfully deleted
	columnTypes := []types.ColumnType{types.ColumnTypeInt}
	schema := evbatch.NewEventSchema([]string{"id"}, columnTypes)
	colBuilders := evbatch.CreateColBuilders(columnTypes)
	for _, id := range m.commandIDsToClear {
		colBuilders[0].(*evbatch.IntColBuilder).Append(id)
	}
	// We create a batch with just the key cols
	batch := evbatch.NewBatchFromBuilders(schema, colBuilders...)
	pBatch := proc.NewProcessBatch(commandManagerProcessorID, batch, common.CommandsDeleteReceiverID, 0, -1)
	// best effort, no need to replicate
	m.batchForwarder.ForwardBatch(pBatch, false, func(err error) {
		if err != nil {
			log.Warnf("failed to save compacted commands %v", err)
		}
	})
	log.Debugf("compaction removed %d commands", len(m.commandIDsToClear))
	m.commandIDsToClear = nil
	return nil
}

func (m *manager) isClusterCompactor() bool {
	return m.clusterCompactor.Load()
}

func (m *manager) SetClusterCompactor(clusterCompactor bool) {
	m.clusterCompactor.Store(clusterCompactor)
}

func (m *manager) releaseClusterLock() {
	if _, err := m.lockManager.ReleaseLock(ExecuteCommandLockName); err != nil {
		log.Errorf("failure in releasing lock %v", err)
	}
}

func (m *manager) ingestCommandBatch(batch *proc.ProcessBatch) error {
	ch := make(chan error, 1)
	// We ingest this with replication, so the command will not be lost if failure occurs.
	m.batchForwarder.ForwardBatch(batch, true, func(err error) {
		ch <- err
	})
	return <-ch
}

func (m *manager) getClusterLock() error {
	for {
		ok, err := m.lockManager.GetLock(ExecuteCommandLockName)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		// Lock is already held - retry after delay
		log.Warnf("lock %s already held, will retry", ExecuteCommandLockName)
		time.Sleep(500 * time.Millisecond)
		continue
	}
}

func (m *manager) getSequences(seqCount int, seqName string, base int) ([]int, error) {
	sequences := make([]int, seqCount)
	for i := 0; i < seqCount; i++ {
		seq, err := m.seqManager.GetNextID(seqName, slabAndReceiverSequencesBatchSize)
		if err != nil {
			return nil, err
		}
		sequences[i] = seq + base
	}
	return sequences, nil
}

func calcSequencesCountForStream(cp *parser.CreateStreamDesc) (receiverCount int, slabCount int) {
	for _, desc := range cp.OperatorDescs {
		switch desc.(type) {
		case *parser.BridgeFromDesc:
			receiverCount++
			slabCount++ // dedup slab
		case *parser.BridgeToDesc:
			receiverCount++
			slabCount += 2
		case *parser.KafkaInDesc:
			receiverCount++
			slabCount++ // offsets slab
		case *parser.KafkaOutDesc:
			slabCount += 2
		case *parser.PartitionDesc:
			receiverCount++
			slabCount++ // dedup slab
		case *parser.BackfillDesc:
			receiverCount++
		case *parser.AggregateDesc:
			slabCount += 3
			receiverCount++
		case *parser.StoreStreamDesc:
			slabCount += 2
		case *parser.StoreTableDesc:
			slabCount++
		case *parser.JoinDesc:
			slabCount += 2
			receiverCount++
		case *parser.TopicDesc:
			receiverCount++
			slabCount += 3
		case *parser.UnionDesc:
			receiverCount++
		}
	}
	return
}

func (m *manager) CheckProcessCommands() error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.stopped.Load() {
		return nil
	}
	return m.loadAndProcessCommands()
}

func (m *manager) Stop() error {
	m.stopped.Store(true)
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.loadCommandsTimer != nil {
		m.loadCommandsTimer.Stop()
	}
	if m.compactionTimer != nil {
		m.compactionTimer.Stop()
	}
	return nil
}

func serializeExtraData(receiverSequences []int, slabSequences []int) []byte {
	buff := make([]byte, 0, len(receiverSequences)*8+8+len(slabSequences)*8+8)
	buff = serializeSequences(buff, receiverSequences)
	buff = serializeSequences(buff, slabSequences)
	return buff
}

func serializeSequences(buff []byte, sequences []int) []byte {
	buff = encoding.AppendUint64ToBufferLE(buff, uint64(len(sequences)))
	for _, part := range sequences {
		buff = encoding.AppendUint64ToBufferLE(buff, uint64(part))
	}
	return buff
}

func deserializeExtraData(buff []byte) ([]int, []int) {
	receiverSeqs, offset := deserializeSequences(buff)
	slabSequences, _ := deserializeSequences(buff[offset:])
	return receiverSeqs, slabSequences
}

func deserializeSequences(buff []byte) ([]int, int) {
	off := 0
	var numSeqs uint64
	numSeqs, off = encoding.ReadUint64FromBufferLE(buff, off)
	sequences := make([]int, int(numSeqs))
	for i := 0; i < int(numSeqs); i++ {
		var part uint64
		part, off = encoding.ReadUint64FromBufferLE(buff, off)
		sequences[i] = int(part)
	}
	return sequences, off
}

func (m *manager) HandleClusterState(cs clustmgr.ClusterState) error {
	if len(cs.GroupStates) < 1 {
		return nil
	}
	gs := cs.GroupStates[0] // Use const
	isCompactor := false
	for _, gn := range gs {
		if gn.Leader && gn.NodeID == m.cfg.NodeID {
			isCompactor = true
			break
		}
	}
	m.SetClusterCompactor(isCompactor)
	return nil
}
