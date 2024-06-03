package query

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/evbatch"
	"github.com/spirit-labs/tektite/expr"
	"github.com/spirit-labs/tektite/iteration"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/opers"
	"github.com/spirit-labs/tektite/parser"
	"github.com/spirit-labs/tektite/proc"
	"github.com/spirit-labs/tektite/protos/clustermsgs"
	"github.com/spirit-labs/tektite/remoting"
	"github.com/spirit-labs/tektite/types"
	"sync"
	"sync/atomic"
	"time"
)

type Manager interface {
	PrepareQuery(prepareQuery parser.PrepareQueryDesc) error
	ExecutePreparedQuery(queryName string, args []any,
		outputFunc func(last bool, numLastBatches int, batch *evbatch.Batch) error) (int, error)
	ExecutePreparedQueryWithHighestVersion(queryName string, args []any, highestVersion int64,
		outputFunc func(last bool, numLastBatches int, batch *evbatch.Batch) error) (int, error)
	ExecuteQueryDirect(tsl string, query parser.QueryDesc,
		outputFunc func(last bool, numLastBatches int, batch *evbatch.Batch) error) error
	SetLastCompletedVersion(version int64)
	ExecuteRemoteQuery(msg *clustermsgs.QueryMessage) error
	ReceiveQueryResult(msg *clustermsgs.QueryResponse)
	GetPreparedQueryParamSchema(preparedQueryName string) *evbatch.EventSchema
	SetClusterMessageHandlers(remotingServer remoting.Server, vbHandler *remoting.TeeBlockingClusterMessageHandler)
	GetLastCompletedVersion() int
	GetLastFlushedVersion() int
	Activate()
	Start() error
	Stop() error
}

type manager struct {
	lock                       sync.RWMutex
	active                     bool
	preparedQueries            map[string]*QInfo
	remoting                   queryRemoting
	remotingListenAddresses    []string
	remotingAddress            string
	resultHandlers             sync.Map
	partitionMapper            proc.PartitionMapper
	clustVersionProvider       clusterVersionProvider
	streamInfoProvider         StreamInfoProvider
	storeIteratorProvider      iteratorProvider
	streamMetaIteratorProvider iteratorProvider
	expressionFactory          *expr.ExpressionFactory
	parser                     *parser.Parser
	maxBatchRows               int
	lastCompletedVersion       int64
	lastFlushedVersion         int64
	nodeID                     int
}

type iteratorProvider interface {
	NewIterator(keyStart []byte, keyEnd []byte, highestVersion uint64, preserveTombstones bool) (iteration.Iterator, error)
}

type QInfo struct {
	SlabInfo           *opers.SlabInfo
	LocalOperators     []opers.Operator
	RemoteOperators    []opers.Operator
	ParamSchema        *evbatch.EventSchema
	RemoteResultSchema *evbatch.EventSchema
	FullKeyLookup      bool
}

func createEmptyBatch(schema *evbatch.EventSchema) *evbatch.Batch {
	builders := evbatch.CreateColBuilders(schema.ColumnTypes())
	return evbatch.NewBatchFromBuilders(schema, builders...)
}

type clusterVersionProvider interface {
	ClusterVersion() int
	// IsReadyAsOfVersion - Returns true if the local cluster node is at the specified cluster version
	IsReadyAsOfVersion(clusterVersion int) bool
}

type StreamInfoProvider interface {
	GetStream(streamName string) *opers.StreamInfo
}

type queryRemoting interface {
	SendQueryMessageAsync(completionFunc func(remoting.ClusterMessage, error), request *clustermsgs.QueryMessage, serverAddress string)
	SendQueryResponse(request *clustermsgs.QueryResponse, serverAddress string) error
	Close()
}

func NewManager(partitionMapper proc.PartitionMapper, clustVersionProvider clusterVersionProvider, nodeID int,
	streamInfoProvider StreamInfoProvider, storeIterProvider iteratorProvider, streamMetaIterProvider iteratorProvider,
	remoting queryRemoting, remotingListenAddresses []string, maxBatchRows int, expressionFactory *expr.ExpressionFactory,
	parser *parser.Parser) Manager {
	return &manager{
		preparedQueries:            map[string]*QInfo{},
		partitionMapper:            partitionMapper,
		clustVersionProvider:       clustVersionProvider,
		streamInfoProvider:         streamInfoProvider,
		storeIteratorProvider:      storeIterProvider,
		streamMetaIteratorProvider: streamMetaIterProvider,
		remoting:                   remoting,
		remotingListenAddresses:    remotingListenAddresses,
		remotingAddress:            remotingListenAddresses[nodeID],
		maxBatchRows:               maxBatchRows,
		lastCompletedVersion:       -1,
		nodeID:                     nodeID,
		expressionFactory:          expressionFactory,
		parser:                     parser,
	}
}

func (m *manager) Start() error {
	return nil
}

func (m *manager) Stop() error {
	m.remoting.Close()
	return nil
}

func (m *manager) Activate() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.active = true
}

/*
Note on 2 phase broadcast of last completed version:
As the version manager completes versions it will broadcast completed version in two phases - phase 1 broadcasts
the completed version to each node, then if that completes successfully it broadcasts last completed version (all nodes)
to all nodes. This means that if a node has received last completed version (all nodes) with version V it knows that
all other nodes have received last completed version with version V already.
When executing a query we use last completed version (all nodes) as the max version we will use on remote nodes when
creating the iterators. When a remote node receives the query request it checks whether it has already seen last completed
version for that version. This avoids a race where otherwise the remote node might not have received the last completed
version for the version in the query message by the time it arrives on the remote node, if a 1 phase approach had been used.
*/

func (m *manager) SetLastCompletedVersion(version int64) {
	atomic.StoreInt64(&m.lastCompletedVersion, version)
}

func (m *manager) SetLastFlushedVersion(version int64) {
	atomic.StoreInt64(&m.lastFlushedVersion, version)
}

func (m *manager) GetLastCompletedVersion() int {
	return int(atomic.LoadInt64(&m.lastCompletedVersion))
}

func (m *manager) GetLastFlushedVersion() int {
	return int(atomic.LoadInt64(&m.lastFlushedVersion))
}

func (m *manager) SetClusterMessageHandlers(remotingServer remoting.Server, vbHandler *remoting.TeeBlockingClusterMessageHandler) {
	remotingServer.RegisterBlockingMessageHandler(remoting.ClusterMessageQueryMessage, &queryMessageHandler{m: m})
	remotingServer.RegisterBlockingMessageHandler(remoting.ClusterMessageQueryResponse, &queryResponseHandler{m: m})
	vbHandler.Handlers = append(vbHandler.Handlers, &versionBroadcastHandler{m: m})
}

type queryMessageHandler struct {
	m *manager
}

func (q *queryMessageHandler) HandleMessage(messageHolder remoting.MessageHolder) (remoting.ClusterMessage, error) {
	queryMessage := messageHolder.Message.(*clustermsgs.QueryMessage)
	return nil, q.m.ExecuteRemoteQuery(queryMessage)
}

type queryResponseHandler struct {
	m *manager
}

func (q *queryResponseHandler) HandleMessage(messageHolder remoting.MessageHolder) (remoting.ClusterMessage, error) {
	queryMessage := messageHolder.Message.(*clustermsgs.QueryResponse)
	q.m.ReceiveQueryResult(queryMessage)
	return nil, nil
}

type versionBroadcastHandler struct {
	m *manager
}

func (v *versionBroadcastHandler) HandleMessage(messageHolder remoting.MessageHolder) (remoting.ClusterMessage, error) {
	msg := messageHolder.Message.(*clustermsgs.VersionsMessage)
	v.m.SetLastCompletedVersion(msg.CompletedVersion)
	v.m.SetLastFlushedVersion(msg.FlushedVersion)
	return nil, nil
}

func (m *manager) GetPreparedQueryParamSchema(preparedQueryName string) *evbatch.EventSchema {
	m.lock.Lock()
	defer m.lock.Unlock()
	pqi, exists := m.preparedQueries[preparedQueryName]
	if !exists {
		return nil
	}
	return pqi.ParamSchema
}

func (m *manager) PrepareQuery(prepareQuery parser.PrepareQueryDesc) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	_, exists := m.preparedQueries[prepareQuery.QueryName]
	if exists {
		return errors.NewQueryErrorf("query with name '%s' has already been prepared", prepareQuery.QueryName)
	}
	pqi, err := m.createQueryInfo(prepareQuery.Query.OperatorDescs, prepareQuery.Params)
	if err != nil {
		return err
	}
	m.preparedQueries[prepareQuery.QueryName] = pqi
	return nil
}

func (m *manager) createQueryInfo(opDescs []parser.Parseable, params []parser.PreparedStatementParam) (*QInfo, error) {
	var operators []opers.Operator
	var prevOperator opers.Operator
	var streamInfo *opers.StreamInfo
	var isFullKeyLookup bool
	hasSort := false
	var paramSchema *evbatch.EventSchema
	lp := len(params)
	if lp > 0 {
		pNames := make([]string, lp)
		pTypes := make([]types.ColumnType, lp)
		for i := 0; i < lp; i++ {
			pNames[i] = params[i].ParamName
			pTypes[i] = params[i].ParamType
		}
		paramSchema = evbatch.NewEventSchema(pNames, pTypes)
	}
loop:
	for i, opDesc := range opDescs {
		var oper opers.Operator
		var err error
		switch desc := opDesc.(type) {
		case *parser.GetDesc:
			streamInfo = m.streamInfoProvider.GetStream(desc.TableName)
			if streamInfo == nil || streamInfo.UserSlab == nil ||
				(streamInfo.UserSlab.Type != opers.SlabTypeUserTable && streamInfo.UserSlab.Type != opers.SlabTypeQueryableInternal) {
				return nil, queryErrorAtTokenf(desc.TableName, desc, "unknown table '%s'", desc.TableName)
			}
			isFullKeyLookup = len(desc.KeyExprs) == len(streamInfo.UserSlab.KeyColIndexes)
			colExprs, err := m.createAndValidateLookupParamExprs(paramSchema, desc.KeyExprs, streamInfo.UserSlab)
			if err != nil {
				return nil, err
			}
			var iterProvider iteratorProvider
			if streamInfo.StreamMeta {
				iterProvider = m.streamMetaIteratorProvider
			} else {
				iterProvider = m.storeIteratorProvider
			}
			oper = NewGetOperator(false, colExprs, nil, true, false, streamInfo.UserSlab.SlabID,
				streamInfo.UserSlab.KeyColIndexes, streamInfo.UserSlab.Schema, iterProvider, m.nodeID)
		case *parser.ScanDesc:
			streamInfo = m.streamInfoProvider.GetStream(desc.TableName)
			isFullKeyLookup = false
			var rangeStartExprs []expr.Expression
			var rangeEndExprs []expr.Expression
			if desc.All {
				if streamInfo == nil || streamInfo.UserSlab == nil ||
					(streamInfo.UserSlab.Type != opers.SlabTypeUserStream && streamInfo.UserSlab.Type != opers.SlabTypeUserTable &&
						streamInfo.UserSlab.Type != opers.SlabTypeQueryableInternal) {
					return nil, queryErrorAtTokenf(desc.TableName, desc, "unknown table or stream '%s'", desc.TableName)
				}
			} else {
				if streamInfo == nil || streamInfo.UserSlab == nil ||
					(streamInfo.UserSlab.Type != opers.SlabTypeUserTable && streamInfo.UserSlab.Type != opers.SlabTypeQueryableInternal) {
					return nil, queryErrorAtTokenf(desc.TableName, desc, "unknown table '%s'", desc.TableName)
				}
				if desc.FromKeyExprs != nil {
					rangeStartExprs, err = m.createAndValidateLookupParamExprs(paramSchema, desc.FromKeyExprs, streamInfo.UserSlab)
					if err != nil {
						return nil, err
					}
				}
				if desc.ToKeyExprs != nil {
					rangeEndExprs, err = m.createAndValidateLookupParamExprs(paramSchema, desc.ToKeyExprs, streamInfo.UserSlab)
					if err != nil {
						return nil, err
					}
				}
			}
			var iterProvider iteratorProvider
			if streamInfo.StreamMeta {
				iterProvider = m.streamMetaIteratorProvider
			} else {
				iterProvider = m.storeIteratorProvider
			}
			oper = NewGetOperator(true, rangeStartExprs, rangeEndExprs, desc.FromIncl,
				desc.ToIncl, streamInfo.UserSlab.SlabID,
				streamInfo.UserSlab.KeyColIndexes, streamInfo.UserSlab.Schema, iterProvider, m.nodeID)
		case *parser.FilterDesc:
			oper, err = opers.NewFilterOperator(prevOperator.OutSchema(), desc.Expr, m.expressionFactory)
		case *parser.ProjectDesc:
			// If the query specifies cols then we don't include offset and event_time
			oper, err = opers.NewProjectOperator(prevOperator.OutSchema(), desc.Expressions, false, m.expressionFactory)
		case *parser.SortDesc:
			if i != len(opDescs)-1 {
				return nil, queryErrorAtTokenf("", desc, "sort must be the last operator in a query")
			}
			// We add this later
			hasSort = true
			break loop // must be the last one
		}
		if err != nil {
			return nil, err
		}
		operators = append(operators, oper)
		prevOperator = oper
	}

	var sortOper opers.Operator
	if hasSort {
		// We need to add the sort after the remove offset operator, if any
		expectedLastBatches := 1
		if !isFullKeyLookup {
			expectedLastBatches = streamInfo.UserSlab.Schema.PartitionScheme.Partitions
		}
		var err error
		sortOper, err = opers.NewSortOperator(prevOperator.OutSchema(), expectedLastBatches,
			opDescs[len(opDescs)-1].(*parser.SortDesc).SortExprs, false, m.expressionFactory)
		if err != nil {
			return nil, err
		}
		operators = append(operators, sortOper)
		prevOperator = sortOper
	}

	for i, oper := range operators {
		if i != len(operators)-1 {
			oper.AddDownStreamOperator(operators[i+1])
		}
	}
	var remoteOperators []opers.Operator
	var localOperators []opers.Operator
	if !hasSort {
		remoteOperators = operators
	} else {
		// Only the sort operator is run locally after results are gathered from remote managers, so we remove
		// the sort operator from the remote operators
		remoteOperators = operators[:len(operators)-1]
		remoteOperators[len(remoteOperators)-1].RemoveDownStreamOperator(sortOper)
		localOperators = []opers.Operator{sortOper}
	}
	// Insert a networkResultsOperator to send the results over the network
	nro := &networkResultsOperator{
		remoting: m.remoting,
	}
	lastOper := remoteOperators[len(remoteOperators)-1]
	lastOper.AddDownStreamOperator(nro)
	remoteOperators = append(remoteOperators, nro)
	return &QInfo{
		SlabInfo:           streamInfo.UserSlab,
		LocalOperators:     localOperators,
		RemoteOperators:    remoteOperators,
		RemoteResultSchema: remoteOperators[len(remoteOperators)-2].OutSchema().EventSchema,
		FullKeyLookup:      isFullKeyLookup,
		ParamSchema:        paramSchema,
	}, nil
}

func (m *manager) createAndValidateLookupParamExprs(schema *evbatch.EventSchema, exprDescs []parser.ExprDesc,
	slabInfo *opers.SlabInfo) ([]expr.Expression, error) {
	var colExprs []expr.Expression
	if len(exprDescs) > len(slabInfo.KeyColIndexes) {
		msg := fmt.Sprintf("failed to prepare/execute query: number of key elements specified (%d) is greater than number of columns in key (%d)",
			len(exprDescs), len(slabInfo.KeyColIndexes))
		return nil, errors.NewTektiteErrorf(errors.PrepareQueryError, msg)
	}
	for i, keyExpr := range exprDescs {
		e, err := m.expressionFactory.CreateExpression(keyExpr, schema)
		if err != nil {
			return nil, err
		}
		keyColType := slabInfo.Schema.EventSchema.ColumnTypes()[slabInfo.KeyColIndexes[i]]
		if !typesCompatible(e.ResultType(), keyColType) {
			return nil, keyExpr.ErrorAtPosition("invalid type for param expression - it returns type %s but key column is of type %s",
				e.ResultType().String(), keyColType.String())
		}
		colExprs = append(colExprs, e)
	}
	return colExprs, nil
}

func typesCompatible(rt1 types.ColumnType, rt2 types.ColumnType) bool {
	if rt1.ID() == types.ColumnTypeIDDecimal {
		if rt2.ID() != types.ColumnTypeIDDecimal {
			return false
		}
		dt1 := rt1.(*types.DecimalType)
		dt2 := rt2.(*types.DecimalType)
		return dt1.Scale == dt2.Scale && dt1.Precision == dt2.Precision
	}
	return rt1.ID() == rt2.ID()
}

func (m *manager) calcNodePartitions(info *QInfo, args *evbatch.Batch) (map[int][]int, int, error) {
	partitionScheme := info.SlabInfo.Schema.PartitionScheme
	if !info.FullKeyLookup {
		// The query doesn't specify values for all key cols so we must fan-out to all partitions
		// e.g. say slab key was [customer_id, tx_id] and the query was to look up all rows where customer_id=x
		nodePartitions, err := m.partitionMapper.NodePartitions(partitionScheme.MappingID, partitionScheme.Partitions)
		if err != nil {
			return nil, 0, err
		}
		// We must check all the partitions are there - during failover they might not be
		count := 0
		for _, parts := range nodePartitions {
			count += len(parts)
		}
		if count != partitionScheme.Partitions {
			return nil, 0, errors.NewTektiteErrorf(errors.Unavailable, "not all partitions available, expected %d actual %d mapping slab name %s",
				partitionScheme.Partitions, count, partitionScheme.MappingID)
		}
		return nodePartitions, partitionScheme.Partitions, nil
	}
	lo := info.RemoteOperators[0].(*GetOperator)
	var partitionKey []byte
	var err error
	if partitionScheme.RawPartitionKey {
		// If the slab is on a stream which receives data from a *kafka in* or *bridge from* operator
		// then the data has been partitioned by the Kafka key, in this case RawPartitionKey is true and we choose
		// the partition based on a simple hash of the specified lookup key value.
		partitionKey, err = lo.CreateRawPartitionKey(args)
	} else {
		// Otherwise, if the slab is after a partition operator, then RawPartitionKey will be set to false, as the data
		// has been re-partitioned based on the keys specified in the partition operator. This can be a composite key
		// and allows for nulls, in this case we need to choose the partition based on that key, so we have to generate
		// it in the same way it was generated when hashing in the partition operator.
		partitionKey, err = lo.CreateRangeStartKey(args)
	}
	if err != nil {
		return nil, 0, err
	}

	hash := common.DefaultHash(partitionKey)
	partID := int(common.CalcPartition(hash, partitionScheme.Partitions))
	nodeID := m.partitionMapper.NodeForPartition(partID, partitionScheme.MappingID, partitionScheme.Partitions)
	return map[int][]int{
		nodeID: {partID},
	}, 1, nil
}

func (m *manager) ExecuteQueryWithRetry(queryName string, args []any,
	outputFunc func(last bool, numLastBatches int, batch *evbatch.Batch) error) (int, error) {
	for {
		numParts, err := m.ExecutePreparedQuery(queryName, args, outputFunc)
		if err == nil {
			return numParts, nil
		}
		if !common.IsUnavailableError(err) {
			return numParts, err
		}
		// We retry if we get an unavailable error - this can occur if leadership changes - it should fix itself
		log.Warnf("failed to execute query %v - will retry", err)
		time.Sleep(1 * time.Second)
	}
}

func (m *manager) ExecuteQueryDirect(tsl string, query parser.QueryDesc,
	outputFunc func(last bool, numLastBatches int, batch *evbatch.Batch) error) error {
	m.lock.RLock()
	defer m.lock.RUnlock()
	info, err := m.createQueryInfo(query.OperatorDescs, nil)
	if err != nil {
		return err
	}
	highestVersion := atomic.LoadInt64(&m.lastCompletedVersion)
	_, err = m.executeQuery(info, "", tsl, nil, highestVersion, outputFunc)
	return err
}

func (m *manager) ExecutePreparedQuery(queryName string, args []any,
	outputFunc func(last bool, numLastBatches int, batch *evbatch.Batch) error) (int, error) {
	highestVersion := atomic.LoadInt64(&m.lastCompletedVersion)
	return m.ExecutePreparedQueryWithHighestVersion(queryName, args, highestVersion, outputFunc)
}

func (m *manager) ExecutePreparedQueryWithHighestVersion(queryName string, args []any, highestVersion int64,
	outputFunc func(last bool, numLastBatches int, batch *evbatch.Batch) error) (int, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	info, exists := m.preparedQueries[queryName]
	if !exists {
		return 0, errors.Errorf("query `%s` does not exist", queryName)
	}
	return m.executeQuery(info, queryName, "", args, highestVersion, outputFunc)
}

func (m *manager) executeQuery(info *QInfo, queryName string, tsl string, args []any, highestVersion int64,
	outputFunc func(last bool, numLastBatches int, batch *evbatch.Batch) error) (int, error) {

	if highestVersion == -1 {
		// No version has completed yet, so there is no data. This would be the case on startup of a new cluster
		// So we return an empty batch
		if err := outputFunc(true, 1, createEmptyBatch(info.RemoteResultSchema)); err != nil {
			return 0, err
		}
		return 0, nil
	}

	execID, _ := uuid.New().MarshalBinary()
	sExecID := common.ByteSliceToStringZeroCopy(execID)

	// We encode the args into an event batch - this is used to evaluate them on the remote side, and it's easy to
	// serialize
	var argsBatch *evbatch.Batch
	var argsBuff []byte
	if args != nil {
		paramTypes := info.ParamSchema.ColumnTypes()
		builders := evbatch.CreateColBuilders(paramTypes)
		for i, arg := range args {
			if arg == nil {
				builders[i].AppendNull()
				continue
			}
			ct := paramTypes[i]
			switch ct.ID() {
			case types.ColumnTypeIDInt:
				builders[i].(*evbatch.IntColBuilder).Append(arg.(int64))
			case types.ColumnTypeIDFloat:
				builders[i].(*evbatch.FloatColBuilder).Append(arg.(float64))
			case types.ColumnTypeIDBool:
				builders[i].(*evbatch.BoolColBuilder).Append(arg.(bool))
			case types.ColumnTypeIDDecimal:
				builders[i].(*evbatch.DecimalColBuilder).Append(arg.(types.Decimal))
			case types.ColumnTypeIDString:
				builders[i].(*evbatch.StringColBuilder).Append(arg.(string))
			case types.ColumnTypeIDBytes:
				builders[i].(*evbatch.BytesColBuilder).Append(arg.([]byte))
			case types.ColumnTypeIDTimestamp:
				builders[i].(*evbatch.TimestampColBuilder).Append(arg.(types.Timestamp))
			default:
				panic("unexpected col type")
			}
		}
		argsBatch = evbatch.NewBatchFromBuilders(info.ParamSchema, builders...)
		argsBuff = argsBatch.Serialize(nil)
	}

	nodePartitions, numParts, err := m.calcNodePartitions(info, argsBatch)
	if err != nil {
		return 0, err
	}
	qrh := &queryResultHandler{
		localOperators: info.LocalOperators,
		outputFunc:     outputFunc,
		schema:         info.RemoteResultSchema,
		numPartitions:  int64(numParts),
	}
	m.resultHandlers.Store(sExecID, qrh)

	ch := make(chan error, 1)
	cf := common.NewCountDownFuture(len(nodePartitions), func(err error) {
		ch <- err
	})
	clusterVersion := m.clustVersionProvider.ClusterVersion()
	for nid, partitions := range nodePartitions {
		address := m.remotingListenAddresses[nid]
		partitionsBuff := serializePartitions(partitions)
		msg := &clustermsgs.QueryMessage{
			ExecId:         execID,
			QueryName:      queryName,
			Tsl:            tsl,
			Args:           argsBuff,
			Partitions:     partitionsBuff,
			SenderAddress:  m.remotingAddress,
			HighestVersion: uint64(highestVersion),
			ClusterVersion: uint64(clusterVersion),
		}
		m.remoting.SendQueryMessageAsync(func(_ remoting.ClusterMessage, err error) {
			cf.CountDown(remoting.MaybeConvertError(err))
		}, msg, address)
	}
	err = <-ch
	if err != nil {
		m.resultHandlers.Delete(sExecID)
	}
	return numParts, err
}

func (m *manager) HandlerCount() int {
	count := 0
	m.resultHandlers.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

type queryResultHandler struct {
	localOperators    []opers.Operator
	outputFunc        func(complete bool, numLastBatches int, batch *evbatch.Batch) error
	schema            *evbatch.EventSchema
	numPartitions     int64
	outputCalledCount int64
	sortState         opers.SortState
}

func (q *queryResultHandler) handleQueryResult(last bool, buff []byte) (bool, error) {
	batch := convertBytesToBatch(buff, q.schema)
	if q.localOperators != nil {
		// For now, this is always a sort. It will only return a non nil batch when it has received all batches
		var err error
		batch, err = q.localOperators[0].HandleQueryBatch(batch, &queryExecCtx{
			last:      last,
			execState: &q.sortState,
		})
		if err != nil {
			return true, err
		}
		if batch != nil {
			// For a sort we only receive a single sorted batch
			if err := q.outputFunc(last, 1, batch); err != nil {
				return true, err
			}
		}
	} else {
		if err := q.outputFunc(last, int(q.numPartitions), batch); err != nil {
			return true, err
		}
	}
	if last {
		count := atomic.AddInt64(&q.outputCalledCount, 1)
		if count > q.numPartitions {
			panic("handler called too many times")
		}
		if count == q.numPartitions {
			return true, nil
		}
	}
	return false, nil
}

func (m *manager) ReceiveQueryResult(msg *clustermsgs.QueryResponse) {
	sExecID := string(msg.ExecId)
	qrh, ok := m.resultHandlers.Load(sExecID)
	if !ok {
		// This can occur if the query failed to send to all remote nodes and the handler was removed - ignore
		return
	}
	complete, err := qrh.(*queryResultHandler).handleQueryResult(msg.Last, msg.Value)
	if err != nil {
		log.Errorf("failed to handle query result %v", err)
	}
	if complete {
		m.resultHandlers.Delete(sExecID)
	}
}

func (m *manager) ExecuteRemoteQuery(msg *clustermsgs.QueryMessage) error {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if !m.active {
		// This can happen if a query comes in before the manager is activated - this is called from the
		// command manager after it has prepared system queries
		return errors.NewTektiteErrorf(errors.Unavailable, "query manager not active")
	}
	var info *QInfo
	if msg.QueryName != "" {
		// Prepared query
		var exists bool
		info, exists = m.preparedQueries[msg.QueryName]
		if !exists {
			return errors.Errorf("query %s does not exist", msg.QueryName)
		}
	} else {
		// direct query
		queryDesc, err := m.parser.ParseQuery(msg.Tsl)
		if err != nil {
			return err
		}
		info, err = m.createQueryInfo(queryDesc.OperatorDescs, nil)
		if err != nil {
			return err
		}
	}
	partitionIDs := deserializePartitions(msg.Partitions)
	if !m.clustVersionProvider.IsReadyAsOfVersion(int(msg.ClusterVersion)) {
		// We check that this node has processed the same cluster version as the sending node, and that there
		// are no processors in the process of becoming live. If there are, an error will be returned and the
		// query will be retried.
		// This protects against the case where a query is issued, is received on the remote node, but a processor
		// is still in the process of failing over and reprocessing batches from the replication queue. We need
		// to wait until the processor is initialised or the query could miss committed data
		return errors.NewTektiteErrorf(errors.Unavailable,
			"cannot handle remote query, remote node is not ready at required version")
	}

	var argsBatch *evbatch.Batch
	if msg.Args != nil {
		argsBatch = evbatch.NewBatchFromSingleBuff(info.ParamSchema, msg.Args)
	}

	lo := info.RemoteOperators[0].(*GetOperator)
	// For now, we just have one loader per partition but, we should experiment to see if it's more efficient to have
	// multiple sharing the same loader - also for Kafka consumers we will have multiple paritions on the same loader
	for _, partID := range partitionIDs {
		ql := &queryLoader{
			info:           info,
			partitionIDs:   []uint64{partID},
			iters:          make([]iteration.Iterator, 1),
			highestVersion: msg.HighestVersion,
			getOperator:    lo,
			rateLimiter:    &dummyRateLimiter{},
			args:           argsBatch,
			execID:         string(msg.ExecId),
			resultAddress:  msg.SenderAddress,
			maxRows:        m.maxBatchRows,
			nodeID:         m.nodeID,
		}
		common.Go(func() {
			if err := ql.start(); err != nil {
				log.Errorf("failed to start query loader %v", err)
			}
		})
	}
	return nil
}

func (m *manager) getPreparedQuery(name string) *QInfo {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.preparedQueries[name]
}

type dummyRateLimiter struct {
}

func (d dummyRateLimiter) Limit() {
}

type queryLoader struct {
	info           *QInfo
	pos            int
	partitionIDs   []uint64
	highestVersion uint64
	iters          []iteration.Iterator
	getOperator    *GetOperator
	maxRows        int
	cancelled      atomic.Bool
	rateLimiter    RateLimiter
	args           *evbatch.Batch
	execID         string
	resultAddress  string
	nodeID         int
}

func (ql *queryLoader) start() error {
	for i, partID := range ql.partitionIDs {
		iter, err := ql.getOperator.CreateIterator(partID, ql.args, ql.highestVersion)
		if err != nil {
			return err
		}
		ql.iters[i] = iter
	}
	return ql.runLoop()
}

type RateLimiter interface {
	Limit()
}

func (ql *queryLoader) runLoop() error {
	for !ql.cancelled.Load() {
		// We load a batch from each partition round-robin
		iter, iterPos, ok := ql.chooseIterator()
		if !ok {
			// Iterators all complete
			break
		}
		batch, more, err := ql.getOperator.LoadBatch(iter, ql.maxRows)
		if err != nil {
			return err
		}
		if !more {
			// no more rows on the iterator
			iter.Close()
			ql.iters[iterPos] = nil
		}
		_, err = ql.getOperator.HandleQueryBatch(batch, &queryExecCtx{
			execID:        ql.execID,
			resultAddress: ql.resultAddress,
			last:          !more,
		})
		if err != nil {
			return err
		}
		ql.rateLimiter.Limit()
	}
	return nil
}

func (ql *queryLoader) chooseIterator() (iteration.Iterator, int, bool) {
	start := ql.pos
	for {
		iter := ql.iters[ql.pos]
		iterPos := ql.pos
		ql.pos++
		if ql.pos == len(ql.iters) {
			ql.pos = 0
		}
		if iter != nil {
			return iter, iterPos, true
		}
		if ql.pos == start {
			return nil, 0, false
		}
	}
}

type queryExecCtx struct {
	execID        string
	resultAddress string
	last          bool
	execState     any
}

func (q *queryExecCtx) ExecID() string {
	return q.execID
}

func (q *queryExecCtx) ResultAddress() string {
	return q.resultAddress
}

func (q *queryExecCtx) Last() bool {
	return q.last
}

func (q *queryExecCtx) ExecState() any {
	return q.execState
}

type networkResultsOperator struct {
	remoting queryRemoting
}

func convertBatchToBytes(batch *evbatch.Batch) ([]byte, error) {
	bytesBytes := batch.ToBytes()
	size := 0
	for _, buff := range bytesBytes {
		size += len(buff) + 8
	}
	size += 16
	bigBuff := make([]byte, 0, size)
	bigBuff = encoding.AppendUint64ToBufferLE(bigBuff, uint64(batch.RowCount))
	bigBuff = encoding.AppendUint64ToBufferLE(bigBuff, uint64(len(bytesBytes)))
	for _, buff := range bytesBytes {
		bigBuff = encoding.AppendUint64ToBufferLE(bigBuff, uint64(len(buff)))
		bigBuff = append(bigBuff, buff...)
	}
	return bigBuff, nil
}

func convertBytesToBatch(bigBuff []byte, schema *evbatch.EventSchema) *evbatch.Batch {
	rowCount, _ := encoding.ReadUint64FromBufferLE(bigBuff, 0)
	numBuffs, _ := encoding.ReadUint64FromBufferLE(bigBuff, 8)
	buffs := make([][]byte, int(numBuffs))
	off := 16
	for i := 0; i < int(numBuffs); i++ {
		var bl uint64
		bl, off = encoding.ReadUint64FromBufferLE(bigBuff, off)
		if bl > 0 {
			ibl := int(bl)
			buffs[i] = bigBuff[off : off+ibl]
			off += ibl
		}
	}
	return evbatch.NewBatchFromBytes(schema, int(rowCount), buffs)
}

func (nr *networkResultsOperator) HandleStreamBatch(*evbatch.Batch, opers.StreamExecContext) (*evbatch.Batch, error) {
	panic("not supported in streams")
}

func (nr *networkResultsOperator) HandleBarrier(opers.StreamExecContext) error {
	panic("not supported in streams")
}

func (nr *networkResultsOperator) HandleQueryBatch(batch *evbatch.Batch, execCtx opers.QueryExecContext) (*evbatch.Batch, error) {
	bytes, err := convertBatchToBytes(batch)
	if err != nil {
		return nil, err
	}
	msg := &clustermsgs.QueryResponse{
		ExecId: []byte(execCtx.ExecID()),
		Value:  bytes,
		Last:   execCtx.Last(),
	}
	return nil, nr.remoting.SendQueryResponse(msg, execCtx.ResultAddress())
}

func (nr *networkResultsOperator) InSchema() *opers.OperatorSchema {
	return nil
}

func (nr *networkResultsOperator) OutSchema() *opers.OperatorSchema {
	return nil
}

func (nr *networkResultsOperator) Setup(opers.StreamManagerCtx) error {
	return nil
}

func (nr *networkResultsOperator) AddDownStreamOperator(opers.Operator) {
}

func (nr *networkResultsOperator) GetDownStreamOperators() []opers.Operator {
	return nil
}

func (nr *networkResultsOperator) RemoveDownStreamOperator(opers.Operator) {
}

func (nr *networkResultsOperator) GetParentOperator() opers.Operator {
	return nil
}

func (nr *networkResultsOperator) SetParentOperator(opers.Operator) {
}

func (nr *networkResultsOperator) SetStreamInfo(*opers.StreamInfo) {
}

func (nr *networkResultsOperator) GetStreamInfo() *opers.StreamInfo {
	return nil
}

func (nr *networkResultsOperator) Teardown(opers.StreamManagerCtx, *sync.RWMutex) {
}

func serializePartitions(partitions []int) []byte {
	buff := make([]byte, 0, len(partitions)*8+4)
	buff = encoding.AppendUint64ToBufferLE(buff, uint64(len(partitions)))
	for _, part := range partitions {
		buff = encoding.AppendUint64ToBufferLE(buff, uint64(part))
	}
	return buff
}

func deserializePartitions(buff []byte) []uint64 {
	off := 0
	var numParts uint64
	numParts, off = encoding.ReadUint64FromBufferLE(buff, off)
	parts := make([]uint64, int(numParts))
	for i := 0; i < int(numParts); i++ {
		var part uint64
		part, off = encoding.ReadUint64FromBufferLE(buff, off)
		parts[i] = part
	}
	return parts
}

type errMsgAtPositionProvider interface {
	ErrorMsgAtToken(msg string, tokenVal string) string
}

func queryErrorAtTokenf(tokenName string, provider errMsgAtPositionProvider, msg string, args ...interface{}) error {
	msg = fmt.Sprintf(msg, args...)
	msg = provider.ErrorMsgAtToken(msg, tokenName)
	return errors.NewQueryErrorf(msg)
}
