package kafkaserver

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/asl/errwrap"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/opers"
	"github.com/spirit-labs/tektite/proc"
	"github.com/spirit-labs/tektite/types"
	"io"
	"net"
	"sync"
)

const (
	readBuffSize = 8 * 1024
)

func NewServer(cfg *conf.Config, metadataProvider MetadataProvider, procProvider processorProvider,
	groupCoordinator *GroupCoordinator, streamMgr streamMgr) *Server {
	return &Server{
		cfg:              cfg,
		metadataProvider: metadataProvider,
		procProvider:     procProvider,
		groupCoordinator: groupCoordinator,
		fetcher:          newFetcher(procProvider, streamMgr, int(cfg.KafkaFetchCacheMaxSizeBytes)),
	}
}

type Server struct {
	cfg                 *conf.Config
	listener            net.Listener
	started             bool
	lock                sync.RWMutex
	acceptLoopExitGroup sync.WaitGroup
	connections         sync.Map
	metadataProvider    MetadataProvider
	procProvider        processorProvider
	groupCoordinator    *GroupCoordinator
	fetcher             *fetcher
	listenCancel        context.CancelFunc
}

type processorProvider interface {
	GetProcessor(processorID int) proc.Processor
	NodeForPartition(partitionID int, mappingID string, partitionCount int) int
}

// Start - note that this does not actually start the kafka Server - we delay really starting this (with Activate)
// until the rest of the Server is started.
func (s *Server) Start() error {
	return nil
}

func (s *Server) Activate() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.started {
		return nil
	}
	list, err := s.createNetworkListener()
	if err != nil {
		return errwrap.WithStack(err)
	}
	s.listener = list
	s.started = true
	s.fetcher.start()
	s.acceptLoopExitGroup.Add(1)
	common.Go(s.acceptLoop)
	log.Debugf("started kafka Server on %s", s.cfg.KafkaServerListenerConfig.Addresses[s.cfg.NodeID])
	return nil
}

func (s *Server) createNetworkListener() (net.Listener, error) {
	var list net.Listener
	var err error
	var tlsConfig *tls.Config
	listenAddress := s.cfg.KafkaServerListenerConfig.Addresses[s.cfg.NodeID]
	if s.cfg.KafkaServerListenerConfig.TLSConfig.Enabled {
		tlsConfig, err = conf.CreateServerTLSConfig(s.cfg.KafkaServerListenerConfig.TLSConfig)
		if err != nil {
			return nil, errwrap.WithStack(err)
		}
		list, err = common.Listen("tcp", listenAddress)
		if err == nil {
			list = tls.NewListener(list, tlsConfig)
		}
	} else {
		list, err = common.Listen("tcp", listenAddress)
	}
	if err != nil {
		return nil, errwrap.WithStack(err)
	}
	return list, nil
}

func (s *Server) acceptLoop() {
	defer s.acceptLoopExitGroup.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			// Ok - was closed
			break
		}
		c := s.newConnection(conn)
		s.connections.Store(c, struct{}{})
		c.start()
	}
}

func (s *Server) Stop() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if !s.started {
		return nil
	}
	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			// Ignore
		}
	}
	// Wait for accept loop to exit
	s.acceptLoopExitGroup.Wait()

	// Now close connections
	s.connections.Range(func(conn, _ interface{}) bool {
		conn.(*connection).stop()
		return true
	})
	s.fetcher.stop()
	s.started = false
	return nil
}

func (s *Server) ListenAddress() string {
	return s.cfg.KafkaServerListenerConfig.Addresses[s.cfg.NodeID]
}

func (s *Server) removeConnection(conn *connection) {
	s.connections.Delete(conn)
}

func (s *Server) newConnection(conn net.Conn) *connection {
	return &connection{
		s:    s,
		conn: conn,
	}
}

type connection struct {
	s          *Server
	conn       net.Conn
	closeGroup sync.WaitGroup
	lock       sync.Mutex
	closed     bool
}

func (c *connection) start() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.closeGroup.Add(1)
	common.Go(c.readLoop)
}

func (c *connection) readLoop() {
	defer common.TektitePanicHandler()
	defer c.closeGroup.Done()
	c.readMessage()
	c.s.removeConnection(c)
	c.lock.Lock()
	defer c.lock.Unlock()
	c.closed = true
}

func (c *connection) readMessage() {
	buff := make([]byte, readBuffSize)
	var err error
	var readPos, n int
	for {
		// read the message size
		bytesRequired := 4 - readPos
		if bytesRequired > 0 {
			n, err = io.ReadAtLeast(c.conn, buff[readPos:], bytesRequired)
			if err != nil {
				break
			}
			readPos += n
		}

		totSize := 4 + int(ReadInt32FromBytes(buff))
		bytesRequired = totSize - readPos
		if bytesRequired > 0 {
			// If we haven't already read enough bytes, read the entire message body
			if totSize > len(buff) {
				// buffer isn't big enough, resize it
				nb := make([]byte, totSize)
				copy(nb, buff)
				buff = nb
			}
			n, err = io.ReadAtLeast(c.conn, buff[readPos:], bytesRequired)
			if err != nil {
				break
			}
			readPos += n
		}
		c.handleMessage(buff[4:totSize])

		remainingBytes := readPos - totSize
		if remainingBytes > 0 {
			// Bytes for another message(s) have already been read, don't throw these away
			if remainingBytes < totSize {
				// we can copy directly as no overlap
				copy(buff, buff[totSize:readPos])
			} else {
				// too many bytes remaining, we have to create a new buffer
				nb := make([]byte, len(buff))
				copy(nb, buff[totSize:readPos])
				buff = nb
			}
		}
		readPos = remainingBytes
	}
	if err == io.EOF {
		return
	}
	log.Errorf("error in reading from connection %v", err)
}

func (c *connection) handleMessage(message []byte) {
	apiKey := ReadInt16FromBytes(message)
	apiVersion := ReadInt16FromBytes(message[2:])
	correlationID := ReadInt32FromBytes(message[4:])
	offset := 8

	requestVersion := requestHeaderVersion(apiKey, apiVersion)
	var clientID NullableString
	if requestVersion >= 1 {
		var bytesRead int
		clientID, bytesRead = ReadNullableStringFromBytes(message[offset:])
		offset += bytesRead
		if requestVersion >= 2 {
			offset++ // tag buffer
		}
	}

	respVersion := responseHeaderVersion(apiKey, apiVersion)
	var respBuffHeaderSize int
	if respVersion == 0 {
		respBuffHeaderSize = 8
	} else {
		respBuffHeaderSize = 9 // extra byte for tag buffer
	}

	c.handleApi(clientID, apiKey, apiVersion, message[offset:], respBuffHeaderSize, func(respBuff []byte) {
		WriteInt32ToBytes(respBuff, int32(len(respBuff)-4))
		WriteInt32ToBytes(respBuff[4:], correlationID)
		_, err := c.conn.Write(respBuff)
		if err != nil {
			log.Errorf("failed to write api response %v", err)
		}
	})
}

func (c *connection) stop() {
	c.lock.Lock()
	c.closed = true
	if err := c.conn.Close(); err != nil {
		// Do nothing - connection might already have been closed (e.g. from Client)
	}
	c.lock.Unlock()
	c.closeGroup.Wait()
}

func AppendStringBytes(buffer []byte, value string) []byte {
	buffer = binary.BigEndian.AppendUint16(buffer, uint16(len(value)))
	return append(buffer, []byte(value)...)
}

func ReadStringFromBytes(buffer []byte) (string, int) {
	l := binary.BigEndian.Uint16(buffer)
	//s := common.ByteSliceToStringZeroCopy(buffer[2 : 2+l])
	s := string(buffer[2 : 2+l])
	return s, 2 + int(l)
}

func WriteInt32ToBytes(buffer []byte, value int32) {
	binary.BigEndian.PutUint32(buffer, uint32(value))
}

func AppendInt32ToBytes(buffer []byte, value int32) []byte {
	return binary.BigEndian.AppendUint32(buffer, uint32(value))
}

func AppendInt16ToBytes(buffer []byte, value int16) []byte {
	return binary.BigEndian.AppendUint16(buffer, uint16(value))
}

func AppendInt64ToBytes(buffer []byte, value int64) []byte {
	return binary.BigEndian.AppendUint64(buffer, uint64(value))
}

func ReadInt32FromBytes(buffer []byte) int32 {
	return int32(binary.BigEndian.Uint32(buffer))
}

func ReadInt64FromBytes(buffer []byte) int64 {
	return int64(binary.BigEndian.Uint64(buffer))
}

func ReadInt16FromBytes(buffer []byte) int16 {
	return int16(binary.BigEndian.Uint16(buffer))
}

type NullableString *string

func ReadNullableStringFromBytes(buffer []byte) (NullableString, int) {
	length := ReadInt16FromBytes(buffer)
	if length == -1 {
		return nil, 2
	}
	str := string(buffer[2 : 2+length])
	return &str, 2 + int(length)
}

func AppendNullableStringToBytes(buffer []byte, value NullableString) []byte {
	if value == nil {
		return AppendInt16ToBytes(buffer, -1)
	}
	buffer = AppendInt16ToBytes(buffer, int16(len(*value)))
	buffer = append(buffer, []byte(*value)...)
	return buffer
}

func ReadCompactStringFromBytes(data []byte) (string, int) {
	sl, bytesRead := binary.Uvarint(data)
	if sl == 0 {
		return "", bytesRead
	}
	strLength := int(sl) - 1
	str := string(data[bytesRead : bytesRead+strLength])
	bytesRead += strLength
	return str, bytesRead
}

type MetadataProvider interface {
	ControllerNodeID() int

	BrokerInfos() []BrokerInfo

	GetTopicInfo(topicName string) (TopicInfo, bool)

	GetAllTopics() []*TopicInfo
}

type BrokerInfo struct {
	NodeID int
	Host   string
	Port   int
}

type TopicInfo struct {
	Name                 string
	ProduceEnabled       bool
	ConsumeEnabled       bool
	CanCache             bool
	ProduceInfoProvider  TopicInfoProvider
	ConsumerInfoProvider ConsumerInfoProvider
	Partitions           []PartitionInfo
}

type TopicInfoProvider interface {
	PartitionScheme() *opers.PartitionScheme
	ReceiverID() int
	GetLastProducedInfo(partitionID int) (int64, int64)
	IngestBatch(recordBatchBytes []byte, processor proc.Processor, partitionID int,
		complFunc func(err error))
}

type ConsumerInfoProvider interface {
	SlabID() int
	PartitionScheme() *opers.PartitionScheme
	EarliestOffset(partitionID int) (int64, int64, bool)
	LatestOffset(partitionID int) (int64, int64, bool, error)
	OffsetByTimestamp(timestamp types.Timestamp, partitionID int) (int64, int64, bool)
}

type PartitionInfo struct {
	ID             int
	LeaderNodeID   int
	ReplicaNodeIDs []int
}
