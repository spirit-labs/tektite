package remoting

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/asl/errwrap"
	"github.com/spirit-labs/tektite/asl/remoting/protos/remotingmsgs"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/protos/clustermsgs"
	"net"
	"reflect"
)

type ClusterMessageType int32

const (
	ClusterMessageTypeUnknown ClusterMessageType = iota + 1
	ClusterMessageQueryMessage
	ClusterMessageQueryResponse
	ClusterMessageReplicateMessage
	ClusterMessageForwardMessage
	ClusterMessageFlushMessage
	ClusterMessageVersionsMessage
	ClusterMessageGetVersionMessage
	ClusterMessageVersionCompleteMessage
	ClusterMessageVersionFlushedMessage
	ClusterMessageFailureDetectedMessage
	ClusterMessageGetLastFailureFlushedVersionMessage
	ClusterMessageGetLastFailureFlushedVersionResponse
	ClusterMessageFailureCompleteMessage
	ClusterMessageIsFailureCompleteMessage
	ClusterMessageIsFailureCompleteResponse
	ClusterMessageLastCommittedRequestMessage
	ClusterMessageLastCommittedResponseMessage
	ClusterMessageSetLastCommittedMessage
	ClusterMessageLevelManagerGetTableIDsForRangeMessage
	ClusterMessageLevelManagerGetTableIDsForRangeResponse
	ClusterMessageLevelManagerRawResponseMessage
	ClusterMessageLevelManagerL0AddMessage
	ClusterMessageLevelManagerApplyChangesMessage
	ClusterMessageLevelManagerLoadLastFlushedVersionMessage
	ClusterMessageLevelManagerLoadLastFlushedVersionResponse
	ClusterMessageLevelManagerStoreLastFlushedVersionMessage
	ClusterMessageLevelManagerGetStatsMessage
	ClusterMessageLevelManagerGetStatsResponse
	ClusterMessageLevelManagerRegisterSlabRetentionMessage
	ClusterMessageLevelManagerUnregisterSlabRetentionMessage
	ClusterMessageLevelManagerGetSlabRetentionMessage
	ClusterMessageLevelManagerGetSlabRetentionResponse
	ClusterMessageCompactionPollMessage
	ClusterMessageCompactionPollResponse
	ClusterMessageLocalObjStoreGet
	ClusterMessageLocalObjStoreGetResponse
	ClusterMessageLocalObjStorePut
	ClusterMessageLocalObjStorePutResponse
	ClusterMessageLocalObjStoreDelete
	ClusterMessageLocalObjStoreDeleteAll
	ClusterMessageLocalObjStoreListObjectsMessage
	ClusterMessageLocalObjStoreListObjectsResponse
	ClusterMessageCommandAvailableMessage
	ClusterMessageShutdownMessage
	ClusterMessageShutdownResponse
	ClusterMessageRemotingTestMessage
)

func TypeForClusterMessage(clusterMessage ClusterMessage) ClusterMessageType {
	switch clusterMessage.(type) {
	case *clustermsgs.QueryMessage:
		return ClusterMessageQueryMessage
	case *clustermsgs.QueryResponse:
		return ClusterMessageQueryResponse
	case *clustermsgs.ReplicateMessage:
		return ClusterMessageReplicateMessage
	case *clustermsgs.FlushMessage:
		return ClusterMessageFlushMessage
	case *clustermsgs.ForwardBatchMessage:
		return ClusterMessageForwardMessage
	case *clustermsgs.VersionsMessage:
		return ClusterMessageVersionsMessage
	case *clustermsgs.GetCurrentVersionMessage:
		return ClusterMessageGetVersionMessage
	case *clustermsgs.VersionCompleteMessage:
		return ClusterMessageVersionCompleteMessage
	case *clustermsgs.VersionFlushedMessage:
		return ClusterMessageVersionFlushedMessage
	case *clustermsgs.FailureDetectedMessage:
		return ClusterMessageFailureDetectedMessage
	case *clustermsgs.GetLastFailureFlushedVersionMessage:
		return ClusterMessageGetLastFailureFlushedVersionMessage
	case *clustermsgs.GetLastFailureFlushedVersionResponse:
		return ClusterMessageGetLastFailureFlushedVersionResponse
	case *clustermsgs.FailureCompleteMessage:
		return ClusterMessageFailureCompleteMessage
	case *clustermsgs.IsFailureCompleteMessage:
		return ClusterMessageIsFailureCompleteMessage
	case *clustermsgs.IsFailureCompleteResponse:
		return ClusterMessageIsFailureCompleteResponse
	case *clustermsgs.LastCommittedRequest:
		return ClusterMessageLastCommittedRequestMessage
	case *clustermsgs.LastCommittedResponse:
		return ClusterMessageLastCommittedResponseMessage
	case *clustermsgs.SetLastCommittedMessage:
		return ClusterMessageSetLastCommittedMessage
	case *clustermsgs.LevelManagerGetTableIDsForRangeMessage:
		return ClusterMessageLevelManagerGetTableIDsForRangeMessage
	case *clustermsgs.LevelManagerGetTableIDsForRangeResponse:
		return ClusterMessageLevelManagerGetTableIDsForRangeResponse
	case *clustermsgs.LevelManagerRawResponse:
		return ClusterMessageLevelManagerRawResponseMessage
	case *clustermsgs.LevelManagerGetSlabRetentionMessage:
		return ClusterMessageLevelManagerGetSlabRetentionMessage
	case *clustermsgs.LevelManagerGetSlabRetentionResponse:
		return ClusterMessageLevelManagerGetSlabRetentionResponse
	case *clustermsgs.LevelManagerApplyChangesRequest:
		return ClusterMessageLevelManagerApplyChangesMessage
	case *clustermsgs.LevelManagerL0AddRequest:
		return ClusterMessageLevelManagerL0AddMessage
	case *clustermsgs.LevelManagerRegisterSlabRetentionMessage:
		return ClusterMessageLevelManagerRegisterSlabRetentionMessage
	case *clustermsgs.LevelManagerUnregisterSlabRetentionMessage:
		return ClusterMessageLevelManagerUnregisterSlabRetentionMessage
	case *clustermsgs.LevelManagerLoadLastFlushedVersionMessage:
		return ClusterMessageLevelManagerLoadLastFlushedVersionMessage
	case *clustermsgs.LevelManagerLoadLastFlushedVersionResponse:
		return ClusterMessageLevelManagerLoadLastFlushedVersionResponse
	case *clustermsgs.LevelManagerStoreLastFlushedVersionMessage:
		return ClusterMessageLevelManagerStoreLastFlushedVersionMessage
	case *clustermsgs.LevelManagerGetStatsMessage:
		return ClusterMessageLevelManagerGetStatsMessage
	case *clustermsgs.LevelManagerGetStatsResponse:
		return ClusterMessageLevelManagerGetStatsResponse
	case *clustermsgs.CompactionPollMessage:
		return ClusterMessageCompactionPollMessage
	case *clustermsgs.CompactionPollResponse:
		return ClusterMessageCompactionPollResponse
	case *clustermsgs.LocalObjStorePutRequest:
		return ClusterMessageLocalObjStorePut
	case *clustermsgs.LocalObjStorePutResponse:
		return ClusterMessageLocalObjStorePutResponse
	case *clustermsgs.LocalObjStoreGetRequest:
		return ClusterMessageLocalObjStoreGet
	case *clustermsgs.LocalObjStoreGetResponse:
		return ClusterMessageLocalObjStoreGetResponse
	case *clustermsgs.LocalObjStoreDeleteRequest:
		return ClusterMessageLocalObjStoreDelete
	case *clustermsgs.LocalObjStoreDeleteAllRequest:
		return ClusterMessageLocalObjStoreDeleteAll
	case *clustermsgs.LocalObjStoreListObjectsRequest:
		return ClusterMessageLocalObjStoreListObjectsMessage
	case *clustermsgs.LocalObjStoreListObjectsResponse:
		return ClusterMessageLocalObjStoreListObjectsResponse
	case *clustermsgs.CommandAvailableMessage:
		return ClusterMessageCommandAvailableMessage
	case *clustermsgs.ShutdownMessage:
		return ClusterMessageShutdownMessage
	case *clustermsgs.ShutdownResponse:
		return ClusterMessageShutdownResponse
	case *remotingmsgs.RemotingTestMessage:
		return ClusterMessageRemotingTestMessage
	default:
		panic(fmt.Sprintf("unknown cluster message %s", reflect.TypeOf(clusterMessage).String()))
	}
}

type ClusterMessage = proto.Message

type ClusterMessageHandler interface {
	HandleMessage(messageHolder MessageHolder, completionFunc func(ClusterMessage, error))
}

type MessageHolder struct {
	Message      ClusterMessage
	ConnectionID int
}

type BlockingClusterMessageHandler interface {
	HandleMessage(messageHolder MessageHolder) (ClusterMessage, error)
}

type blockingClusterMessageHandlerAdaptor struct {
	handler BlockingClusterMessageHandler
}

func NewBlockingClusterMessageHandlerAdaptor(handler BlockingClusterMessageHandler) ClusterMessageHandler {
	return &blockingClusterMessageHandlerAdaptor{handler: handler}
}

func (b *blockingClusterMessageHandlerAdaptor) HandleMessage(messageHolder MessageHolder,
	completionFunc func(ClusterMessage, error)) {
	resp, err := b.handler.HandleMessage(messageHolder)
	completionFunc(resp, err)
}

type TeeBlockingClusterMessageHandler struct {
	Handlers []BlockingClusterMessageHandler
}

func (t *TeeBlockingClusterMessageHandler) HandleMessage(messageHolder MessageHolder) (ClusterMessage, error) {
	for _, handler := range t.Handlers {
		_, err := handler.HandleMessage(messageHolder)
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func DeserializeClusterMessage(data []byte) (ClusterMessage, error) {
	if len(data) == 0 {
		return nil, nil
	}
	b := proto.NewBuffer(data)
	nt, err := b.DecodeVarint()
	if err != nil {
		return nil, errwrap.WithStack(err)
	}
	var msg ClusterMessage
	switch ClusterMessageType(nt) {
	case ClusterMessageQueryMessage:
		msg = &clustermsgs.QueryMessage{}
	case ClusterMessageQueryResponse:
		msg = &clustermsgs.QueryResponse{}
	case ClusterMessageReplicateMessage:
		msg = &clustermsgs.ReplicateMessage{}
	case ClusterMessageForwardMessage:
		msg = &clustermsgs.ForwardBatchMessage{}
	case ClusterMessageFlushMessage:
		msg = &clustermsgs.FlushMessage{}
	case ClusterMessageVersionsMessage:
		msg = &clustermsgs.VersionsMessage{}
	case ClusterMessageGetVersionMessage:
		msg = &clustermsgs.GetCurrentVersionMessage{}
	case ClusterMessageVersionCompleteMessage:
		msg = &clustermsgs.VersionCompleteMessage{}
	case ClusterMessageVersionFlushedMessage:
		msg = &clustermsgs.VersionFlushedMessage{}
	case ClusterMessageFailureDetectedMessage:
		msg = &clustermsgs.FailureDetectedMessage{}
	case ClusterMessageGetLastFailureFlushedVersionMessage:
		msg = &clustermsgs.GetLastFailureFlushedVersionMessage{}
	case ClusterMessageGetLastFailureFlushedVersionResponse:
		msg = &clustermsgs.GetLastFailureFlushedVersionResponse{}
	case ClusterMessageFailureCompleteMessage:
		msg = &clustermsgs.FailureCompleteMessage{}
	case ClusterMessageIsFailureCompleteMessage:
		msg = &clustermsgs.IsFailureCompleteMessage{}
	case ClusterMessageIsFailureCompleteResponse:
		msg = &clustermsgs.IsFailureCompleteResponse{}
	case ClusterMessageLastCommittedRequestMessage:
		msg = &clustermsgs.LastCommittedRequest{}
	case ClusterMessageLastCommittedResponseMessage:
		msg = &clustermsgs.LastCommittedResponse{}
	case ClusterMessageSetLastCommittedMessage:
		msg = &clustermsgs.SetLastCommittedMessage{}
	case ClusterMessageLevelManagerGetTableIDsForRangeMessage:
		msg = &clustermsgs.LevelManagerGetTableIDsForRangeMessage{}
	case ClusterMessageLevelManagerGetTableIDsForRangeResponse:
		msg = &clustermsgs.LevelManagerGetTableIDsForRangeResponse{}
	case ClusterMessageLevelManagerRawResponseMessage:
		msg = &clustermsgs.LevelManagerRawResponse{}
	case ClusterMessageLevelManagerGetSlabRetentionMessage:
		msg = &clustermsgs.LevelManagerGetSlabRetentionMessage{}
	case ClusterMessageLevelManagerGetSlabRetentionResponse:
		msg = &clustermsgs.LevelManagerGetSlabRetentionResponse{}
	case ClusterMessageLevelManagerApplyChangesMessage:
		msg = &clustermsgs.LevelManagerApplyChangesRequest{}
	case ClusterMessageLevelManagerL0AddMessage:
		msg = &clustermsgs.LevelManagerL0AddRequest{}
	case ClusterMessageLevelManagerRegisterSlabRetentionMessage:
		msg = &clustermsgs.LevelManagerRegisterSlabRetentionMessage{}
	case ClusterMessageLevelManagerUnregisterSlabRetentionMessage:
		msg = &clustermsgs.LevelManagerUnregisterSlabRetentionMessage{}
	case ClusterMessageLevelManagerLoadLastFlushedVersionMessage:
		msg = &clustermsgs.LevelManagerLoadLastFlushedVersionMessage{}
	case ClusterMessageLevelManagerLoadLastFlushedVersionResponse:
		msg = &clustermsgs.LevelManagerLoadLastFlushedVersionResponse{}
	case ClusterMessageLevelManagerStoreLastFlushedVersionMessage:
		msg = &clustermsgs.LevelManagerStoreLastFlushedVersionMessage{}
	case ClusterMessageLevelManagerGetStatsMessage:
		msg = &clustermsgs.LevelManagerGetStatsMessage{}
	case ClusterMessageLevelManagerGetStatsResponse:
		msg = &clustermsgs.LevelManagerGetStatsResponse{}
	case ClusterMessageCompactionPollMessage:
		msg = &clustermsgs.CompactionPollMessage{}
	case ClusterMessageCompactionPollResponse:
		msg = &clustermsgs.CompactionPollResponse{}
	case ClusterMessageLocalObjStoreGet:
		msg = &clustermsgs.LocalObjStoreGetRequest{}
	case ClusterMessageLocalObjStoreGetResponse:
		msg = &clustermsgs.LocalObjStoreGetResponse{}
	case ClusterMessageLocalObjStorePut:
		msg = &clustermsgs.LocalObjStorePutRequest{}
	case ClusterMessageLocalObjStorePutResponse:
		msg = &clustermsgs.LocalObjStorePutResponse{}
	case ClusterMessageLocalObjStoreDelete:
		msg = &clustermsgs.LocalObjStoreDeleteRequest{}
	case ClusterMessageLocalObjStoreDeleteAll:
		msg = &clustermsgs.LocalObjStoreDeleteAllRequest{}
	case ClusterMessageLocalObjStoreListObjectsMessage:
		msg = &clustermsgs.LocalObjStoreListObjectsRequest{}
	case ClusterMessageLocalObjStoreListObjectsResponse:
		msg = &clustermsgs.LocalObjStoreListObjectsResponse{}
	case ClusterMessageCommandAvailableMessage:
		msg = &clustermsgs.CommandAvailableMessage{}
	case ClusterMessageShutdownMessage:
		msg = &clustermsgs.ShutdownMessage{}
	case ClusterMessageShutdownResponse:
		msg = &clustermsgs.ShutdownResponse{}
	case ClusterMessageRemotingTestMessage:
		msg = &remotingmsgs.RemotingTestMessage{}
	default:
		return nil, errwrap.Errorf("invalid notification type %d", nt)
	}
	return msg, errwrap.WithStack(b.Unmarshal(msg))
}

type messageType byte

const (
	requestMessageType = iota + 1
	responseMessageType
)

type ClusterRequest struct {
	requiresResponse bool
	sequence         int64
	requestMessage   ClusterMessage
}

type ClusterResponse struct {
	sequence        int64
	ok              bool
	errCode         int
	errMsg          string
	errExtraData    []byte
	responseMessage ClusterMessage
}

type messageHandler func(msgType messageType, msg []byte) error

func (n *ClusterRequest) serialize(buff []byte) ([]byte, error) {
	var rrb byte
	if n.requiresResponse {
		rrb = 1
	} else {
		rrb = 0
	}
	buff = append(buff, rrb)
	buff = encoding.AppendUint64ToBufferLE(buff, uint64(n.sequence))
	nBytes, err := serializeClusterMessage(n.requestMessage)
	if err != nil {
		return nil, errwrap.WithStack(err)
	}
	buff = append(buff, nBytes...)
	return buff, nil
}

func (n *ClusterRequest) deserialize(buff []byte) error {
	offset := 0
	if rrb := buff[offset]; rrb == 1 {
		n.requiresResponse = true
	} else if rrb == 0 {
		n.requiresResponse = false
	} else {
		panic("invalid requires response byte")
	}
	offset++
	var seq uint64
	seq, offset = encoding.ReadUint64FromBufferLE(buff, offset)
	n.sequence = int64(seq)
	var err error
	n.requestMessage, err = DeserializeClusterMessage(buff[offset:])
	return errwrap.WithStack(err)
}

func (n *ClusterResponse) serialize(buff []byte) ([]byte, error) {
	var bok byte
	if n.ok {
		bok = 1
	} else {
		bok = 0
	}
	buff = append(buff, bok)
	if !n.ok {
		buff = encoding.AppendUint32ToBufferLE(buff, uint32(n.errCode))
		buff = encoding.AppendStringToBufferLE(buff, n.errMsg)
		buff = encoding.AppendUint32ToBufferLE(buff, uint32(len(n.errExtraData)))
		if len(n.errExtraData) > 0 {
			buff = append(buff, n.errExtraData...)
		}
	}
	buff = encoding.AppendUint64ToBufferLE(buff, uint64(n.sequence))
	if n.ok && n.responseMessage != nil {
		nBytes, err := serializeClusterMessage(n.responseMessage)
		if err != nil {
			return nil, errwrap.WithStack(err)
		}
		buff = append(buff, nBytes...)
	}
	return buff, nil
}

func (n *ClusterResponse) deserialize(buff []byte) error {
	offset := 0
	if bok := buff[offset]; bok == 1 {
		n.ok = true
	} else if bok == 0 {
		n.ok = false
	} else {
		panic(fmt.Sprintf("invalid ok %d", bok))
	}
	offset++
	if !n.ok {
		var code uint32
		code, offset = encoding.ReadUint32FromBufferLE(buff, offset)
		n.errCode = int(code)
		n.errMsg, offset = encoding.ReadStringFromBufferLE(buff, offset)
		var extraDataLength uint32
		extraDataLength, offset = encoding.ReadUint32FromBufferLE(buff, offset)
		if extraDataLength > 0 {
			l := int(extraDataLength)
			extraCopy := make([]byte, l)
			extraData := buff[offset : offset+l]
			copy(extraCopy, extraData)
			n.errExtraData = extraCopy
			offset += l
		}
	}
	seq, offset := encoding.ReadUint64FromBufferLE(buff, offset)
	n.sequence = int64(seq)
	var err error
	n.responseMessage, err = DeserializeClusterMessage(buff[offset:])
	return err
}

func writeMessage(msgType messageType, msg []byte, conn net.Conn) error {
	if msgType == 0 {
		panic("message type written is zero")
	}
	bytes := make([]byte, 0, messageHeaderSize+len(msg))
	bytes = append(bytes, byte(msgType))
	bytes = encoding.AppendUint32ToBufferLE(bytes, uint32(len(msg)))
	bytes = append(bytes, msg...)
	_, err := conn.Write(bytes)
	if err != nil {
		if err2 := conn.Close(); err2 != nil {
			// Ignore
		}
	}
	if err != nil {
		return errwrap.WithStack(Error{Msg: err.Error()})
	}
	return nil
}

func readMessage(handler messageHandler, conn net.Conn, closeAction func(error)) {
	defer common.TektitePanicHandler()
	var msgBuf []byte
	readBuff := make([]byte, readBuffSize)
	msgLen := -1
	for {
		n, err := conn.Read(readBuff)
		if err != nil {
			closeAction(err)
			// Connection closed
			// We need to close the connection from this side too, to avoid leak of connections in CLOSE_WAIT state
			if err := conn.Close(); err != nil {
				// Do nothing
			}
			return
		}
		msgBuf = append(msgBuf, readBuff[0:n]...)
		msgType := messageType(msgBuf[0])
		for len(msgBuf) >= messageHeaderSize {
			if msgLen == -1 {
				u, _ := encoding.ReadUint32FromBufferLE(msgBuf, 1)
				msgLen = int(u)
			}
			if len(msgBuf) >= messageHeaderSize+msgLen {
				// We got a whole message
				msg := msgBuf[messageHeaderSize : messageHeaderSize+msgLen]
				msg = common.ByteSliceCopy(msg)
				if err := handler(msgType, msg); err != nil {
					log.Errorf("failed to handle message %v", err)
					return
				}
				msgBuf = msgBuf[messageHeaderSize+msgLen:]
				msgLen = -1
			} else {
				break
			}
		}
	}
}

func serializeClusterMessage(clusterMessage ClusterMessage) ([]byte, error) {
	b := proto.NewBuffer(nil)
	nt := TypeForClusterMessage(clusterMessage)
	if nt == ClusterMessageTypeUnknown {
		return nil, errwrap.Errorf("invalid cluster message type %d", nt)
	}
	err := b.EncodeVarint(uint64(nt))
	if err != nil {
		return nil, errwrap.WithStack(err)
	}
	err = b.Marshal(clusterMessage)
	if err != nil {
		return nil, errwrap.WithStack(err)
	}
	return b.Bytes(), nil
}
