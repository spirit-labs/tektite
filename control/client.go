package control

import (
	"encoding/binary"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/transport"
)

type Client interface {
	GetOffsets(infos []GetOffsetInfo) ([]int64, error)

	ApplyLsmChanges(regBatch lsm.RegistrationBatch) error

	QueryTablesInRange(keyStart []byte, keyEnd []byte) (lsm.OverlappingTables, error)

	Close() error
}

// client - note this is not goroutine safe!
// on error, the caller must close the connection
type client struct {
	m              *Controller
	clusterVersion int
	address        string
	conn           transport.Connection
	connFactory    transport.ConnectionFactory
	closed         bool
}

var _ Client = &client{}

func (c *client) getConnection() (transport.Connection, error) {
	if c.closed {
		return nil, errors.New("client has been closed")
	}
	if c.conn != nil {
		return c.conn, nil
	}
	conn, err := c.connFactory(c.address)
	if err != nil {
		return nil, err
	}
	c.conn = conn
	return conn, nil
}

func (c *client) Close() error {
	c.address = ""
	c.closed = true
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *client) ApplyLsmChanges(regBatch lsm.RegistrationBatch) error {
	conn, err := c.getConnection()
	if err != nil {
		return err
	}
	req := ApplyChangesRequest{
		ClusterVersion: c.clusterVersion,
		RegBatch:       regBatch,
	}
	request := req.Serialize(createRequestBuffer())
	_, err = conn.SendRPC(transport.HandlerIDControllerApplyChanges, request)
	return err
}

func (c *client) QueryTablesInRange(keyStart []byte, keyEnd []byte) (lsm.OverlappingTables, error) {
	conn, err := c.getConnection()
	if err != nil {
		return nil, err
	}
	req := QueryTablesInRangeRequest{
		ClusterVersion: c.clusterVersion,
		KeyStart:       keyStart,
		KeyEnd:         keyEnd,
	}
	request := req.Serialize(createRequestBuffer())
	respBuff, err := conn.SendRPC(transport.HandlerIDControllerQueryTablesInRange, request)
	if err != nil {
		return nil, err
	}
	return lsm.DeserializeOverlappingTables(respBuff, 0), nil
}

func (c *client) GetOffsets(infos []GetOffsetInfo) ([]int64, error) {
	conn, err := c.getConnection()
	if err != nil {
		return nil, err
	}
	req := GetOffsetsRequest{
		ClusterVersion: c.clusterVersion,
		Infos:          infos,
	}
	request := req.Serialize(createRequestBuffer())
	respBuff, err := conn.SendRPC(transport.HandlerIDControllerGetOffsets, request)
	if err != nil {
		return nil, err
	}
	var resp GetOffsetsResponse
	resp.Deserialize(respBuff, 0)
	return resp.Offsets, nil
}

func createRequestBuffer() []byte {
	buff := make([]byte, 0, 128)                  // Initial size guess
	buff = binary.BigEndian.AppendUint16(buff, 1) // rpc version - currently 1
	return buff
}
