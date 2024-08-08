package remoting

import (
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/asl/errwrap"
	"github.com/spirit-labs/tektite/common"
	"sync"
)

type Client struct {
	stopped     bool
	connections sync.Map
	lock        sync.Mutex
	TLSConf     conf.TLSConfig
}

// NewClient creates an instance of a remoting client.
func NewClient(tlsConf conf.TLSConfig) *Client {
	if !tlsConf.Enabled {
		return &Client{}
	}
	return &Client{TLSConf: tlsConf}
}

func (c *Client) Broadcast(request ClusterMessage, serverAddresses ...string) error {
	rh := newBlockingBroadcastRespHandler(len(serverAddresses))
	c.broadcastAsync(&rh.broadcastRespHandler, []ClusterMessage{request}, serverAddresses...)
	return rh.waitForResponse()
}

func (c *Client) BroadcastAsync(completionFunc func(error), request ClusterMessage, serverAddresses ...string) {
	c.broadcastAsync(newBroadcastRespHandler(len(serverAddresses), completionFunc), []ClusterMessage{request}, serverAddresses...)
}

func (c *Client) BroadcastAsyncMultipleRequests(completionFunc func(error), requests []ClusterMessage, serverAddresses ...string) {
	c.broadcastAsync(newBroadcastRespHandler(len(serverAddresses), completionFunc), requests, serverAddresses...)
}

func (c *Client) broadcastAsync(rh *broadcastRespHandler, requests []ClusterMessage, serverAddresses ...string) {
	multipleRequests := len(requests) > 1
	// Tries its best to broadcast to all recipients - does not give up after first error,
	// returns first error if any fail.
	for i, serverAddress := range serverAddresses {
		var request ClusterMessage
		if multipleRequests {
			request = requests[i]
		} else {
			request = requests[0]
		}
		c.sendRequest(serverAddress, rh, request)
	}
}

func (c *Client) BroadcastOneWay(request ClusterMessage, serverAddresses ...string) {
	for _, serverAddress := range serverAddresses {
		c.sendRequest(serverAddress, nil, request)
	}
}

func (c *Client) SendRPC(request ClusterMessage, serverAddress string) (ClusterMessage, error) {
	rh := newBlockingRpcRespHandler()
	c.sendRPCAsync(&rh.rpcRespHandler, request, serverAddress)
	return rh.waitForResponse()
}

func (c *Client) SendRPCAsync(completionFunc func(ClusterMessage, error), request ClusterMessage, serverAddress string) {
	rh := &rpcRespHandler{completionFunc: completionFunc}
	c.sendRPCAsync(rh, request, serverAddress)
}

func (c *Client) sendRPCAsync(rh *rpcRespHandler, request ClusterMessage, serverAddress string) {
	c.sendRequest(serverAddress, rh, request)
}

func (c *Client) sendRequest(serverAddress string, rh responseHandler, request ClusterMessage) {
	conn, err := c.getConnection(serverAddress)
	if err != nil {
		rh.HandleResponse(nil, err)
		return
	}
	// Note we do not delete connections from map on failure - closed connections remain in the map and will be attempted
	// to be recreated next time a request attempt is made. Actively deleting connections introduces a race condition
	// where we could have more than one connection to the same server at same time
	if err := conn.QueueRequest(request, rh); err != nil {
		if common.IsUnavailableError(err) {
			// The write queue is full - we do not close the connection and it's probably fine. The sender will retry
			rh.HandleResponse(nil, err)
			return
		}
		// It's possible the connection is cached but is closed - e.g. it hasn't been used for some time and has
		// been closed by a NAT / firewall - in this case we will try and connect again
		conn, err = c.maybeCreateAndCacheConnection(conn.serverAddress, conn)
		if err != nil {
			rh.HandleResponse(nil, err)
			return
		}
		if err = conn.QueueRequest(request, rh); err != nil {
			rh.HandleResponse(nil, err)
			return
		}
	}
}

func (c *Client) Stop() {
	c.lock.Lock()
	c.stopped = true
	c.lock.Unlock()
	c.connections.Range(func(sa, v interface{}) bool {
		cc, ok := v.(*clientConnection)
		if !ok {
			panic("not a clientConnection")
		}
		cc.Close()
		c.connections.Delete(sa)
		return true
	})
}

func (c *Client) Start() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.stopped = false
}

type respHolder struct {
	resp ClusterMessage
	err  error
}

func (c *Client) getConnection(serverAddress string) (*clientConnection, error) {
	cc, ok := c.connections.Load(serverAddress)
	var conn *clientConnection
	if !ok {
		var err error
		conn, err = c.maybeCreateAndCacheConnection(serverAddress, nil)
		if err != nil {
			return nil, err
		}
	} else {
		conn, ok = cc.(*clientConnection)
		if !ok {
			panic("not a clientConnection")
		}
	}
	return conn, nil
}

func (c *Client) maybeCreateAndCacheConnection(serverAddress string, oldConn *clientConnection) (*clientConnection, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.stopped {
		return nil, Error{Msg: "client is stopped"}
	}
	cl, ok := c.connections.Load(serverAddress) // check again under the lock - another gr might have created one
	if ok {
		cc, ok := cl.(*clientConnection)
		if !ok {
			panic("not a clientConnection")
		}
		// If we're recreating a connection after a failure, then the old conn will still be in the map - we don't
		// want to return that
		if oldConn == nil || oldConn != cc {
			return cc, nil
		}
	}
	tlsConf, err := getClientTLSConfig(c.TLSConf)
	if err != nil {
		return nil, errwrap.WithStack(err)
	}
	cc, err := createConnection(serverAddress, tlsConf)
	if err != nil {
		return nil, err
	}
	c.connections.Store(serverAddress, cc)
	return cc, nil
}

func MaybeConvertError(err error) error {
	var rerr Error
	if errwrap.As(err, &rerr) {
		// Remoting errors are considered retryable (e.g. transient can't connect to another node of the cluster)
		// so we pass back unavailable
		return common.NewTektiteErrorf(common.Unavailable, "transient remoting error %v", err)
	}
	return err
}
