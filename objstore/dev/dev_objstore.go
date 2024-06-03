package dev

import (
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/protos/clustermsgs"
	"github.com/spirit-labs/tektite/remoting"
)

type Store struct {
	rServer remoting.Server
	store   *InMemStore
}

func NewDevStore(listenAddress string) *Store {
	rServer := remoting.NewServer(listenAddress, conf.TLSConfig{})
	return &Store{
		rServer: rServer,
		store:   NewInMemStore(0),
	}
}

func (d *Store) Start() error {
	d.rServer.RegisterBlockingMessageHandler(remoting.ClusterMessageLocalObjStoreGet, &getMessageHandler{store: d.store})
	d.rServer.RegisterBlockingMessageHandler(remoting.ClusterMessageLocalObjStoreAdd, &addMessageHandler{store: d.store})
	d.rServer.RegisterBlockingMessageHandler(remoting.ClusterMessageLocalObjStoreDelete, &deleteMessageHandler{store: d.store})
	return d.rServer.Start()
}

func (d *Store) Stop() error {
	return d.rServer.Stop()
}

type getMessageHandler struct {
	store *InMemStore
}

func (g *getMessageHandler) HandleMessage(messageHolder remoting.MessageHolder) (remoting.ClusterMessage, error) {
	gm := messageHolder.Message.(*clustermsgs.LocalObjStoreGetRequest)
	value, err := g.store.Get(gm.Key)
	if err != nil {
		return nil, err
	}
	return &clustermsgs.LocalObjStoreGetResponse{Value: value}, nil
}

type addMessageHandler struct {
	store *InMemStore
}

func (a *addMessageHandler) HandleMessage(messageHolder remoting.MessageHolder) (remoting.ClusterMessage, error) {
	gm := messageHolder.Message.(*clustermsgs.LocalObjStoreAddRequest)
	err := a.store.Put(gm.Key, gm.Value)
	return nil, err
}

type deleteMessageHandler struct {
	store *InMemStore
}

func (d *deleteMessageHandler) HandleMessage(messageHolder remoting.MessageHolder) (remoting.ClusterMessage, error) {
	gm := messageHolder.Message.(*clustermsgs.LocalObjStoreDeleteRequest)
	err := d.store.Delete(gm.Key)
	return nil, err
}

func NewDevStoreClient(address string) *Client {
	rClient := remoting.NewClient(conf.TLSConfig{})
	return &Client{
		rClient: rClient,
		address: address,
	}
}

type Client struct {
	rClient *remoting.Client
	address string
}

func (c *Client) Get(key []byte) ([]byte, error) {
	req := &clustermsgs.LocalObjStoreGetRequest{Key: key}
	resp, err := c.rClient.SendRPC(req, c.address)
	if err != nil {
		return nil, remoting.MaybeConvertError(err)
	}
	vResp := resp.(*clustermsgs.LocalObjStoreGetResponse)
	return vResp.Value, nil
}

func (c *Client) Put(key []byte, value []byte) error {
	req := &clustermsgs.LocalObjStoreAddRequest{Key: key, Value: value}
	_, err := c.rClient.SendRPC(req, c.address)
	if err != nil {
		return remoting.MaybeConvertError(err)
	}
	return nil
}

func (c *Client) Delete(key []byte) error {
	req := &clustermsgs.LocalObjStoreDeleteRequest{Key: key}
	_, err := c.rClient.SendRPC(req, c.address)
	if err != nil {
		return remoting.MaybeConvertError(err)
	}
	return nil
}

func (c *Client) Start() error {
	return nil
}

func (c *Client) Stop() error {
	c.rClient.Stop()
	return nil
}
