/*
Package transport defines interfaces for the Tektite simple RPC framework - this is used for sending RPCs between
different Tektite nodes
*/
package transport

/*
Server is the interface implemented by a transport server
*/
type Server interface {
	// RegisterHandler is called to register a RequestHandler with the specified id
	RegisterHandler(handlerID int, handler RequestHandler) bool
	// Address returns the address of the Server
	Address() string
	// Start starts the server
	Start() error
	// Stop stops the server - releasing all resources
	Stop() error
}

// ResponseWriter is called to send a response back to the client, or an error
type ResponseWriter func(response []byte, err error) error

// RequestHandler is implemented on the server to handle a request and return a response
type RequestHandler func(ctx *ConnectionContext, request []byte, responseBuff []byte, responseWriter ResponseWriter) error

// ConnectionFactory returns a Connection that connects to the specified server address
type ConnectionFactory func(address string) (Connection, error)

type ConnectionContext struct {
	// ConnectionID is a unique (per transport server) id of the connection
	ConnectionID int
	// ClientAddress is the address of the client
	ClientAddress string
}

/*
Connection is the interface implemented by a client connection
*/
type Connection interface {
	// SendRPC sends a request to the server handler and returns a response, or an error
	SendRPC(handlerID int, request []byte) ([]byte, error)
	// SendOneway sends a message to the server handler and returns immediately
	SendOneway(handlerID int, message []byte) error
	// Close closes the connection
	Close() error
}
