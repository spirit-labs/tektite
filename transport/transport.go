package transport

type Transport interface {
	CreateConnection(address string, handler ResponseHandler) (Connection, error)

	RegisterHandler(handler RequestHandler)
}

type ResponseHandler func(message []byte) error

type RequestHandler func(message []byte, responseWriter ResponseHandler) error

type Connection interface {
	WriteMessage(message []byte) error
	Close() error
}
