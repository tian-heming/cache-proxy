package proto

import "errors"

// frontend 连接关闭
var (
	ErrQuit = errors.New("close client conn")
)

//Request 可转发处理的请求约束
type Request interface {
	CmdString() string
	Cmd() []byte
	Key() []byte
	Put()
}

// ProxyConn decode bytes from client and encode write to conn.
// frontend的连接，对于Message这种结构的封装
type ProxyConn interface {
	Decode([]*Message) ([]*Message, error)
	Encode(msg *Message) error
	Flush() error
	IsAuthorized() bool
	CmdCheck(m *Message) (bool, error)
}

// NodeConn handle Msg to backend cache server and read response.
// backend node的连接（主程内置或新建），对于Message这种结构的封装
type NodeConn interface {
	Write(*Message) error
	Read(*Message) error
	Flush() error
	Close() error
	Addr() string
}

// Pinger for executor ping node.
// backend node的健康检查器
type Pinger interface {
	Ping() error
	Close() error
}

// Forwarder is the interface for backend run and process the messages.
// frontend和backend 之间请求的转发者，对于Message这种结构的封装
type Forwarder interface {
	Forward([]*Message) error
	Close() error
	Update(servers []string) error
}
