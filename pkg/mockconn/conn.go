package mockconn

import (
	"bytes"
	"io"
	"net"
	"sync/atomic"
	"time"
)

const (
	stateClosed  = 1
	stateOpening = 0
)

type mockAddr string

func (m mockAddr) Network() string {
	return "tcp"
}
func (m mockAddr) String() string {
	return string(m)
}

//MockConn  mock tcp链接
type MockConn struct {
	addr   mockAddr
	rbuf   *bytes.Buffer
	Wbuf   *bytes.Buffer
	data   []byte
	repeat int
	Err    error
	closed int32
}

//读连接
func (m *MockConn) Read(b []byte) (n int, err error) {
	//检查链接是否关闭
	if atomic.LoadInt32(&m.closed) == stateClosed {
		return 0, io.EOF
	}

	//检查连接是否有错误
	if m.Err != nil {
		err = m.Err
		return
	}
	//是否有数据多次写设定
	if m.repeat > 0 {
		//数据写入连接的rbuf里
		m.rbuf.Write(m.data)
		m.repeat--
	}
	//从连接的rbuf里读取len(b)的字节数据，返回读取字节数
	return m.rbuf.Read(b)
}

//写连接
func (m *MockConn) Write(b []byte) (n int, err error) {
	if atomic.LoadInt32(&m.closed) == stateClosed {
		return 0, io.EOF
	}
	if m.Err != nil {
		err = m.Err
		return
	}
	//b指令数据写入到 连接的Wbuf
	return m.Wbuf.Write(b)
}

//批量写到连接
func (m *MockConn) writeBuffers(buf *net.Buffers) (int64, error) {
	if m.Err != nil {
		return 0, m.Err
	}
	return buf.WriteTo(m.Wbuf)
}

//Close 设定closed为true，连接关闭标识
func (m *MockConn) Close() error {
	atomic.StoreInt32(&m.closed, stateClosed)
	return nil
}

// LocalAddr conn的本端地址
func (m *MockConn) LocalAddr() net.Addr { return m.addr }

//RemoteAddr conn的远端地址
func (m *MockConn) RemoteAddr() net.Addr { return m.addr }

//SetDeadline def
func (m *MockConn) SetDeadline(t time.Time) error { return nil }

//SetReadDeadline def
func (m *MockConn) SetReadDeadline(t time.Time) error { return nil }

//SetWriteDeadline def
func (m *MockConn) SetWriteDeadline(t time.Time) error { return nil }

//CreateMockConn 模拟一个proxy的上游tcp连接（client tcp连接--->proxy）
func CreateMockConn(data []byte, r int) net.Conn {
	mconn := &MockConn{
		addr:   "127.0.0.1:12345",
		rbuf:   bytes.NewBuffer(nil),
		Wbuf:   new(bytes.Buffer),
		data:   data, //模拟连接上的数据存储位置
		repeat: r,
	}
	return mconn
}

//CreateMockDownStremConn  模拟一个proxy的下游tcp连接（proxy<---backend tcp conn）
func CreateMockDownStremConn() (net.Conn, *bytes.Buffer) {
	buf := new(bytes.Buffer)
	mconn := &MockConn{
		addr: "127.0.0.1:12345",
		Wbuf: buf,
	}
	return mconn, buf
}
