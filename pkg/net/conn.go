package net

import (
	"errors"
	"net"
	"time"
)

var (
	//ErrConnClosed 链接关闭
	ErrConnClosed = errors.New("connection is closed")
)

// Conn 系统级net.Conn接口内嵌
// add:增加一些超时设置
type Conn struct {
	addr string
	net.Conn
	closed bool

	dialTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration
}

//DialWithTimeout 新建一个有超时控制的conn
func DialWithTimeout(addr string, dialTimeout, readTimeout, writeTimeout time.Duration) (c *Conn) {
	sock, _ := net.DialTimeout("tcp", addr, dialTimeout)
	//tips:
	//内嵌interface net.Conn字段初始化必需是实现了该net.Conn接口的任意非接口类型
	//不是struct自己要实现，是他成员对象要实现这个内嵌接口，他成员实现了他自己也就有了（继承）
	c = &Conn{addr: addr, Conn: sock, dialTimeout: dialTimeout, readTimeout: readTimeout, writeTimeout: writeTimeout}
	return
}

//NewConn 由给到的sock来创建新的conn链接
func NewConn(sock net.Conn, readTimeout, writeTimeout time.Duration) (c *Conn) {
	c = &Conn{Conn: sock, readTimeout: readTimeout, writeTimeout: writeTimeout}
	return
}

//Dup 根据自身拨号信息 重新拨号新建conn链接
func (c *Conn) Dup() *Conn {
	return DialWithTimeout(c.addr, c.dialTimeout, c.readTimeout, c.writeTimeout)
}

//Read 定义链接读超时
func (c *Conn) Read(b []byte) (n int, err error) {
	if c.closed || c.Conn == nil {
		return 0, ErrConnClosed
	}
	if timeout := c.readTimeout; timeout != 0 {
		// 设置conn链接可读的截止日期
		if err = c.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			//链接读超时设置失败，直接返回
			return
		}
	}
	//链接读数据，超读时时返回错误
	n, err = c.Conn.Read(b)
	return
}

//Write 定义链接写超时
func (c *Conn) Write(b []byte) (n int, err error) {
	if c.closed || c.Conn == nil {
		return 0, ErrConnClosed
	}
	if timeout := c.writeTimeout; timeout != 0 {
		if err = c.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			return
		}
	}
	n, err = c.Conn.Write(b)
	return
}

// Close 关闭链接.关闭真正持有链接的对象
func (c *Conn) Close() error {
	if c.Conn != nil && !c.closed {
		c.closed = true
		return c.Conn.Close()
	}
	return nil
}

//Writev 带缓存批量写，减少系统调用write频繁写链接
func (c *Conn) Writev(buf *net.Buffers) (int64, error) {
	if c.closed || c.Conn == nil {
		return 0, ErrConnClosed
	}
	n, err := buf.WriteTo(c.Conn)
	return n, err
}
