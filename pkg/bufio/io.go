package bufio

import (
	"bufio"
	"bytes"
	"io"
	libnet "mycache/pkg/net"
	"net"
)

var (
	//ErrBufferFull 缓冲区满 错误
	ErrBufferFull = bufio.ErrBufferFull
)

var (
	//换行符
	crlfBytes = []byte("\r\n")
)

//Reader def
type Reader struct {
	rd  io.Reader //存放可读对象
	b   *Buffer   //内置缓冲区
	err error
}

//NewReader 将io.Reader对象封装成一个带Buffer缓冲的buifio.Reader对象
func NewReader(rd io.Reader, b *Buffer) *Reader {
	return &Reader{rd: rd, b: b}
}

//读取r.rd里连接上的数据到r.b里
func (r *Reader) fill() error {
	// 把r.rd里的连接上的数据追加填充到b.buf里（从r.b.buf[r.b.w:]位置开始填充）
	//返回填充的字节数n
	n, err := r.rd.Read(r.b.buf[r.b.w:])
	//移动b缓冲区的写标识
	r.b.w += n
	if err != nil {
		r.err = err
		return err
	} else if n == 0 {
		return io.ErrNoProgress
	}
	return nil
}

// Advance proxy to buffer advance
func (r *Reader) Advance(n int) {
	r.b.Advance(n)
}

//Mark 读取的标记
func (r *Reader) Mark() int {
	return r.b.r
}

// AdvanceTo reset buffer read pos.
func (r *Reader) AdvanceTo(mark int) {
	r.Advance(mark - r.b.r)
}

// Buffer will return the reference of local buffer
func (r *Reader) Buffer() *Buffer {
	return r.b
}

// Read will trying to read until the buffer is full
func (r *Reader) Read() error {
	if r.err != nil {
		return r.err
	}
	//如果b缓冲区中未读的数据量等于了本身缓冲总容量
	//缓冲已满，
	if r.b.buffered() == r.b.len() {
		//对b缓冲区buf切片进去二倍的扩容
		r.b.grow()
	}
	//如果b缓冲区中已写的数据量等于了本身缓冲总容量
	//写缓冲满，就把已读的丢弃，缩小缓冲占用
	if r.b.w == r.b.len() {
		//对b缓冲区buf切片进行收缩
		r.b.shrink()
	}
	//填充满缓冲区r.buf
	if err := r.fill(); err != io.EOF {
		return err
	}
	return nil
}

// ReadLine will read until meet the first crlf bytes.
func (r *Reader) ReadLine() (line []byte, err error) {
	if r.err != nil {
		return nil, r.err
	}
	//在可读数据段里找\n\r字节值的位置
	idx := bytes.Index(r.b.buf[r.b.r:r.b.w], crlfBytes)
	//没找到
	if idx == -1 {
		line = nil
		err = ErrBufferFull
		return
	}
	//读取完整的一行数据line
	line = r.b.buf[r.b.r : r.b.r+idx+2]
	//移动读取标识，包含\n\r 分隔符
	r.b.r += idx + 2
	return
}

// ReadSlice will read until the delim or return ErrBufferFull.
// It never contains any I/O operation
func (r *Reader) ReadSlice(delim byte) (data []byte, err error) {
	if r.err != nil {
		return nil, r.err
	}
	idx := bytes.IndexByte(r.b.buf[r.b.r:r.b.w], delim)
	if idx == -1 {
		data = nil
		err = ErrBufferFull
		return
	}
	data = r.b.buf[r.b.r : r.b.r+idx+1]
	r.b.r += idx + 1
	return
}

// ReadExact will read n size bytes or return ErrBufferFull.
// It never contains any I/O operation
func (r *Reader) ReadExact(n int) (data []byte, err error) {
	if r.err != nil {
		return nil, r.err
	}
	if r.b.buffered() < n {
		err = ErrBufferFull
		return
	}
	data = r.b.buf[r.b.r : r.b.r+n]
	r.b.r += n
	return
}

const (
	maxWritevSize = 1024
)

// Writer implements buffering for an io.Writer object.
// If an error occurs writing to a Writer, no more data will be
// accepted and all subsequent writes, and Flush, will return the error.
// After all data has been written, the client should call the
// Flush method to guarantee all data has been forwarded to
// the underlying io.Writer.
type Writer struct {
	wr    *libnet.Conn
	bufsp net.Buffers //由数据时，内核到tcp函数自动send到连接流里
	bufs  [][]byte    //写缓冲区 Flush刷到bufsp里
	cnt   int

	err error
}

// NewWriter returns a new Writer whose buffer has the default size.
//将wr对象封装成一个带缓冲的bufio.Writer对象
func NewWriter(wr *libnet.Conn) *Writer {
	return &Writer{wr: wr, bufs: make([][]byte, 0, maxWritevSize)}
}

// Flush writes any buffered data to the underlying io.Writer.
func (w *Writer) Flush() error {
	if w.err != nil {
		return w.err
	}
	if len(w.bufs) == 0 {
		return nil
	}
	w.bufsp = net.Buffers(w.bufs[:w.cnt])
	_, err := w.wr.Writev(&w.bufsp)
	if err != nil {
		w.err = err
	}
	w.bufs = w.bufs[:0]
	w.cnt = 0
	return w.err
}

// Write writes the contents of p into the buffer.
// It returns the number of bytes written.
// If nn < len(p), it also returns an error explaining
// why the write is short.
func (w *Writer) Write(p []byte) (err error) {
	if w.err != nil {
		return w.err
	}
	if p == nil {
		return nil
	}
	w.bufs = append(w.bufs, p)
	w.cnt++
	if len(w.bufs) == maxWritevSize {
		err = w.Flush()
	}
	return
}
