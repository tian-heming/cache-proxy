package proto

import (
	"fmt"
	"sync"
	"time"

	"mycache/pkg/types"
)

var (
	defaultTime = time.Now()
)

var msgPool = &sync.Pool{
	//Pool没有时 new一个
	New: func() interface{} {
		return &Message{}
	},
}

// GetMsgs alloc a slice to the message
// 初始化message组
func GetMsgs(n int, caps ...int) []*Message {
	largs := len(caps)
	if largs > 1 {
		panic(fmt.Sprintf("optional argument except 1, but get %d", largs))
	}
	//声明个slice
	var msgs []*Message
	//初始化slice
	if largs == 0 {
		msgs = make([]*Message, n)
	} else {
		msgs = make([]*Message, n, caps[0])
	}
	for idx := range msgs {
		//给msgs内部每个对象空间都赋予n个msg对象
		msgs[idx] = getMsg()
	}
	return msgs
}

// PutMsgs Release message. //清空所有消息对象的内存上的数据，归还到消息池里
func PutMsgs(msgs []*Message) {
	for _, m := range msgs {
		for _, sm := range m.subs {
			sm.clear()
			putMsg(sm) //清空复用的消息对象放回到消息池里
		}
		for _, r := range m.req {
			r.Put()
		}
		m.clear()
		putMsg(m) //清空复用的消息对象放回到消息池里
	}
}

// getMsg get the msg from pool
func getMsg() *Message {
	return msgPool.Get().(*Message)
}

// putMsg put the msg into pool
func putMsg(m *Message) {
	msgPool.Put(m)
}

// Message read from client.
type Message struct {
	Type types.CacheType // 流里的消息类型 "redis"

	req    []Request       //消息里封装了多个req请求，req请求里封装了resp协议请求体和resp响应体，请求体和响应体里有具体的载荷数据
	reqNum int             //message封装的req里真实请求的个数
	subs   []*Message      //内嵌，套娃消息
	wg     *sync.WaitGroup //保证所有消息都被处理，异步阻塞

	// Start Time, Write Time, ReadTime, EndTime, Start Pipe Time, End Pipe Time, Start Pipe Time, End Pipe Time
	st, wt, rt, et, spt, ept, sit, eit time.Time
	addr                               string //""
	err                                error
}

// NewMessage will create new message object.
// this will be used be sub msg req.
func NewMessage() *Message {
	return getMsg()
}

// Reset will clean the msg
// 对象返回池子时要清空重置对象的信息
func (m *Message) Reset() {
	m.Type = types.CacheTypeUnknown
	m.reqNum = 0
	m.st, m.wt, m.rt, m.et, m.spt, m.ept, m.sit, m.eit = defaultTime, defaultTime, defaultTime, defaultTime, defaultTime, defaultTime, defaultTime, defaultTime
	m.err = nil
}

// clear will clean the msg
func (m *Message) clear() {
	m.Reset()
	m.req = nil
	m.wg = nil
	m.subs = nil
}

// TotalDur will return the total duration of a command.
func (m *Message) TotalDur() time.Duration {
	return m.et.Sub(m.st)
}

// RemoteDur will return the remote execute time of remote mc node.
func (m *Message) RemoteDur() time.Duration {
	return m.rt.Sub(m.wt)
}

// WaitWriteDur ...
func (m *Message) WaitWriteDur() time.Duration {
	return m.wt.Sub(m.st)
}

// PreEndDur ...
func (m *Message) PreEndDur() time.Duration {
	return m.et.Sub(m.rt)
}

// PipeDur ...
func (m *Message) PipeDur() time.Duration {
	return m.ept.Sub(m.spt)
}

// InputDur ...
func (m *Message) InputDur() time.Duration {
	return m.eit.Sub(m.sit)
}

// Addr ...
func (m *Message) Addr() string {
	return m.addr
}

// MarkStart will set the start time of the command to now.
func (m *Message) MarkStart() {
	m.st = time.Now()
}

// MarkWrite will set the write time of the command to now.
func (m *Message) MarkWrite() {
	m.wt = time.Now()
}

// MarkRead will set the read time of the command to now.
func (m *Message) MarkRead() {
	m.rt = time.Now()
}

// MarkEnd will set the end time of the command to now.
func (m *Message) MarkEnd() {
	m.et = time.Now()
}

// MarkStartPipe ...
func (m *Message) MarkStartPipe() {
	m.spt = time.Now()
}

// MarkEndPipe ...
func (m *Message) MarkEndPipe() {
	m.ept = time.Now()
}

// MarkStartInput ...
func (m *Message) MarkStartInput() {
	m.sit = time.Now()
}

// MarkEndInput ...
func (m *Message) MarkEndInput() {
	m.eit = time.Now()
}

// MarkAddr ...
func (m *Message) MarkAddr(addr string) {
	m.addr = addr
}

// ResetSubs will return the Msg data to flush and reset
func (m *Message) ResetSubs() {
	if !m.IsBatch() {
		//只有一个request时返回
		return
	}
	for i := range m.subs[:m.reqNum] {
		m.subs[i].Reset()
	}
	m.reqNum = 0
}

// NextReq will iterator itself until nil.
func (m *Message) NextReq() (req Request) {
	if m.reqNum < len(m.req) {
		req = m.req[m.reqNum]
		m.reqNum++
	}
	return
}

// WithRequest with proto request.
func (m *Message) WithRequest(req Request) {
	m.req = append(m.req, req)
	m.reqNum++
}

func (m *Message) setRequest(req Request) {
	m.req = m.req[:0]
	m.reqNum = 0
	m.WithRequest(req)
}

// Request returns proto Msg.
func (m *Message) Request() Request {
	if m.req != nil && len(m.req) > 0 {
		return m.req[0]
	}
	return nil
}

// Requests return all request.
func (m *Message) Requests() []Request {
	if m.reqNum == 0 {
		return nil
	}
	return m.req[:m.reqNum]
}

// IsBatch returns whether or not batch.
//是否包含多个请求对象
func (m *Message) IsBatch() bool {
	return m.reqNum > 1
}

// Batch returns sub Msg if is batch.
func (m *Message) Batch() []*Message {
	slen := m.reqNum
	if slen == 0 {
		return nil
	}
	//比较 取最小
	var min = minInt(len(m.subs), slen)
	for i := 0; i < min; i++ {
		m.subs[i].Type = m.Type
		m.subs[i].setRequest(m.req[i])
	}
	delta := slen - len(m.subs)
	for i := 0; i < delta; i++ {
		//新建两个msg 放到套娃坑位里
		msg := getMsg()
		msg.Type = m.Type
		msg.st = m.st
		msg.setRequest(m.req[min+i])
		msg.WithWaitGroup(m.wg)
		m.subs = append(m.subs, msg)
	}
	//返回套娃里的msg，外部的不返回，切片方式过滤
	return m.subs[:slen]
}

// WithWaitGroup with wait group.
func (m *Message) WithWaitGroup(wg *sync.WaitGroup) {
	m.wg = wg
}

// Add add wait group.
func (m *Message) Add() {
	if m.wg != nil {
		m.wg.Add(1)
	}
}

// Done mark handle message done.
//标记该message处理作业完成
func (m *Message) Done() {
	if m.wg != nil {
		m.wg.Done()
	}
}

// WithError with error.
func (m *Message) WithError(err error) {
	m.err = err
}

// Err returns error.
func (m *Message) Err() error {
	if m.err != nil {
		return m.err
	}
	if !m.IsBatch() {
		return nil
	}
	for _, s := range m.subs[:m.reqNum] {
		if s.err != nil {
			return s.err
		}
	}
	return nil
}

// ErrMessage return err Msg.
func ErrMessage(err error) *Message {
	return &Message{err: err}
}

func minInt(a, b int) int {
	if a > b {
		return b
	}
	return a
}
