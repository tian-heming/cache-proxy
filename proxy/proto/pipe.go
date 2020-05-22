package proto

import (
	"errors"
	"mycache/pkg/hashkit"
	"sync"
	"sync/atomic"
)

const (
	opened = int32(0)
	closed = int32(1)

	pipeMaxCount = 32 //管道里消息最大个数
)

//pipe: https://www.cnblogs.com/luoxn28/p/11794540.html
var (
	errPipeChanFull = errors.New("pipe chan is full")
)

// NodeConnPipe multi MsgPipe for node conns.
// 连接管道
type NodeConnPipe struct {
	conns  int32           //管道里最大的连接数
	inputs []chan *Message //消息chan组
	mps    []*msgPipe      //消息管道组
	l      sync.RWMutex

	errCh chan error

	state int32
}

// NewNodeConnPipe new NodeConnPipe.
// 节点连接的管道，一个管道对象里有多个连接
func NewNodeConnPipe(conns int32, newNc func() NodeConn) (ncp *NodeConnPipe) {
	if conns <= 0 {
		panic("the number of connections cannot be zero")
	}
	ncp = &NodeConnPipe{
		conns:  conns,                        //该pipe的连接数量
		inputs: make([]chan *Message, conns), //初始化conns数量长度的chan数组,用来保存多个chan，chan用来传递*Message消息
		mps:    make([]*msgPipe, conns),      //初始化conns数量长度的*msgPipe数组，用来保存多个msgPipe数据
		errCh:  make(chan error, 1),          //初始化一个chan，用来传递错误信息
	}
	for i := int32(0); i < ncp.conns; i++ {
		//每个chan初始化chan容量
		ncp.inputs[i] = make(chan *Message, pipeMaxCount*pipeMaxCount)
		//新建msg piple赋值给ncp，传入node的连接newNc
		ncp.mps[i] = newMsgPipe(ncp.inputs[i], newNc, ncp.errCh)
	}
	return
}

// Push push message into input chan.
//推送具体消息到pipe的 输入通道input chan
func (ncp *NodeConnPipe) Push(m *Message) {
	m.Add() //阻塞标志
	// 消息类型的双向通道
	var input chan *Message
	ncp.l.RLock() //加锁 处理mesg
	if ncp.state == opened {
		if ncp.conns == 1 {
			input = ncp.inputs[0]
		} else {
			req := m.Request()
			if req != nil {
				crc := int32(hashkit.Crc16(req.Key()))
				input = ncp.inputs[crc%ncp.conns]
			} else {
				// NOTE: impossible!!!
			}
		}
	}
	ncp.l.RUnlock()
	if input != nil {
		select { //循环
		case input <- m: //m输入到input chan里
			m.MarkStartInput() //标记下该m已经输入input通道里
			return
		default:
		}
	}
	m.WithError(errPipeChanFull)
	m.Done() //处理完成一个
}

// ErrorEvent return error chan.
func (ncp *NodeConnPipe) ErrorEvent() <-chan error {
	return ncp.errCh
}

// Close close pipe.
func (ncp *NodeConnPipe) Close() {
	close(ncp.errCh)
	ncp.l.Lock()
	ncp.state = closed
	for _, input := range ncp.inputs {
		close(input)
	}
	ncp.l.Unlock()
}

// msgPipe message pipeline.
// 消息管道
type msgPipe struct {
	nc    atomic.Value    //backend node conn
	newNc func() NodeConn //持有创建连接的回调
	input <-chan *Message //只读chan

	batch [pipeMaxCount]*Message //数组，批量消息
	count int

	errCh chan<- error //只写chan
}

// newMsgPipe new msgPipe and return.
//创建一个消息管道，然后返回改管道
func newMsgPipe(input <-chan *Message, newNc func() NodeConn, errCh chan<- error) (mp *msgPipe) {
	mp = &msgPipe{
		newNc: newNc,
		input: input,
		errCh: errCh,
	}
	mp.nc.Store(newNc())
	go mp.pipe()
	return
}
func (mp *msgPipe) pipe() {
	/*

	 */
	var (
		nc  = mp.nc.Load().(NodeConn)
		m   *Message
		ok  bool
		err error
	)
	for {
		for {
			if m == nil {
				select {
				case m, ok = <-mp.input:
					if !ok {
						nc.Close()
						return
					}
					m.MarkEndInput()
				default:
				}
				if m == nil {
					break
				}
			}
			mp.batch[mp.count] = m
			mp.count++
			//消息写入时间标记
			m.MarkWrite()
			//连接的地址
			nc.Addr()
			//正式写入
			err = nc.Write(m)
			m = nil
			if err != nil {
				goto MEND
			}
			if mp.count >= pipeMaxCount {
				break
			}
		}
		if err == nil && mp.count > 0 {
			if err = nc.Flush(); err != nil {
				goto MEND
			}
			for i := 0; i < mp.count; i++ {
				if err == nil {
					err = nc.Read(mp.batch[i])
					mp.batch[i].MarkRead()
					mp.batch[i].MarkAddr(nc.Addr())
				} else {
					goto MEND
				}
			}
		}
	MEND:
		for i := 0; i < mp.count; i++ {
			msg := mp.batch[i]
			msg.WithError(err) // NOTE: maybe err is nil
			// if prom.On {
			// 	cmd := msg.Request().CmdString()
			// 	duration := msg.RemoteDur()
			// 	msg.Done()
			// 	if err != nil {
			// 		prom.ErrIncr(nc.Cluster(), nc.Addr(), cmd, "network err")
			// 	} else {
			// 		prom.HandleTime(nc.Cluster(), nc.Addr(), cmd, int64(duration/time.Microsecond))
			// 	}
			// } else {
			// 	msg.Done()
			// }
			msg.Done()
		}
		mp.count = 0
		if err != nil {
			nc = mp.reNewNc(nc, err)
			err = nil
		}
		m, ok = <-mp.input // NOTE: avoid infinite loop
		if !ok {
			nc.Close()
			return
		}
		m.MarkEndInput()
	}
}
func (mp *msgPipe) reNewNc(nc NodeConn, err error) NodeConn {
	if err != nil {
		select {
		case mp.errCh <- err: // NOTE: action
		default:
		}
	}
	nc.Close()
	mp.nc.Store(mp.newNc())
	return mp.nc.Load().(NodeConn)
}
