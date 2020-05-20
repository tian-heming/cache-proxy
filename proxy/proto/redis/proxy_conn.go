package redis

import (
	"bytes"
	"fmt"
	"strconv"

	"mycache/pkg/bufio"
	"mycache/pkg/conv"
	libnet "mycache/pkg/net"
	"mycache/pkg/types"
	"mycache/proxy/proto"

	"github.com/pkg/errors"
)

var (
	nullBytes            = []byte("-1\r\n")
	okBytes              = []byte("OK\r\n")
	pongDataBytes        = []byte("+PONG\r\n")
	justOkBytes          = []byte("+OK\r\n")
	invalidPasswordBytes = []byte("-ERR invalid password\r\n")
	noAuthBytes          = []byte("-NOAUTH Authentication required.\r\n")
	//notSupportDataBytes = []byte("Error: command not support")
)

// ProxyConn is export for redis cluster.
type ProxyConn = proxyConn

// Bw return proxyConn Writer.
func (pc *ProxyConn) Bw() *bufio.Writer {
	return pc.bw
}

//
type proxyConn struct {
	br        *bufio.Reader //终端连接上的读缓存区（client-->proxy读方向请求数据的缓存区）
	bw        *bufio.Writer //终端连接上的写缓存区 （client<--proxy写方向回复数据的缓存区）
	completed bool          //br缓存是否可读

	resp *resp //br里缓存区数据反序列化结构对象（流-->struct）

	authorized bool   //proxy对客户端连接的认证
	password   string //密码
}

// NewProxyConn creates new redis Encoder and Decoder.
func NewProxyConn(conn *libnet.Conn, password string) proto.ProxyConn {
	r := &proxyConn{
		br:        bufio.NewReader(conn, bufio.Get(1024)),
		bw:        bufio.NewWriter(conn),
		completed: true, //连接代理已就绪
		password:  password,
		resp:      &resp{}, //返回的协议数据
	}
	if password != "" {
		r.authorized = false
	} else {
		r.authorized = true
	}
	return r
}

//传入初始的msgs对象
func (pc *proxyConn) Decode(msgs []*proto.Message) ([]*proto.Message, error) {
	var err error
	//连接上的br数据是否可读
	if pc.completed {
		//把br里的rd数据读到br里b缓存区里
		if err = pc.br.Read(); err != nil {
			return nil, err
		}
		//读完标记为false
		pc.completed = false
	}
	//初始化msgs结构数据
	//流数据读进pc.br缓存区后，解码到msgs结构对象里
	for i := range msgs {
		msgs[i].Type = types.CacheTypeRedis
		// decode
		if err = pc.decode(msgs[i]); err == bufio.ErrBufferFull {
			pc.completed = true
			return msgs[:i], nil
		} else if err != nil {
			return nil, err
		}
		msgs[i].MarkStart()
	}
	return msgs, nil
}

// 解码pc.br缓存区的数据到message里，传入的是msg空对象
func (pc *proxyConn) decode(msg *proto.Message) (err error) {
	// for migrate sync PING process
	//持续读取pc缓存区里的数据到msg里
	for {
		//标记缓存区的读取位置
		mark := pc.br.Mark()
		//缓存区解码到resp对象里
		if err = pc.resp.decode(pc.br); err != nil {
			if err == bufio.ErrBufferFull {
				pc.br.AdvanceTo(mark)
			}
			return
		}
		//如果是数组，则跳出
		if pc.resp.arraySize != 0 {
			break
		}
	}

	//流数据解码到resp
	if pc.resp.arraySize < 1 {
		r := nextReq(msg)
		r.resp.copy(pc.resp)
		return
	}
	//把用户的终端输入的指令字符名转成大写格式（set-->SET）
	conv.UpdateToUpper(pc.resp.array[0].data)
	cmd := pc.resp.array[0].data // NOTE: when array, first is command

	// 匹配redis支持的各种指令
	if bytes.Equal(cmd, cmdMSetBytes) {
		if pc.resp.arraySize%2 == 0 {
			err = ErrBadRequest
			return
		}
		mid := pc.resp.arraySize / 2 // Gets the number of command groups contained in the mset
		for i := 0; i < mid; i++ {
			r := nextReq(msg)
			r.mType = mergeTypeOK
			r.resp.reset() // NOTE: *3\r\n
			r.resp.respType = respArray
			r.resp.data = append(r.resp.data, arrayLenThree...)
			// array resp: mset
			nre1 := r.resp.next() // NOTE: $4\r\nMSET\r\n
			nre1.reset()
			nre1.respType = respBulk
			nre1.data = append(nre1.data, cmdMSetBytes...)
			// array resp: key
			nre2 := r.resp.next() // NOTE: $klen\r\nkey\r\n
			nre2.copy(pc.resp.array[i*2+1])
			// array resp: value
			nre3 := r.resp.next() // NOTE: $vlen\r\nvalue\r\n
			nre3.copy(pc.resp.array[i*2+2])
		}
	} else if bytes.Equal(cmd, cmdMGetBytes) {
		for i := 1; i < pc.resp.arraySize; i++ {
			r := nextReq(msg)
			r.mType = mergeTypeJoin
			r.resp.reset() // NOTE: *2\r\n
			r.resp.respType = respArray
			r.resp.data = append(r.resp.data, arrayLenTwo...)
			// array resp: get
			nre1 := r.resp.next() // NOTE: $3\r\nGET\r\n
			nre1.reset()
			nre1.respType = respBulk
			nre1.data = append(nre1.data, cmdGetBytes...)
			// array resp: key
			nre2 := r.resp.next() // NOTE: $klen\r\nkey\r\n
			nre2.copy(pc.resp.array[i])
		}
	} else if bytes.Equal(cmd, cmdDelBytes) || bytes.Equal(cmd, cmdExistsBytes) {
		for i := 1; i < pc.resp.arraySize; i++ {
			r := nextReq(msg)
			r.mType = mergeTypeCount
			r.resp.reset() // NOTE: *2\r\n
			r.resp.respType = respArray
			r.resp.data = append(r.resp.data, arrayLenTwo...)
			// array resp: get
			nre1 := r.resp.next() // NOTE: $3\r\nDEL\r\n | $6\r\nEXISTS\r\n
			nre1.copy(pc.resp.array[0])
			// array resp: key
			nre2 := r.resp.next() // NOTE: $klen\r\nkey\r\n
			nre2.copy(pc.resp.array[i])
		}
	} else {
		//下一个请求
		r := nextReq(msg)
		//复制pc.resp数据到新的请求r里
		r.resp.copy(pc.resp)
	}
	return
}

//到这里m还是没有值，等待赋值
func nextReq(m *proto.Message) *Request {
	req := m.NextReq()
	if req == nil {
		r := getReq()
		//m传入一个新r请求对象
		m.WithRequest(r)
		return r
	}
	r := req.(*Request)
	r.mType = mergeTypeNo
	return r
}

func (pc *proxyConn) Encode(m *proto.Message) (err error) {
	if err = m.Err(); err != nil {
		se := errors.Cause(err).Error()
		pc.bw.Write(respErrorBytes)
		pc.bw.Write([]byte(se))
		pc.bw.Write(crlfBytes) //crlfBytes 字节流结尾
		return
	}
	req, ok := m.Request().(*Request)
	if !ok {
		return ErrBadAssert
	}
	switch req.mType {
	case mergeTypeOK:
		err = pc.mergeOK(m)
	case mergeTypeJoin:
		err = pc.mergeJoin(m)
	case mergeTypeCount:
		err = pc.mergeCount(m)
	default:
		err = req.reply.encode(pc.bw)
	}
	if err != nil {
		err = errors.WithStack(err)
	}
	return
}

func (pc *proxyConn) mergeOK(m *proto.Message) (err error) {
	_ = pc.bw.Write(respStringBytes)
	err = pc.bw.Write(okBytes)
	return
}

func (pc *proxyConn) mergeCount(m *proto.Message) (err error) {
	var sum = 0
	for _, mreq := range m.Requests() {
		req, ok := mreq.(*Request)
		if !ok {
			return ErrBadAssert
		}
		ival, err := conv.Btoi(req.reply.data)
		if err != nil {
			return ErrBadCount
		}
		sum += int(ival)
	}
	_ = pc.bw.Write(respIntBytes)
	_ = pc.bw.Write([]byte(strconv.Itoa(sum)))
	err = pc.bw.Write(crlfBytes)
	return
}

func (pc *proxyConn) mergeJoin(m *proto.Message) (err error) {
	reqs := m.Requests()
	_ = pc.bw.Write(respArrayBytes)
	if len(reqs) == 0 {
		err = pc.bw.Write(nullBytes)
		return
	}
	_ = pc.bw.Write([]byte(strconv.Itoa(len(reqs))))
	if err = pc.bw.Write(crlfBytes); err != nil {
		return
	}
	for _, mreq := range reqs {
		req, ok := mreq.(*Request)
		if !ok {
			return ErrBadAssert
		}
		if err = req.reply.encode(pc.bw); err != nil {
			return
		}
	}
	return
}

func (pc *proxyConn) Flush() (err error) {
	return pc.bw.Flush()
}

func (pc *proxyConn) CmdCheck(m *proto.Message) (isSpecialCmd bool, err error) {
	isSpecialCmd = false

	req, ok := m.Request().(*Request)
	if !ok {
		return isSpecialCmd, ErrBadAssert
	}

	//不支持的命令
	if !req.IsSupport() {
		err = pc.Bw().Write([]byte(fmt.Sprintf("-ERR unknown command `%s`, with args beginning with:\r\n", req.CmdString())))
		return
	}

	//不是特殊命令
	if !req.IsSpecial() {
		//没有带认证auth
		if !pc.authorized {
			err = pc.Bw().Write(noAuthBytes)
			return
		}
		return
	}

	//特殊命令，auth，ping，quit，command
	if req.IsSpecial() {
		isSpecialCmd = true
		reqData := req.resp.array[0].data
		//字节流判断 消息组里第一个命令是否是auth命令
		if bytes.Equal(reqData, cmdAuthBytes) {
			if bytes.Equal(req.Key(), []byte(pc.password)) {
				pc.authorized = true
				err = pc.Bw().Write(justOkBytes) //返回客户端conn
			} else {
				pc.authorized = false
				err = pc.Bw().Write(invalidPasswordBytes)
			}

		} else if bytes.Equal(reqData, cmdPingBytes) {
			if status := pc.authorized; status {
				err = pc.Bw().Write(pongDataBytes)
			} else {
				err = pc.Bw().Write(noAuthBytes)
			}
		} else if bytes.Equal(reqData, cmdQuitBytes) {
			err = pc.Bw().Write(justOkBytes)
		} else if bytes.Equal(reqData, cmdCommandBytes) {
			err = pc.Bw().Write([]byte(":-1\r\n"))
		}
	} else {
		//常规命令时，校验认证
		if !pc.authorized {
			err = pc.Bw().Write(noAuthBytes) //未认证直接返回
		}
	}

	return
}

func (pc *proxyConn) SetAuthorized(status bool) {
	pc.authorized = status
}

func (pc *proxyConn) IsAuthorized() bool {
	return pc.authorized
}

func (pc *proxyConn) SetPassword(password string) {
	pc.password = password
}

func (pc *proxyConn) GetPassword() string {
	return pc.password
}
