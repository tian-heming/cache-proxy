package proxy

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"mycache/pkg/log"
	libnet "mycache/pkg/net"

	// "overlord/pkg/prom"
	"mycache/pkg/types"
	"mycache/proxy/proto"

	// "overlord/proxy/proto/memcache"
	// mcbin "overlord/proxy/proto/memcache/binary"
	"mycache/proxy/proto/redis"
	// rclstr "overlord/proxy/proto/redis/cluster"
	// "overlord/proxy/slowlog"

	"github.com/pkg/errors"
)

//handler.go # client端的处理方法，负责分配message和处理message的生命周期，内部有waitgroup管理proxy->node中间的异步行为
const (
	handlerOpening = int32(0)
	handlerClosed  = int32(1)
)

// variables need to change
var (
	// TODO: config and reduce to small
	concurrent    = 2    //最大tcp连接数
	maxConcurrent = 1024 //最大消息数
)

// Handler handle conn.
type Handler struct {
	p  *Proxy
	cc *ClusterConfig

	// slog       slowlog.Handler
	// slowerThan time.Duration

	forwarder proto.Forwarder //

	conn *libnet.Conn    //超时控制终端连接 tcp层
	pc   proto.ProxyConn //封装编解码功能的 超时控制终端连接 app层

	closed int32
	err    error
}

// NewHandler new a conn handler.
// 连接的处理方法，处理之前匹配连接的请求协议类型
func NewHandler(p *Proxy, cc *ClusterConfig, conn net.Conn, forwarder proto.Forwarder) (h *Handler) {
	h = &Handler{
		p:         p,         //当前代理服务实例
		cc:        cc,        //代理的node配置
		forwarder: forwarder, //该类型协议的转发器
	}

	// if cc.SlowlogSlowerThan != 0 {
	// 	h.slowerThan = time.Duration(cc.SlowlogSlowerThan) * time.Microsecond
	// 	h.slog = slowlog.Get(cc.Name)
	// }

	//h.conn 为客户端的连接conn加上rw超时参数（成员实现方法继承）
	h.conn = libnet.NewConn(conn, time.Second*time.Duration(h.p.c.Proxy.ReadTimeout), time.Second*time.Duration(h.p.c.Proxy.WriteTimeout))
	// cache type
	//B case: 进来连接的正常处理调用，
	//根据连接的具体类型来处理
	switch cc.CacheType {
	// case types.CacheTypeMemcache:
	// 	h.pc = memcache.NewProxyConn(h.conn) //该具有超时控制的新conn业务连接的代理 (Memcache编码协议的代理)
	// case types.CacheTypeMemcacheBinary:
	// 	h.pc = mcbin.NewProxyConn(h.conn)
	case types.CacheTypeRedis:
		// 复制proxy代理具体协议的conn
		h.pc = redis.NewProxyConn(h.conn, h.cc.Password) //redis编码协议的代理，并对该连接认证
	// case types.CacheTypeRedisCluster:
	// 	h.pc = rclstr.NewProxyConn(h.conn, forwarder, h.cc.Password) //rediscluster编码协议的代理;redis单实例和redis cluster的编解码协议略有增减
	default:
		panic(types.ErrNoSupportCacheType)
	}
	// prom.ConnIncr(cc.Name)
	return
}

// Handle reads Msg from client connection and dispatchs Msg back to cache servers,
// then reads response from cache server and writes response into client connection.
func (h *Handler) Handle() {
	go h.handle() //新开G异步去处理该连接，h对象里有保存该连接，可以读取数据和处理完数据结果再写入该连接
}

//新G去处理这个任务
func (h *Handler) handle() {
	/*
		var这里如此的意图:go handler接来下的私有全局变量，接下来的处理围绕这些变量引用读写，注意并发安全
	*/
	var (
		messages []*proto.Message    //存放要接收的多个消息
		msgs     []*proto.Message    //存放待发送的多个消息
		wg       = &sync.WaitGroup{} //消息并发时G异步阻塞控制组
		err      error
	)
	//分配最大message并发对象，一开始是默认2个并发对象【只分配内存，没有真实数据】
	messages = h.allocMaxConcurrent(wg, messages, len(msgs))
	/*
		持续读写流，解码流数据，注意并发大时解码对象的内存占用
	*/
	for {
		// 1. read until limit or error
		// 读取流数据解码到分配好的message对象空间
		if msgs, err = h.pc.Decode(messages); err != nil {
			//解码错误 走默认处理流程（回收资源，关闭连接，记录日志）
			h.deferHandle(messages, err)
			return
		}

		// 2. handle special command: AUTH,PING,QUIT,COMMAND
		isSpecialCmd := false
		//成功解码到message数据
		if len(msgs) > 0 {
			//检查msgs里是否有特殊cmd-PING
			isSpecialCmd, err = h.pc.CmdCheck(msgs[0])
			if err != nil {
				//预检不通过，清空写缓存区
				h.pc.Flush()
				//走默认处理流程
				h.deferHandle(messages, err)
				return
			}
		}

		//转发带认证的常规命令到forwarder的node连接??? 仅仅是proxy看下？？
		if !isSpecialCmd && h.pc.IsAuthorized() {
			// 3. send to cluster
			//使用转发器里内置的node_conn发送所有消息到backend node服务器
			//注意:这里有新G参与，处理时要处理完全
			h.forwarder.Forward(msgs)

			//阻塞直到每个msgs都执行done()了，在继续往下执行
			wg.Wait() //阻塞处理 知道消息都发送到input chan里-作业完成，并标记每个msg的MarkStartPipe

			// 4. encode
			for _, msg := range msgs {
				//msg发送结束标记
				msg.MarkEndPipe()
				if err = h.pc.Encode(msg); err != nil {
					h.pc.Flush() //清空写缓存(发送出去)
					h.deferHandle(messages, err)
					return
				}
				msg.MarkEnd()
				// if prom.On {
				// 	prom.ProxyTime(h.cc.Name, msg.Request().CmdString(), int64(msg.TotalDur()/time.Microsecond))
				// }
			}
		}

		//清空连接上的数据
		if err = h.pc.Flush(); err != nil {
			h.deferHandle(messages, err)
			return
		}

		// 5. check slowlog before release resource
		// if h.slowerThan != 0 {
		// 	for _, msg := range msgs {
		// 		if msg.TotalDur() > h.slowerThan {
		// 			h.slog.Record(msg.Slowlog())
		// 		}
		// 	}
		// }
		//发送完 就重置
		for _, msg := range msgs {
			msg.ResetSubs() //先重置该消息的所有子消息
			msg.Reset()     //再重置消息自己
		}
		// 6. alloc MaxConcurrent
		//依据msgs参数扩展消息对象池
		messages = h.allocMaxConcurrent(wg, messages, len(msgs))
	}
}

// 分配并发要求的内存空间，并给每个msg加个共享的并发阻塞器wg
func (h *Handler) allocMaxConcurrent(wg *sync.WaitGroup, msgs []*proto.Message, lastCount int) []*proto.Message {
	var alloc int
	if msgsLength := len(msgs); msgsLength == 0 {
		alloc = concurrent //默认两个并发
	} else if msgsLength < maxConcurrent && msgsLength == lastCount {
		alloc = msgsLength * concurrent //在允许范围内，一次可以执行两个连接的并发
	}
	//如果alloc有值则需要分配内存给消息流msgs存储使用
	if alloc > 0 {
		//添加消息到池子
		proto.PutMsgs(msgs)
		//从池子里获取消息
		//创建池大小的优化，目的是接近于刚刚好大小（alloc）的池空间，（切片过大会也会放在堆上，造成gc压力）
		//GetMsgs的第二个变长参数caps ...int就是用来搞优化的，案例见：https://blog.cyeam.com/golang/2017/02/08/go-optimize-slice-pool
		msgs = proto.GetMsgs(alloc) // TODO: change the msgs by lastCount trending
		for _, msg := range msgs {  //这个msg 是个切片本身值，对他取&地址是要小心，整个循环他的内存地址都是复用的（值地址拷贝），不要直接&取地址的方式对外传值，要先把值放到一个temp新变量里，把这个新变量发出去
			msg.WithWaitGroup(wg) //msg这个是别地方的引用，这里修改会影响到第一次定义的时的值的，就为每个消息都传入一个共享的并发阻塞器，他们自身都可以add()来累加并发组的阻塞任务数量
		}
	}
	return msgs
}

//异常时按deferHandle处理返回
func (h *Handler) deferHandle(msgs []*proto.Message, err error) {
	proto.PutMsgs(msgs)
	h.closeWithError(err)
	return
}

//错位时关闭终端连接
func (h *Handler) closeWithError(err error) {
	if atomic.CompareAndSwapInt32(&h.closed, handlerOpening, handlerClosed) {
		h.err = err
		_ = h.conn.Close()
		atomic.AddInt32(&h.p.conns, -1) // NOTE: decr!!!
		if err == proto.ErrQuit {
			return
		}
		// if prom.On {
		// 	prom.ConnDecr(h.cc.Name)
		// }
		if log.V(2) && errors.Cause(err) != io.EOF {
			log.Warnf("cluster(%s) addr(%s) remoteAddr(%s) handler close error:%+v", h.cc.Name, h.cc.ListenAddr, h.conn.RemoteAddr(), err)
		}
	}
}
