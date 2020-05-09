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

	forwarder proto.Forwarder

	conn *libnet.Conn
	pc   proto.ProxyConn

	closed int32
	err    error
}

// NewHandler new a conn handler.
//创建处理指定编码协议连接的handler对象，真正处理是go这个对象的handle方法（新G处理该方法）
func NewHandler(p *Proxy, cc *ClusterConfig, conn net.Conn, forwarder proto.Forwarder) (h *Handler) {
	h = &Handler{
		p:         p,         //当前代理服务实例
		cc:        cc,        //代理的bakend配置
		forwarder: forwarder, //该类型协议的转发器
	}

	// if cc.SlowlogSlowerThan != 0 {
	// 	h.slowerThan = time.Duration(cc.SlowlogSlowerThan) * time.Microsecond
	// 	h.slog = slowlog.Get(cc.Name)
	// }

	//h.conn 为客户端连接conn包装下超时控制
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
		h.pc = redis.NewProxyConn(h.conn, h.cc.Password) //该具有超时控制的新conn业务连接的代理 (redis编码协议的代理)
	// case types.CacheTypeRedisCluster:
	// 	h.pc = rclstr.NewProxyConn(h.conn, forwarder, h.cc.Password) ///该具有超时控制的新conn业务连接的代理 (rediscluster编码协议的代理);redis单实例和redis cluster的编解码协议略有增减
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
	var (
		messages []*proto.Message    //保存resp消息流
		msgs     []*proto.Message    //保存resp消息这个需要共享，所以定义两个一样的类型
		wg       = &sync.WaitGroup{} //并发同步组
		err      error
	)
	//分配最大消息实例
	messages = h.allocMaxConcurrent(wg, messages, len(msgs))
	for {
		// 1. read until limit or error
		// 解码连接的数据流放到messages
		if msgs, err = h.pc.Decode(messages); err != nil {
			//解码失败时 返回默认的处理写回到连接（终止处理连接返回）
			h.deferHandle(messages, err)
			return
		}

		// 2. handle special command
		isSpecialCmd := false
		if len(msgs) > 0 {
			//检查msgs里是否有特殊cmd
			isSpecialCmd, err = h.pc.CmdCheck(msgs[0])
			if err != nil {
				//检测不通过，则返回默认的处理 写回到连接（终止处理连接返回）
				h.pc.Flush()
				h.deferHandle(messages, err)
				return
			}
		}

		//检测该请求是否带有认证
		if !isSpecialCmd && h.pc.IsAuthorized() {
			// 3. send to cluster
			//使用转发器里内置的conn发送所有消息到backend服务器
			h.forwarder.Forward(msgs)
			//阻塞知道每个msgs都执行done()了，在继续往下执行
			wg.Wait() //阻塞处理 知道消息都发送到input chan里-作业完成，并标记每个msg的MarkStartPipe

			// 4. encode
			for _, msg := range msgs {
				//msg发送结束标记
				msg.MarkEndPipe()
				if err = h.pc.Encode(msg); err != nil {
					h.pc.Flush() //编码协议并发送
					h.deferHandle(messages, err)
					return
				}
				msg.MarkEnd()
				// if prom.On {
				// 	prom.ProxyTime(h.cc.Name, msg.Request().CmdString(), int64(msg.TotalDur()/time.Microsecond))
				// }
			}
		}

		//数据写代理连接
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
		//msg的重置会影响msgs
		for _, msg := range msgs {
			msg.ResetSubs() //先重置该消息的所有子消息
			msg.Reset()     //再重置消息自己
		}
		// 6. alloc MaxConcurrent
		//依据msgs参数扩展消息对象池
		messages = h.allocMaxConcurrent(wg, messages, len(msgs))
	}
}

// 分配消息流内存空间，并给每个msg加个共享的并发阻塞器wg
func (h *Handler) allocMaxConcurrent(wg *sync.WaitGroup, msgs []*proto.Message, lastCount int) []*proto.Message {
	var alloc int
	if msgsLength := len(msgs); msgsLength == 0 {
		alloc = concurrent //默认两个并发
	} else if msgsLength < maxConcurrent && msgsLength == lastCount {
		alloc = msgsLength * concurrent //在允许范围内，一次可以执行两个连接的并发
	}
	//如果alloc不等于0，则需要分配内存给消息流msgs存储使用
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

func (h *Handler) deferHandle(msgs []*proto.Message, err error) {
	proto.PutMsgs(msgs)
	h.closeWithError(err)
	return
}

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
