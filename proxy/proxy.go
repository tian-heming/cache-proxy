package proxy

import (
	errs "errors"
	"mycache/pkg/types"
	"mycache/proxy/proto"
	"mycache/proxy/proto/redis"
	"net"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"mycache/pkg/log"
	libnet "mycache/pkg/net"

	"github.com/pkg/errors"
	"gopkg.in/fsnotify.v1"
)

//全局错误变量
var (
	ErrProxyMoreMaxConns = errs.New("Proxy accept more than max connextions")
	ErrProxyReloadIgnore = errs.New("Proxy reload cluster config is ignored")
	ErrProxyReloadFail   = errs.New("Proxy reload cluster config is failed")
)

//Proxy 定义个Proxy类型
type Proxy struct {
	c          *Config
	ccf        string // cluster configure file name
	ccs        []*ClusterConfig
	forwarders map[string]proto.Forwarder
	lock       sync.Mutex
	conns      int32 //服务实例已经维护的tcp连接数

	closed bool
}

// New new a proxy by config.
func New(c *Config) (p *Proxy, err error) {
	if err = c.Validate(); err != nil {
		err = errors.WithStack(err)
		return
	}
	p = &Proxy{}
	p.c = c
	return
}

//Serve is the main accept() loop of proxy server.
func (p *Proxy) Serve(ccs []*ClusterConfig) {
	p.ccs = ccs
	if len(ccs) == 0 {
		log.Warnf("overlord will never listen on any port due to cluster is not specified")
	}
	p.lock.Lock()
	p.forwarders = map[string]proto.Forwarder{}
	p.lock.Unlock()
	for _, cc := range ccs {
		log.Infof("start to serve cluster[%s] with configs %v", cc.Name, *cc)
		p.serve(cc)
	}
}

func (p *Proxy) serve(cc *ClusterConfig) {
	//为后端配置项创建后端目标转发器
	forwarder := NewForwarder(cc)
	//转发器绑定到当前服务实例中
	p.forwarders[cc.Name] = forwarder
	//为后端配置项创建tcp请求监听器
	l, err := Listen(cc.ListenProto, cc.ListenAddr)
	if err != nil {
		panic(err)
	}
	log.Infof("mycache proxy cluster[%s] addr(%s) start listening", cc.Name, cc.ListenAddr)
	go p.accept(cc, l, forwarder)
}
func (p *Proxy) accept(cc *ClusterConfig, l net.Listener, forwarder proto.Forwarder) {
	for {
		if p.closed {
			log.Infof("mycache proxy cluster[%s] addr(%s) stop listen", cc.Name, cc.ListenAddr)
			return
		}
		conn, err := l.Accept()
		if err != nil {
			if conn != nil {
				//丢弃异常链接
				_ = conn.Close()
			}
			log.Errorf("cluster(%s) addr(%s) accept connection error:%+v", cc.Name, cc.ListenAddr, err)
			continue
		}
		//检测最大并发数
		if p.c.Proxy.MaxConnections > 0 {
			if conns := atomic.LoadInt32(&p.conns); conns > p.c.Proxy.MaxConnections {
				// cache type
				// A case:不处理的连接处理（给错误信息返回即可，不进入正常处理调用）
				var encoder proto.ProxyConn
				switch cc.CacheType {
				// case types.CacheTypeMemcache:
				// 	//Memcache业务请求端conn的代理连接（Memcache编码协议的代理）
				// 	encoder = memcache.NewProxyConn(libnet.NewConn(conn, time.Second, time.Second))
				// case types.CacheTypeMemcacheBinary:
				// 	encoder = mcbin.NewProxyConn(libnet.NewConn(conn, time.Second, time.Second))
				case types.CacheTypeRedis:
					encoder = redis.NewProxyConn(libnet.NewConn(conn, time.Second, time.Second), cc.Password)
					// case types.CacheTypeRedisCluster:
					// 	encoder = rclstr.NewProxyConn(libnet.NewConn(conn, time.Second, time.Second), nil, cc.Password)
					// }
					//interface类型不是nil时，表示cc的类型存在匹配项
					if encoder != nil {
						// 该代理连接去按指定编码协议编码消息回写到连接（错误消息回写）
						_ = encoder.Encode(proto.ErrMessage(ErrProxyMoreMaxConns))
						_ = encoder.Flush() //把错误信息写回到指定编码协议的连接
					}
					//
					_ = conn.Close() //关闭这个终端请求的tcp连接
					if log.V(4) {
						log.Warnf("proxy reject connection count(%d) due to more than max(%d)", conns, p.c.Proxy.MaxConnections)
					}
					continue
				}
			}
			atomic.AddInt32(&p.conns, 1)                //原子+1
			NewHandler(p, cc, conn, forwarder).Handle() //.Handle()这个是个go 函数，异步处理
		}
		atomic.AddInt32(&p.conns, 1) //原子+1
		//新建个Handler去处理该连接上的请求
		NewHandler(p, cc, conn, forwarder).Handle()
	}
}

// Close close proxy resource.
func (p *Proxy) Close() error {
	if p.closed {
		return nil
	}
	for _, forwarder := range p.forwarders {
		forwarder.Close()
	}
	p.closed = true
	return nil
}

// MonitorConfChange reload servers.
func (p *Proxy) MonitorConfChange(ccf string) {
	p.ccf = ccf
	// start watcher
	watch, err := fsnotify.NewWatcher()
	if err != nil {
		log.Errorf("failed to create file change watcher and get error:%v", err)
		return
	}
	defer watch.Close()
	absPath, err := filepath.Abs(filepath.Dir(p.ccf))
	if err != nil {
		log.Errorf("failed to get abs path of file:%s and get error:%v", p.ccf, err)
		return
	}
	if err = watch.Add(absPath); err != nil {
		log.Errorf("failed to monitor content change of dir:%s with error:%v", absPath, err)
		return
	}
	log.Infof("proxy is watching changes cluster config absolute path as %s", absPath)
	for {
		if p.closed {
			log.Infof("proxy is closed and exit configure file:%s monitor", p.ccf)
			return
		}
		select {
		case ev := <-watch.Events:
			if ev.Op&fsnotify.Create == fsnotify.Create || ev.Op&fsnotify.Write == fsnotify.Write || ev.Op&fsnotify.Rename == fsnotify.Rename {
				time.Sleep(time.Second)
				newConfs, err := LoadClusterConf(p.ccf)
				if err != nil {
					// prom.ErrIncr(p.ccf, p.ccf, "config reload", err.Error())
					log.Errorf("failed to load conf file:%s and got error:%v", p.ccf, err)
					continue
				}
				changed := parseChanged(newConfs, p.ccs)
				for _, conf := range changed {
					if err = p.updateConfig(conf); err == nil {
						log.Infof("reload successful cluster:%s config succeed", conf.Name)
					} else {
						// prom.ErrIncr(conf.Name, conf.Name, "cluster reload", err.Error())
						log.Errorf("reload failed cluster:%s config and get error:%v", conf.Name, err)
					}
				}
				log.Infof("watcher file:%s occurs event:%s and reload finish", ev.Name, ev.String())
				continue
			}
			if log.V(5) {
				log.Infof("watcher file:%s occurs event:%s and ignore", ev.Name, ev.String())
			}
		case err := <-watch.Errors:
			log.Errorf("watcher dir:%s get error:%v", absPath, err)
			return
		}
	}
}

func (p *Proxy) updateConfig(conf *ClusterConfig) (err error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	f, ok := p.forwarders[conf.Name]
	if !ok {
		err = errors.Wrapf(ErrProxyReloadIgnore, "cluster:%s", conf.Name)
		return
	}
	if err = f.Update(conf.Servers); err != nil {
		err = errors.Wrapf(ErrProxyReloadFail, "cluster:%s error:%v", conf.Name, err)
		return
	}
	for _, oldConf := range p.ccs {
		if oldConf.Name != conf.Name {
			continue
		}
		oldConf.Servers = make([]string, len(conf.Servers), cap(conf.Servers))
		copy(oldConf.Servers, conf.Servers)
		return
	}
	return
}
func parseChanged(newConfs, oldConfs []*ClusterConfig) (changed []*ClusterConfig) {

	changed = make([]*ClusterConfig, 0, len(oldConfs))
	for _, cf := range newConfs {
		sort.Strings(cf.Servers)
	}

	for _, cf := range oldConfs {
		sort.Strings(cf.Servers)
	}

	for _, newConf := range newConfs {
		for _, oldConf := range oldConfs {
			if newConf.Name != oldConf.Name {
				continue
			}

			if !deepEqualOrderedStringSlice(newConf.Servers, oldConf.Servers) {
				changed = append(changed, newConf)
			}
			break
		}
	}
	return
}
func deepEqualOrderedStringSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
