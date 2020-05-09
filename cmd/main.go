package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof" // NOTE: use http pprof
	"os"
	"os/signal"
	"strings"
	"syscall"

	"mycache/pkg/log"
	"mycache/proxy"

	// "overlord/pkg/prom"

	// "overlord/proxy/slowlog"
	"mycache/version"
)

var (
	check bool   //校验配置文件
	stat  string //pprof
	// metrics         bool
	confFile        string //proxy conf
	clusterConfFile string //backend conf
	reload          bool   //动态下发配置
	// slowlogFile       string
	// slowlogSlowerThan int
)

type clustersFlag []string

func (c *clustersFlag) String() string {
	return strings.Join([]string(*c), " ")
}

func (c *clustersFlag) Set(n string) error {
	*c = append(*c, n)
	return nil
}

//cli程序的使用说明
var usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of Overlord proxy:\n")
	flag.PrintDefaults()
}

func init() {
	flag.Usage = usage
	flag.BoolVar(&check, "t", false, "conf file check")
	flag.StringVar(&stat, "stat", "", "stat listen addr. high priority than conf.stat.")
	// flag.BoolVar(&metrics, "metrics", false, "proxy support prometheus metrics and reuse stat port.")
	flag.StringVar(&confFile, "conf", "", "conf file of proxy itself.")
	flag.StringVar(&clusterConfFile, "cluster", "", "conf file of backend cluster.")
	flag.BoolVar(&reload, "reload", false, "reloading the servers in cluster config file.")
	// flag.StringVar(&slowlogFile, "slowlog", "", "slowlog is the file where slowlog output")
	// flag.IntVar(&slowlogSlowerThan, "slower-than", 0, "slower-than is the microseconds which slowlog must slower than.")
}

func main() {
	flag.Parse()
	if version.ShowVersion() {
		os.Exit(0)
	}

	if check {
		parseConfig()
		os.Exit(0)
	}
	c, ccs := parseConfig()
	if log.Init(c.Config) {
		defer log.Close()
	}
	// init slowlog if need
	// err := slowlog.Init(slowlogFile)
	// if err != nil {
	// 	log.Errorf("fail to init slowlog due %s", err)
	// }

	// new proxy
	p, err := proxy.New(c)
	if err != nil {
		panic(err)
	}
	defer p.Close()
	p.Serve(ccs)
	if reload {
		go p.MonitorConfChange(clusterConfFile)
	}
	// pprof
	if c.Stat != "" {
		go http.ListenAndServe(c.Stat, nil)
		// if c.Proxy.UseMetrics {
		// 	prom.Init()
		// } else {
		// 	prom.On = false
		// }
	}
	////prom.VersionState(version.Str())
	// hanlde signal
	signalHandler()
}

func parseConfig() (c *proxy.Config, ccs []*proxy.ClusterConfig) {
	if confFile != "" {
		c = &proxy.Config{}
		if err := c.LoadFromFile(confFile); err != nil {
			panic(err)
		}
	} else {
		c = proxy.DefaultConfig()
	}
	// high priority start
	if stat != "" {
		c.Stat = stat
	}
	// if metrics {
	// 	c.Proxy.UseMetrics = metrics
	// }
	// high priority end
	var tmpCCS, err = proxy.LoadClusterConf(clusterConfFile)
	if err != nil {
		panic(err)
	}

	// reset slowlogslowerthan
	// if slowlogSlowerThan > 0 {
	// 	for _, cc := range tmpCCS {
	// 		cc.SlowlogSlowerThan = slowlogSlowerThan
	// 	}
	// }

	ccs = tmpCCS
	return
}

func signalHandler() {
	var ch = make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for {
		log.Infof("overlord proxy version[%s] start serving", version.Str())
		si := <-ch
		log.Infof("overlord proxy version[%s] signal(%s) stop the process", version.Str(), si.String())
		switch si {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			log.Infof("overlord proxy version[%s] exited", version.Str())
			return
		case syscall.SIGHUP:
		default:
			return
		}
	}
}
