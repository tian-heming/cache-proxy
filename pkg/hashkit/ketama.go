package hashkit

import (
	"crypto/md5"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"mycache/pkg/log"
)

const (
	_pointsPerServer = 160 //每个服务实例可以分指定数量节点数
	_maxHostLen      = 64  //服务实例主机名最大长度 ”redis-0“
)

// hash环：https://www.twblogs.net/a/5eb007e886ec4d7b5300d51a
// 一致性hash：https://www.jianshu.com/p/735a3d4789fc
// 资料：https://cloud.tencent.com/developer/article/1488073
//服务器节点信息
type nodeHash struct {
	node string //服务器别名
	hash uint   //hash服务器的区间值之一  4字节（uint32）node hash值
}

//所有服务器节点信息
type tickArray struct {
	nodes  []nodeHash //64个
	length int        //320
}

func (p *tickArray) Len() int           { return p.length }
func (p *tickArray) Less(i, j int) bool { return p.nodes[i].hash < p.nodes[j].hash }
func (p *tickArray) Swap(i, j int)      { p.nodes[i], p.nodes[j] = p.nodes[j], p.nodes[i] }
func (p *tickArray) Sort()              { sort.Sort(p) }

// HashRing ketama hash ring的数据结构.
type HashRing struct {
	nodes []string     //node 实例别名 ["redis2","redis1"]
	spots []int        //权重weight [1,1] int
	ticks atomic.Value //存放所有虚拟节点信息（节点名，节点区域hash值之一）
	lock  sync.Mutex
	hash  func([]byte) uint //该环的hash函数
}

// Ketama new a hash ring with ketama consistency.
// Default hash: fnv1a64
func Ketama() (h *HashRing) {
	h = new(HashRing)
	h.hash = hashFnv1a64 //绑定hash函数，分发服务器节点hash的
	return
}

// newRingWithHash new a hash ring with a hash func.
//生成指定hash函数的散列ring，默认hashFnv1a64散列方法
func newRingWithHash(hash func([]byte) uint) (h *HashRing) {
	h = Ketama()
	h.hash = hash
	return
}

// Init init ring.
func (h *HashRing) Init(nodes []string, spots []int) {
	h.lock.Lock()
	h.init(nodes, spots)
	h.lock.Unlock()
}

// Init init hash ring with nodes.
// 节点哈希环
func (h *HashRing) init(nodes []string, spots []int) {
	if len(nodes) != len(spots) {
		panic("nodes length not equal spots length")
	}
	h.nodes = nodes //服务器 别名列表
	h.spots = spots //服务器设置的权重
	var (
		ticks          []nodeHash   //所有节点hash值
		svrn           = len(nodes) //所有服务器实例数量
		totalw         int          //总权重值
		pointerCnt     int
		pointerPerSvr  int
		pointerPerHash = 4 //一个node对应4种hash
	)
	for _, sp := range spots {
		totalw += sp
	}
	for idx, node := range nodes {
		pct := float64(spots[idx]) / float64(totalw) //node权重占比
		pointerPerSvr = int((pct*_pointsPerServer/4*float64(svrn) + 0.0000000001) * 4)
		for pidx := 1; pidx <= pointerPerSvr/pointerPerHash; pidx++ {
			//每个节点名：“redis-0”
			host := fmt.Sprintf("%s-%d", node, pidx-1) //host=“redis-0”
			if len(host) > _maxHostLen {
				host = host[:_maxHostLen]
			}
			for x := 0; x < pointerPerHash; x++ {
				//node的ketama算法得出的别名节点hash值
				value := h.ketamaHash(host, len(host), x)
				n := &nodeHash{
					node: node,
					hash: value,
				}
				ticks = append(ticks, *n) //保存到全局tick列表里
			}
		}
		pointerCnt += pointerPerSvr
	}
	ts := &tickArray{nodes: ticks, length: len(ticks)}
	ts.Sort()
	h.ticks.Store(ts)
}

func (h *HashRing) ketamaHash(key string, kl, alignment int) (v uint) {
	hs := md5.New()
	_, _ = hs.Write([]byte(key))
	bs := hs.Sum(nil)
	hs.Reset()
	v = (uint(bs[3+alignment*4]&0xFF) << 24) | (uint(bs[2+alignment*4]&0xFF) << 16) | (uint(bs[1+alignment*4]&0xFF) << 8) | (uint(bs[0+alignment*4] & 0xFF))
	return v
}

// AddNode a new node to the hash ring.
// n: name of the server
// s: multiplier for default number of ticks (useful when one cache node has more resources, like RAM, than another)
func (h *HashRing) AddNode(node string, spot int) {
	var (
		tmpNode []string
		tmpSpot []int
		exitst  bool
	)
	h.lock.Lock()
	defer h.lock.Unlock()
	for i, nd := range h.nodes {
		tmpNode = append(tmpNode, nd)
		if nd == node {
			exitst = true
			tmpSpot = append(tmpSpot, spot)
			log.Infof("add exist node %s update spot from %d to %d", nd, h.spots[i], spot)
		} else {
			tmpSpot = append(tmpSpot, h.spots[i])
		}
	}
	if !exitst {
		tmpNode = append(tmpNode, node)
		tmpSpot = append(tmpSpot, spot)
		log.Infof("add node %s spot %d", node, spot)
	}
	h.init(tmpNode, tmpSpot)
}

// DelNode delete a node from the hash ring.
// n: name of the server
// s: multiplier for default number of ticks (useful when one cache node has more resources, like RAM, than another)
func (h *HashRing) DelNode(n string) {
	var (
		tmpNode []string
		tmpSpot []int
		del     bool
	)
	h.lock.Lock()
	defer h.lock.Unlock()
	for i, nd := range h.nodes {
		if nd != n {
			tmpNode = append(tmpNode, nd)
			tmpSpot = append(tmpSpot, h.spots[i])
		} else {
			del = true
			log.Info("ketama del node ", n)
		}
	}
	if del {
		h.init(tmpNode, tmpSpot)
	}
}

// GetNode returns result node by given key.
func (h *HashRing) GetNode(key []byte) (string, bool) {
	ts, ok := h.ticks.Load().(*tickArray)
	if !ok || ts.length == 0 {
		return "", false
	}
	value := h.hash(key)

	i := sort.Search(ts.length, func(i int) bool { return ts.nodes[i].hash >= value })
	if i == ts.length {
		i = 0
	}
	return ts.nodes[i].node, true
}
