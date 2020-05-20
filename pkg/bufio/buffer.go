package bufio

import (
	"sort"
	"sync"
)

const (
	maxBufferSize     = 512 * 1024 * 1024 //最大容量
	defaultBufferSize = 512               //初始容量
	growFactor        = 2                 //扩容增长因子
)

//全局变量
var (
	sizes []int        //记录已经分配的内存块规格，512,1024,2048这种，对512M的逐渐2倍使用
	pools []*sync.Pool //存放已经分配的内存块，复用池，对512M使用过程中，每片内存使用pool池去复用，避免多次创建和释放 gc压力
)

func init() {
	//初始化sizes
	sizes = make([]int, 0)
	//起始size
	threshold := defaultBufferSize
	for threshold <= maxBufferSize {
		//切片尾部增加，保证顺序
		sizes = append(sizes, threshold)
		threshold *= growFactor //每次都双倍扩容
	}
	//初始化池pool
	//对每片内存都放到池子里管理，开辟了5片，就把这5片内存都放到pool池里，避免gc频繁回收
	pools = make([]*sync.Pool, len(sizes))
	for idx := range pools {
		//按正序初始化到pool里
		initBufPool(idx)
	}
}
func initBufPool(idx int) {
	//按开辟的字节片和它的索引，对应到pools里存放
	pools[idx] = &sync.Pool{
		New: func() interface{} {
			return NewBuffer(sizes[idx])
		},
	}
}

//Buffer 缓冲区
type Buffer struct {
	buf  []byte
	r, w int //r 缓冲区读过的字节量计数，w 缓冲区总共写的字节量计数，这两个参数来标识buf里的数据读写情况
}

//NewBuffer 新建缓冲区
func NewBuffer(size int) *Buffer {
	return &Buffer{buf: make([]byte, size)}
}

//Bytes 读取缓冲区的字节
func (b *Buffer) Bytes() []byte {
	return b.buf[b.r:b.w]
}

//2倍扩容
//申请新的字节切片，把原先的buf字节数据 拷贝一份到新的切片里
func (b *Buffer) grow() {
	nb := make([]byte, len(b.buf)*growFactor)
	copy(nb, b.buf[:b.w])
	b.buf = nb
}

//len 缓冲区字节数据量
func (b *Buffer) len() int {
	return len(b.buf)
}

//Advance 累计读取的字节量
func (b *Buffer) Advance(n int) {
	b.r += n
}

//缓冲区容量收缩，释放已读数据的内存占用
func (b *Buffer) shrink() {
	//本就未读过，则忽略收缩
	if b.r == 0 {
		return
	}
	//未读的数据段 收缩到一个新的b.buf里
	copy(b.buf, b.buf[b.r:b.w])
	//b的数据段为"写-已读"的段
	b.w -= b.r
	//标记该缓冲区为未读
	b.r = 0
}

//返回缓冲中未读取的数据量
func (b *Buffer) buffered() int {
	return b.w - b.r
}

//Reset 清空缓冲区
func (b *Buffer) Reset() {
	// b.buf = b.buf[:0]
	// b.buf = b.buf[:cap(b.buf)]
	b.r, b.w = 0, 0
}

// Get 获取一个size大小的Buffer对象，池化复用这个Buffer对象
func Get(size int) *Buffer {
	//申请的内存块大小还没默认的大，直接开辟默认大小内存块
	if size <= defaultBufferSize {
		size = defaultBufferSize
	}
	//检查这种规格的内存块位置
	i := sort.SearchInts(sizes, size)
	//sizes 规格个数增长大约pools里的对象增长
	if i >= len(pools) {
		return NewBuffer(size)
	}
	//已经存在该规格的，直接到池里拿
	b := pools[i].Get().(*Buffer)
	b.Reset()
	return b
}

// Put 释放Buffer对象的内存到池里
func Put(b *Buffer) {
	i := sort.SearchInts(sizes, b.len())
	if i < len(pools) {
		pools[i].Put(b)
	}
}
