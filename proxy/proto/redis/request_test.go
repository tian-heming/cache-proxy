package redis

import (
	"mycache/pkg/bufio"
	"mycache/pkg/mockconn"
	libnet "mycache/pkg/net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// topis：tcp层的流数据怎么和app层的结构数据往来？？
func TestRequestNewRequest(t *testing.T) {
	var bs = []byte("*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n")
	// conn
	conn := libnet.NewConn(mockconn.CreateMockConn(bs, 1), time.Second, time.Second)
	br := bufio.NewReader(conn, bufio.Get(1024)) //缓冲连接对象 【4层tcp层 建立连接传输流数据】
	br.Read()                                    //读取io.Reader实例的数据到br缓冲区区里，直到br写缓冲区满(size:1024)
	req := getReq()                              //获取resp req请求对象，该对象封装了完整的resp请求和回复对象，类似gin的gin.context对象(该对象做了临时池复用优化)
	err := req.resp.decode(br)                   //将br返回的缓冲区数据解码到req.resp对象里 【5+层app层 封装连接到缓冲区，去编解码数据到指定序列化协议对象，这个对象是频繁对象，需要适当优化】
	assert.Nil(t, err)
	assert.Equal(t, mergeTypeNo, req.mType)
	assert.Equal(t, 2, req.resp.arraySize)
	assert.Equal(t, "LLEN", req.CmdString())
	assert.Equal(t, []byte("LLEN"), req.Cmd())
	assert.Equal(t, "mylist", string(req.Key()))
	assert.True(t, req.IsSupport())
	assert.False(t, req.IsCtl())
}

//BenchmarkCmdTypeCheck 压测IsSupport函数
func BenchmarkCmdTypeCheck(b *testing.B) {
	req := getReq()
	req.resp.array = append(req.resp.array, &resp{
		data: []byte("3\r\nSET"),
	})
	for i := 0; i < b.N; i++ {
		//字节编码成字符串做map的key，性能高效
		req.IsSupport()
	}
}
