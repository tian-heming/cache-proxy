package redis

import (
	"mycache/pkg/bufio"
	"mycache/pkg/mockconn"
	libnet "mycache/pkg/net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

//读取的流数据是什么结构存到app层的对象里的？？
func TestRespDecode(t *testing.T) {
	ts := []struct {
		Name       string
		Bytes      []byte   //流数据载荷
		ExpectTp   byte     //resp请求类型
		ExpectLen  int      //预期长度
		ExpectData []byte   //预期数据
		ExpectArr  [][]byte //预期数组
	}{
		{
			Name:       "ok",
			Bytes:      []byte("+OK\r\n"), //单行字符串
			ExpectTp:   respString,
			ExpectLen:  0,
			ExpectData: []byte("OK"),
		},
		{
			Name:       "error",
			Bytes:      []byte("-Error message\r\n"), //错误类型
			ExpectTp:   respError,
			ExpectLen:  0,
			ExpectData: []byte("Error message"),
		},
		{
			Name:       "int",
			Bytes:      []byte(":1000\r\n"), //数值类型
			ExpectTp:   respInt,
			ExpectLen:  0,
			ExpectData: []byte("1000"),
		},
		{
			Name:       "bulk",
			Bytes:      []byte("$6\r\nfoobar\r\n"), //多行字符串
			ExpectTp:   respBulk,
			ExpectLen:  0,
			ExpectData: []byte("6\r\nfoobar"),
		},
		{
			Name:       "array1",
			Bytes:      []byte("*2\r\n$3\r\nfoo\r\n$4\r\nbara\r\n"), //字符串数组类型
			ExpectTp:   respArray,
			ExpectLen:  2,
			ExpectData: []byte("2"),
			ExpectArr: [][]byte{
				[]byte("3\r\nfoo"),
				[]byte("4\r\nbara"),
			},
		},
		{
			Name:       "array2",
			Bytes:      []byte("*3\r\n:1\r\n:2\r\n:3\r\n"), //数值数组类型
			ExpectTp:   respArray,
			ExpectLen:  3,
			ExpectData: []byte("3"),
			ExpectArr: [][]byte{
				[]byte("1"),
				[]byte("2"),
				[]byte("3"),
			},
		},
		{
			Name:       "array3",
			Bytes:      []byte("*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n"), //数组嵌套数组类型
			ExpectTp:   respArray,
			ExpectLen:  2,
			ExpectData: []byte("2"),
			ExpectArr: [][]byte{
				[]byte("3"),
				[]byte("2"),
			},
		},
	}
	for _, tt := range ts {
		t.Run(tt.Name, func(t *testing.T) {
			//mockconn.CreateConn 模拟连接处理服务器的连接，默认模拟该连接包含数据tt.Bytes，重试一次
			conn := libnet.NewConn(mockconn.CreateMockConn(tt.Bytes, 1), time.Second, time.Second)
			str := string(tt.Bytes)
			println(str)
			r := &resp{} //resp序列化对象，包含内嵌数组协议项（解码conn的数据到r里）
			r.reset()
			//参数是传一个实现了io.Reader接口的实例对象
			br := bufio.NewReader(conn, bufio.Get(1024))
			br.Read()
			//按行解码
			if err := r.decode(br); err != nil {
				t.Fatalf("decode error:%v", err)
			}
			assert.Equal(t, tt.ExpectTp, r.respType)
			assert.Equal(t, tt.ExpectLen, r.arraySize)
			assert.Equal(t, tt.ExpectData, r.data)
			if len(tt.ExpectArr) > 0 {
				for i, ea := range tt.ExpectArr {
					assert.Equal(t, ea, r.array[i].data)
				}
			}
		})
	}
}

func TestRespEncode(t *testing.T) {
	ts := []struct {
		Name   string
		Resp   *resp
		Expect []byte
	}{
		{
			Name: "ok",
			Resp: &resp{
				respType: respString,
				data:     []byte("OK"),
			},
			Expect: []byte("+OK\r\n"),
		},
		{
			Name: "error",
			Resp: &resp{
				respType: respError,
				data:     []byte("Error message"),
			},
			Expect: []byte("-Error message\r\n"),
		},
		{
			Name: "int",
			Resp: &resp{
				respType: respInt,
				data:     []byte("1000"),
			},
			Expect: []byte(":1000\r\n"),
		},
		{
			Name: "bulk",
			Resp: &resp{
				respType: respBulk,
				data:     []byte("6\r\nfoobar"),
			},
			Expect: []byte("$6\r\nfoobar\r\n"),
		},
		{
			Name: "array1",
			Resp: &resp{
				respType: respArray,
				data:     []byte("2"),
				array: []*resp{
					&resp{
						respType: respBulk,
						data:     []byte("3\r\nfoo"),
					},
					&resp{
						respType: respBulk,
						data:     []byte("4\r\nbara"),
					},
				},
				arraySize: 2,
			},
			Expect: []byte("*2\r\n$3\r\nfoo\r\n$4\r\nbara\r\n"),
		},
		{
			Name: "array2",
			Resp: &resp{
				respType: respArray,
				data:     []byte("3"),
				array: []*resp{
					&resp{
						respType: respInt,
						data:     []byte("1"),
					},
					&resp{
						respType: respInt,
						data:     []byte("2"),
					},
					&resp{
						respType: respInt,
						data:     []byte("3"),
					},
				},
				arraySize: 3,
			},
			Expect: []byte("*3\r\n:1\r\n:2\r\n:3\r\n"),
		},
		{
			Name: "array3",
			Resp: &resp{
				respType: respArray,
				data:     []byte("2"),
				array: []*resp{
					&resp{
						respType: respArray,
						data:     []byte("3"),
						array: []*resp{
							&resp{
								respType: respInt,
								data:     []byte("1"),
							},
							&resp{
								respType: respInt,
								data:     []byte("2"),
							},
							&resp{
								respType: respInt,
								data:     []byte("3"),
							},
						},
						arraySize: 3,
					},
					&resp{
						respType: respArray,
						data:     []byte("2"),
						array: []*resp{
							&resp{
								respType: respString,
								data:     []byte("Foo"),
							},
							&resp{
								respType: respError,
								data:     []byte("Bar"),
							},
						},
						arraySize: 2,
					},
				},
				arraySize: 2,
			},
			Expect: []byte("*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n"),
		},
	}
	for _, tt := range ts {
		t.Run(tt.Name, func(t *testing.T) {
			//mockconn.CreateConn 这个连接是模拟 连接 处理服务器的连接，该连接初始时没有数据nil，重试一次，由客户端发数据给它
			conn := libnet.NewConn(mockconn.CreateMockConn(nil, 1), time.Second, time.Second)
			bw := bufio.NewWriter(conn)

			err := tt.Resp.encode(bw)
			bw.Flush() //flush 函数 对于网络io是将数据从本地缓冲区移到本地写缓冲区缓冲区去，对应文件磁盘io是从缓冲区落盘，对应网络io 则由内核send到流里
			assert.Nil(t, err)

			buf := make([]byte, 1024)
			n, err := conn.Conn.(*mockconn.MockConn).Wbuf.Read(buf) //从服务端socket读数据
			assert.Nil(t, err)
			assert.Equal(t, tt.Expect, buf[:n])
		})
	}
}
