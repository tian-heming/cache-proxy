package redis

import (
	"errors"
	"mycache/pkg/mockconn"
	libnet "mycache/pkg/net"
	"mycache/proxy/proto"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

//主题：代理链接的编解码
func _decodeMessage(t *testing.T, data string) []*proto.Message {
	// 模拟代理服务器成功建立连接
	conn := libnet.NewConn(mockconn.CreateMockConn([]byte(data), 1), time.Second, time.Second)
	//在这个通信连接里获取客户端socket传递的数据
	pc := NewProxyConn(conn, "")
	//初始化个16个message对象的消息组
	msgs := proto.GetMsgs(16)
	//连接上的数据 解码到msgas的内存空间里
	nmsgs, err := pc.Decode(msgs)
	assert.NoError(t, err)
	return nmsgs
}

// 行数据解码成消息
func TestDecodeInlineSet(t *testing.T) {
	data := "set a b\r\n" //[]uint8{115,101,116,32,97,32,98,13,10}
	//构建一个message对象
	nmsgs := _decodeMessage(t, data)
	assert.Len(t, nmsgs, 1)

	//从message对象里断言req请求，请求里包含了resp协议请求体和resp协议回复体
	req := nmsgs[0].Request().(*Request)
	assert.Equal(t, mergeTypeNo, req.mType)
	assert.Equal(t, 3, req.resp.arraySize)
	assert.Equal(t, []byte("3\r\nSET"), req.resp.array[0].data)
	assert.Equal(t, []byte("1\r\na"), req.resp.array[1].data)
	assert.Equal(t, []byte("1\r\nb"), req.resp.array[2].data)
}

func TestDecodeInlineGet(t *testing.T) {
	data := "get a\r\n"
	nmsgs := _decodeMessage(t, data)
	assert.Len(t, nmsgs, 1)

	req := nmsgs[0].Request().(*Request)
	assert.Equal(t, mergeTypeNo, req.mType)
	assert.Equal(t, 2, req.resp.arraySize)
	assert.Equal(t, []byte("3\r\nGET"), req.resp.array[0].data)
	assert.Equal(t, []byte("1\r\na"), req.resp.array[1].data)
}

func TestDecodeBasicOk(t *testing.T) {
	//终端发出的命令（resp规定终端使用*数组结构发请求）
	data := "*2\r\n$3\r\nGET\r\n$4\r\nbaka\r\n"
	nmsgs := _decodeMessage(t, data)
	assert.Len(t, nmsgs, 1)

	req := nmsgs[0].Request().(*Request)
	assert.Equal(t, mergeTypeNo, req.mType)
	assert.Equal(t, 2, req.resp.arraySize)
	assert.Equal(t, "GET", req.CmdString())
	assert.Equal(t, []byte("GET"), req.Cmd())
	assert.Equal(t, "baka", string(req.Key()))
	assert.Equal(t, []byte("2"), req.resp.data)
	assert.Equal(t, []byte("3\r\nGET"), req.resp.array[0].data)
	assert.Equal(t, []byte("4\r\nbaka"), req.resp.array[1].data)
}

func TestDecodeComplexOk(t *testing.T) {
	data := "*3\r\n$4\r\nMGET\r\n$4\r\nbaka\r\n$4\r\nkaba\r\n*5\r\n$4\r\nMSET\r\n$1\r\na\r\n$1\r\nb\r\n$3\r\neee\r\n$5\r\n12345\r\n*3\r\n$4\r\nMGET\r\n$4\r\nenen\r\n$4\r\nnime\r\n*2\r\n$3\r\nGET\r\n$5\r\nabcde\r\n*3\r\n$3\r\nDEL\r\n$1\r\na\r\n$1\r\nb\r\n"
	conn := libnet.NewConn(mockconn.CreateMockConn([]byte(data), 1), time.Second, time.Second)
	pc := NewProxyConn(conn, "")
	// test reuse command
	msgs := proto.GetMsgs(16)
	msgs[1].WithRequest(getReq())
	msgs[1].WithRequest(getReq())
	msgs[1].Reset()
	msgs[2].WithRequest(getReq())
	msgs[2].WithRequest(getReq())
	msgs[2].WithRequest(getReq())
	msgs[2].Reset()
	// decode
	nmsgs, err := pc.Decode(msgs)
	assert.NoError(t, err)
	assert.Len(t, nmsgs, 5)
	// MGET baka
	assert.Len(t, nmsgs[0].Batch(), 2)
	req := msgs[0].Requests()[0].(*Request)
	assert.Equal(t, mergeTypeJoin, req.mType)
	assert.Equal(t, 2, req.resp.arraySize)
	assert.Equal(t, "GET", req.CmdString())
	assert.Equal(t, []byte("GET"), req.Cmd())
	assert.Equal(t, "baka", string(req.Key()))
	assert.Equal(t, []byte("2"), req.resp.data)
	assert.Equal(t, []byte("3\r\nGET"), req.resp.array[0].data)
	assert.Equal(t, []byte("4\r\nbaka"), req.resp.array[1].data)
	// MGET kaba
	req = msgs[0].Requests()[1].(*Request)
	assert.Equal(t, mergeTypeJoin, req.mType)
	assert.Equal(t, 2, req.resp.arraySize)
	assert.Equal(t, "GET", req.CmdString())
	assert.Equal(t, []byte("GET"), req.Cmd())
	assert.Equal(t, "kaba", string(req.Key()))
	assert.Equal(t, []byte("2"), req.resp.data)
	assert.Equal(t, []byte("3\r\nGET"), req.resp.array[0].data)
	assert.Equal(t, []byte("4\r\nkaba"), req.resp.array[1].data)
	// MSET a b
	assert.Len(t, nmsgs[1].Batch(), 2)
	req = msgs[1].Requests()[0].(*Request)
	assert.Equal(t, mergeTypeOK, req.mType)
	assert.Equal(t, 3, req.resp.arraySize)
	assert.Equal(t, "MSET", req.CmdString())
	assert.Equal(t, []byte("MSET"), req.Cmd())
	assert.Equal(t, "a", string(req.Key()))
	assert.Equal(t, []byte("3"), req.resp.data)
	assert.Equal(t, []byte("4\r\nMSET"), req.resp.array[0].data)
	assert.Equal(t, []byte("1\r\na"), req.resp.array[1].data)
	assert.Equal(t, []byte("1\r\nb"), req.resp.array[2].data)
	// MSET eee 12345
	req = msgs[1].Requests()[1].(*Request)
	assert.Equal(t, mergeTypeOK, req.mType)
	assert.Equal(t, 3, req.resp.arraySize)
	assert.Equal(t, "MSET", req.CmdString())
	assert.Equal(t, []byte("MSET"), req.Cmd())
	assert.Equal(t, "eee", string(req.Key()))
	assert.Equal(t, []byte("3"), req.resp.data)
	assert.Equal(t, []byte("4\r\nMSET"), req.resp.array[0].data)
	assert.Equal(t, []byte("3\r\neee"), req.resp.array[1].data)
	assert.Equal(t, []byte("5\r\n12345"), req.resp.array[2].data)
	// MGET enen
	assert.Len(t, nmsgs[0].Batch(), 2)
	req = msgs[2].Requests()[0].(*Request)
	assert.Equal(t, mergeTypeJoin, req.mType)
	assert.Equal(t, 2, req.resp.arraySize)
	assert.Equal(t, "GET", req.CmdString())
	assert.Equal(t, []byte("GET"), req.Cmd())
	assert.Equal(t, "enen", string(req.Key()))
	assert.Equal(t, []byte("2"), req.resp.data)
	assert.Equal(t, []byte("3\r\nGET"), req.resp.array[0].data)
	assert.Equal(t, []byte("4\r\nenen"), req.resp.array[1].data)
	// MGET nime
	req = msgs[2].Requests()[1].(*Request)
	assert.Equal(t, mergeTypeJoin, req.mType)
	assert.Equal(t, 2, req.resp.arraySize)
	assert.Equal(t, "GET", req.CmdString())
	assert.Equal(t, []byte("GET"), req.Cmd())
	assert.Equal(t, "nime", string(req.Key()))
	assert.Equal(t, []byte("2"), req.resp.data)
	assert.Equal(t, []byte("3\r\nGET"), req.resp.array[0].data)
	assert.Equal(t, []byte("4\r\nnime"), req.resp.array[1].data)
	// GET abcde
	req = msgs[3].Requests()[0].(*Request)
	assert.Equal(t, mergeTypeNo, req.mType)
	assert.Equal(t, 2, req.resp.arraySize)
	assert.Equal(t, "GET", req.CmdString())
	assert.Equal(t, []byte("GET"), req.Cmd())
	assert.Equal(t, "abcde", string(req.Key()))
	assert.Equal(t, []byte("2"), req.resp.data)
	assert.Equal(t, []byte("3\r\nGET"), req.resp.array[0].data)
	assert.Equal(t, []byte("5\r\nabcde"), req.resp.array[1].data)

	req = msgs[4].Requests()[0].(*Request)
	assert.Equal(t, mergeTypeCount, req.mType)
	assert.Equal(t, 2, req.resp.arraySize)
	assert.Equal(t, "DEL", req.CmdString())
	assert.Equal(t, "a", string(req.Key()))
	assert.Equal(t, []byte("2"), req.resp.data)
}

func TestEncodeNotSupportCtl(t *testing.T) {
	//msg里封装了rep请求对象，该对象里封装了各自各样具体的resp请求/回复数据项，最后这些消息携带数据在tcp连接通道里往返通信
	msg := proto.NewMessage() //从消息池里msgPool获取消息对象msg，这种承载各自实例数据的中介对象，复用是重置字段值就可以了，不用频繁重写创建
	//捞重置的req对象给msg封装用
	req := getReq()   //从请求体池reqPool里获取请求体对象req，承载各自resp操作命令（resp请求命令，reply响应命令）的resp请求数据，复用是重置字段值就可以了，不用频繁重写创建
	req.resp = &resp{ //resp请求项
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
	}
	//消息里装载具体的resp 请求，并计数
	msg.WithRequest(req)
	conn, buf := mockconn.CreateMockDownStremConn()
	//conn := libnet.NewConn(mockconn.CreateConn(nil, 1), time.Second, time.Second)
	//proxy新建本地socket接入conn的一端，代理
	pc := NewProxyConn(libnet.NewConn(conn, time.Second, time.Second), "")
	_, err := pc.CmdCheck(msg)
	assert.NoError(t, err)
	err = pc.Flush()
	assert.NoError(t, err)
	data := make([]byte, 2048)
	size, err := buf.Read(data)
	assert.NoError(t, err)
	assert.Equal(t, data[:size], []byte("-ERR unknown command `foo`, with args beginning with:\r\n"))

	req.resp.next()
	req.resp.array[0].data = cmdPingBytes
	_, err = pc.CmdCheck(msg)
	assert.NoError(t, err)
	err = pc.Flush()
	assert.NoError(t, err)
	data = make([]byte, 2048)
	size, err = buf.Read(data)
	assert.NoError(t, err)
	assert.Equal(t, data[:size], pongDataBytes)

	req.resp.array[0].data = cmdQuitBytes
	_, err = pc.CmdCheck(msg)
	assert.NoError(t, err)
	err = pc.Flush()
	assert.NoError(t, err)
	data = make([]byte, 2048)
	size, err = buf.Read(data)
	assert.NoError(t, err)
	assert.Equal(t, data[:size], justOkBytes)
}

func TestEncodeMergeOk(t *testing.T) {
	ts := []struct {
		Name   string
		MType  mergeType
		Reply  []*resp
		Expect string
	}{
		{
			Name:  "mergeNotSupport",
			MType: mergeTypeNo,
			Reply: []*resp{
				&resp{
					respType: respString,
					data:     []byte("123456789"),
				},
			},
			Expect: "+123456789\r\n",
		},
		{
			Name:  "mergeOK",
			MType: mergeTypeOK,
			Reply: []*resp{
				&resp{
					respType: respString,
					data:     []byte("OK"),
				},
				&resp{
					respType: respString,
					data:     []byte("OK"),
				},
			},
			Expect: "+OK\r\n",
		},
		{
			Name:  "mergeCount",
			MType: mergeTypeCount,
			Reply: []*resp{
				&resp{
					respType: respInt,
					data:     []byte("1"),
				},
				&resp{
					respType: respInt,
					data:     []byte("1"),
				},
			},
			Expect: ":2\r\n",
		},
		{
			Name:  "mergeJoin",
			MType: mergeTypeJoin,
			Reply: []*resp{
				&resp{
					respType: respString,
					data:     []byte("abc"),
				},
				&resp{
					respType: respString,
					data:     []byte("ooo"),
				},
				&resp{
					respType: respString,
					data:     []byte("mmm"),
				},
			},
			Expect: "*3\r\n+abc\r\n+ooo\r\n+mmm\r\n",
		},
	}
	for _, tt := range ts {
		t.Run(tt.Name, func(t *testing.T) {
			msg := proto.NewMessage()
			for _, rpl := range tt.Reply {
				req := getReq()
				req.mType = tt.MType
				req.reply = rpl
				msg.WithRequest(req)
			}
			if msg.IsBatch() {
				msg.Batch()
			}
			conn, buf := mockconn.CreateMockDownStremConn()
			pc := NewProxyConn(libnet.NewConn(conn, time.Second, time.Second), "")
			err := pc.Encode(msg)
			if !assert.NoError(t, err) {
				return
			}
			err = pc.Flush()
			if !assert.NoError(t, err) {
				return
			}
			data := make([]byte, 2048)
			size, err := buf.Read(data)
			assert.NoError(t, err)
			assert.Equal(t, tt.Expect, string(data[:size]))
		})
	}
}

func TestEncodeWithError(t *testing.T) {
	msg := proto.NewMessage()
	req := getReq()
	req.mType = mergeTypeNo
	req.reply = nil
	msg.WithRequest(req)
	mockErr := errors.New("baka error")
	msg.WithError(mockErr)
	msg.Done()

	conn, buf := mockconn.CreateMockDownStremConn()
	pc := NewProxyConn(libnet.NewConn(conn, time.Second, time.Second), "")
	err := pc.Encode(msg)
	assert.Error(t, err)
	assert.Equal(t, mockErr, err)

	err = pc.Flush()
	assert.NoError(t, err)

	data := make([]byte, 2048)
	size, err := buf.Read(data)
	assert.NoError(t, err)
	assert.Equal(t, "-baka error\r\n", string(data[:size]))
}

func TestEncodeWithPing(t *testing.T) {
	msg := proto.NewMessage()
	req := getReq()
	req.mType = mergeTypeNo
	req.resp = &resp{
		respType: respArray,
		array: []*resp{
			&resp{
				respType: respBulk,
				data:     []byte("4\r\nPING"),
			},
		},
		arraySize: 1,
	}
	req.reply = &resp{}
	msg.WithRequest(req)

	conn, buf := mockconn.CreateMockDownStremConn()
	pc := NewProxyConn(libnet.NewConn(conn, time.Second, time.Second), "")
	_, err := pc.CmdCheck(msg)
	assert.NoError(t, err)
	err = pc.Flush()
	assert.NoError(t, err)

	data := make([]byte, 2048)
	size, err := buf.Read(data)
	assert.NoError(t, err)
	assert.Equal(t, "+PONG\r\n", string(data[:size]))
}
