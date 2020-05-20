package proto

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

//message.go # message对象及分配逻辑
func TestMessage(t *testing.T) {
	//从池里获取一个对象
	msgs := GetMsgs(1)
	assert.Len(t, msgs, 1)
	//msgs 这里全程引用，放回时回清空内存上的数据，包活释放自己封装的request对象
	PutMsgs(msgs)
	msgs = GetMsgs(1, 1)
	assert.Len(t, msgs, 1)
	PutMsgs(msgs)

	//这个里msg和msgs里消息是一个，内存地址一样
	//NewMessage 封装了getMsgs
	msg := NewMessage()
	msg.Reset()
	msg.clear()
	//消息封装第一个requst
	msg.WithRequest(&mockRequest{})
	//如果只有一个req，则直接返回
	msg.ResetSubs()

	//消息封装第二个requst
	//这里的空结构不占内存
	msg.WithRequest(&mockRequest{})
	//返回封装的第一个request
	req := msg.Request()
	assert.NotNil(t, req)
	//返回封装的所有request（批量返回）
	reqs := msg.Requests()
	assert.Len(t, reqs, 2)
	//是否封装了一批request
	isb := msg.IsBatch()
	assert.True(t, isb)
	msgs = msg.Batch()
	assert.Len(t, msgs, 2)

	msg.ResetSubs()
	req = msg.NextReq()
	assert.NotNil(t, req)

	//处理一批消息
	wg := &sync.WaitGroup{}
	msg.WithWaitGroup(wg) //msg最外层增加wg
	msg.Add()             //添加一个阻塞任务
	msg.Done()            //做完一个阻塞任务

	msg.MarkStart() // 消息开始标记
	time.Sleep(time.Millisecond * 50)
	msg.MarkRead() //消息读取标记
	time.Sleep(time.Millisecond * 50)
	msg.MarkWrite() //消息写入标记
	time.Sleep(time.Millisecond * 50)
	msg.MarkEnd()        //消息结束标记
	ts := msg.TotalDur() //返回消息处理耗时
	assert.NotZero(t, ts)
	ts = msg.RemoteDur()
	assert.NotZero(t, ts)

	msg.WithError(errors.New("some error"))
	err := msg.Err()
	assert.EqualError(t, err, "some error")

	emsg := ErrMessage(errors.New("some error"))
	err = emsg.Err()
	assert.EqualError(t, err, "some error")
}
