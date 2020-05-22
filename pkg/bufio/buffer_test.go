package bufio

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBufferGrowOk(t *testing.T) {
	//测试Get
	b := Get(defaultBufferSize)
	//测试增长
	b.grow()
	assert.Equal(t, 0, b.r)                   //读取过的长度
	assert.Equal(t, 0, b.w)                   //写入数据的长度
	assert.Len(t, b.buf, defaultBufferSize*2) //b.buf对象的长度
	assert.Equal(t, len(b.buf), b.len())
	//b放入池里
	Put(b)
}

func TestBuffer(t *testing.T) {
	b := Get(defaultBufferSize)
	assert.Len(t, b.buf, defaultBufferSize)
	assert.Len(t, b.Bytes(), 0)
	b.w = 1
	assert.Len(t, b.Bytes(), 1)
	b.Reset()
	assert.Len(t, b.Bytes(), 0)
	Put(b)
}

func TestGetOk(t *testing.T) {
	b := Get(defaultBufferSize)
	assert.Len(t, b.buf, defaultBufferSize)

	b = Get(maxBufferSize)
	assert.Len(t, b.buf, maxBufferSize)

	b = Get(maxBufferSize + 1)
	assert.Len(t, b.buf, maxBufferSize+1)
	Put(b)
}

func TestBufferAdvance(t *testing.T) {
	b := Get(defaultBufferSize)
	b.r += 100
	b.Advance(-10)
	assert.Equal(t, 90, b.r)
	Put(b)
}

func TestBufferShrink(t *testing.T) {
	b := Get(defaultBufferSize)
	copy(b.buf, []byte("abcde")) //零拷贝？？
	b.r += 3
	b.w += 5
	b.shrink()
	assert.Equal(t, []byte("de"), b.Bytes())
	Put(b)
}
