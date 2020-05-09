package main

import (
	"testing"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
)

func TestRedisClient(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "192.168.80.238:7787", // 使用代理地址
		Password: "",                    // redis认证
		DB:       0,                     // 选择数据库
	})
	pong, err := rdb.Ping().Result() // Output: PONG <nil>
	t.Log("======哈======")
	assert.Equal(t, "PONG", pong)
	t.Log(pong, err)
}
