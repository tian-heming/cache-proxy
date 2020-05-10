package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
)

func TestRedisClient(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     ":26379", // 使用代理地址
		Password: "",       // redis认证
		DB:       0,        // 选择数据库
	})
	pong, err := rdb.Ping().Result() // Output: PONG <nil>
	t.Log("======哈======")
	assert.Equal(t, "PONG", pong)
	t.Log(pong, err)
	for i := 0; i < 1000; i++ {
		res, err := rdb.Set(fmt.Sprintf("%dkey", i), fmt.Sprintf("value%d of %dkey", i, i), 100*time.Second).Result()
		assert.Equal(t, nil, err)
		t.Log(res)
	}
}
