package main

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
)

func numberGen() string {
	daystr := strings.Replace(time.Now().Format("2006-01-02")[2:], "-", "", -1)
	return daystr + fmt.Sprintf("%06v", rand.New(rand.NewSource(time.Now().UnixNano())).Int63n(1000000))
}

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
	//写1000条在默认数据库,ttl:10分钟
	for i := 0; i < 10; i++ {
		thisKey := numberGen()
		for i := 100; i > 0; i-- {
			res, err := rdb.Set(fmt.Sprintf(`%s.%d`, thisKey, i),
				fmt.Sprintf("value%d of %s-key", i, thisKey), 10*time.Minute).Result()
			if err != nil {
				t.Log(res)
			}
			assert.Equal(t, nil, err)
		}

	}
	defer rdb.Close()
}

func TestMapOne(t *testing.T) {
	a := map[string]struct{}{
		"ss": {},
	}
	t.Log(a, &a)
	c, b := a["ss1"]

	t.Log(c, b)
}
