package hashkit

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRingOk(t *testing.T) {
	ring := NewRing("redis_cluster", "crc16")
	assert.NotNil(t, ring)

	ring = NewRing("ketama", "fnv1a_64")
	assert.NotNil(t, ring)
}

//TestFetchPrimesByNumber
func TestFetchPrimesByNumber(t *testing.T) {
	// data := FetchPrimesByNumber(10, false)
	// t.Log(data)
	datamax := FetchPrimesByNumber(4294967296-1, true)
	t.Log(datamax)

}

//FetchPrimesByNumber 2^32空间(4294967296-1)的 全部 或 最大 素数
func FetchPrimesByNumber(number uint32, max bool) (primes []uint32) {
	if number <= 1 {
		return
	}
	for i := number; i > 1; i-- {
		isprimes := true
		for j := uint32(2); j < i; j++ {
			if i%j == 0 {
				isprimes = false
				break
			}
			if a := i - 1; j == a && max {
				primes = append(primes, i)
				return
			}
		}
		if isprimes {
			primes = append(primes, i)
		}
	}
	return
}
