package hashkit

const (
	prime64  = 1099511628211 //64位空间指数
	offset64 = 14695981039346656037

	prime64tw  = 1099511628211 & 0x0000ffff
	offset64tw = 14695981039346656037 & 0xffffffff

	prime32  = 16777619
	offset32 = 2166136261
)

//https://www.cnblogs.com/guodf/p/9677959.html
// node hash(ip)-->uint二进制数值（想得到32位的hash值，方法：取比32位大的最小的位数->位，先算对应64位的hash值，再转换成32位的值即可）https://my.oschina.net/weiweiblog/blog/1665189
func hashFnv1a64(key []byte) uint {
	hash := uint32(offset64tw)
	for _, c := range key { //要hash的数的每个字节byte_of_data
		hash ^= uint32(c)         //hash = hash xor octet_of_data 意思是把当前取来的字节和当前的hash值的第八位做抑或运算
		hash *= uint32(prime64tw) //hash = hash * prime64tw 字符串hash特定范围的数字
	}
	return uint(hash)
}

func hashFnv164(key []byte) uint {
	var hash uint64 = offset64
	for _, c := range key {
		hash *= prime64
		hash ^= uint64(c)
	}
	return uint(uint32(hash))
}

func hashFnv1a32(key []byte) uint {
	var hash uint32 = offset32
	for _, c := range key {
		hash ^= uint32(c)
		hash *= prime32
	}
	return uint(hash)
}

func hashFnv132(key []byte) (value uint) {
	var hash uint32 = offset32
	for _, c := range key {
		hash *= prime32
		hash ^= uint32(c)
	}
	return uint(hash)
}
