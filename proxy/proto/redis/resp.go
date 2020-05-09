package redis

import (
	"bytes"
	"fmt"
	"mycache/pkg/bufio"
	"mycache/pkg/conv"
	"strconv"
)

/**
 * Created by tianheming on 20/5/8.
 */

//定义redis resp的通信数据类型-单字节(ASCII)
type respType = byte

//定义redis resp的基础数据类型标识
const (
	respUnknown respType = '0'
	respError   respType = '-'
	respString  respType = '+'
	respInt     respType = ':'
	respBulk    respType = '$'
	respArray   respType = '*'
)

//定义resp的基础数据的字节流
var (
	respStringBytes = []byte("+")
	respErrorBytes  = []byte("-")
	respIntBytes    = []byte(":")
	respBulkBytes   = []byte("$")
	respArrayBytes  = []byte("*")

	nullDataBytes = []byte("-1")
)

//RESP协议对象,导出类型
type RESP = resp

// Type return resp type.
func (r *RESP) Type() byte {
	return r.respType
}

// Data return resp data.
func (r *RESP) Data() []byte {
	return r.data
}

// Array return resp array.
func (r *RESP) Array() []*RESP {
	return r.array[:r.arraySize]
}

// Decode decode by Reader.
func (r *RESP) Decode(br *bufio.Reader) (err error) {
	return r.decode(br)
}

// Encode encode into Writer.
func (r *RESP) Encode(w *bufio.Writer) (err error) {
	return r.encode(w)
}

//redis resp协议对象
type resp struct {
	respType respType //resp哪种数据类型

	data []byte // 非数组类型的字节流数据

	array     []*resp // 数组类型respArray的数据结构，内嵌resp对象自己，可能是任意类型的resp
	arraySize int     //数组类型respArray的大小,直接复用这个值，省去对array数据字节流的收字节数判断
}

//重置一个resp对象
func (r *resp) reset() {
	r.respType = respUnknown
	r.data = r.data[:0]
	r.arraySize = 0
}

func (r *resp) copy(re *resp) {
	r.reset()
	r.respType = re.respType
	r.data = append(r.data, re.data...)
	for i := 0; i < re.arraySize; i++ {
		nre := r.next()
		nre.copy(re.array[i])
	}
}

func (r *resp) next() *resp {
	if r.arraySize < len(r.array) {
		subResp := r.array[r.arraySize]
		subResp.reset()
		r.arraySize++
		return subResp
	}
	subResp := &resp{}
	subResp.reset()
	r.array = append(r.array, subResp)
	r.arraySize++
	return subResp
}

func (r *resp) decode(br *bufio.Reader) (err error) {
	r.reset()
	// start read
	line, err := br.ReadLine() //逐行读缓冲器（\r\n分割为行）
	if err != nil {
		return err
	}
	respType := line[0] //首字节为数据类型标识位
	r.respType = respType
	switch respType { //按resp的编码协议去拆解这些二进制-ASCII数值数据
	case respString, respInt, respError:
		//如果是简单的字符数据或数值数据，则就地解码成对象
		r.data = append(r.data, line[1:len(line)-2]...)
	case respBulk: //如果是复杂的数据类型 则使用对应的解码方法去反序列化数据
		err = r.decodeBulk(line, br)
	case respArray:
		err = r.decodeArray(line, br)
	default:
		err = r.decodeInline(line)
	}
	return
}

// decodeInline Handle Telnet requests
func (r *resp) decodeInline(line []byte) (err error) {
	fields := bytes.Fields(line)
	flen := len(fields)
	if flen == 0 {
		err = ErrBadRequest
		return
	}
	r.arraySize = flen
	r.data = []byte(strconv.Itoa(flen))
	r.array = make([]*resp, flen)
	r.respType = respArray
	for i, field := range fields {
		r.array[i] = &resp{
			respType:  respBulk,
			data:      []byte(fmt.Sprintf("%d\r\n%s", len(field), field)),
			array:     nil,
			arraySize: 0,
		}
	}
	return
}

func (r *resp) decodeBulk(line []byte, br *bufio.Reader) (err error) {
	ls := len(line)
	bulkLengthBytes := line[1 : ls-2]
	bulkLength, err := conv.Btoi(bulkLengthBytes)
	if err != nil {
		return
	}
	if bulkLength == -1 {
		r.data = r.data[:0]
		return
	}
	br.Advance(-ls)
	all := ls + int(bulkLength) + 2
	data, err := br.ReadExact(all)
	if err == bufio.ErrBufferFull {
		return err
	} else if err != nil {
		return
	}
	r.data = append(r.data, data[1:len(data)-2]...)
	return
}

func (r *resp) decodeArray(line []byte, br *bufio.Reader) (err error) {
	ls := len(line)
	arrayLengthBytes := line[1 : ls-2]
	arrayLength, err := conv.Btoi(arrayLengthBytes)
	if err != nil {
		return
	}
	if arrayLength == -1 {
		r.data = r.data[:0]
		return
	}
	r.data = append(r.data, arrayLengthBytes...)
	mark := br.Mark()
	for i := 0; i < int(arrayLength); i++ {
		nre := r.next()
		if err = nre.decode(br); err != nil {
			br.AdvanceTo(mark)
			br.Advance(-ls)
			return
		}
	}
	return
}
func (r *resp) encode(w *bufio.Writer) (err error) {
	switch r.respType {
	case respInt, respString, respError:
		err = r.encodePlain(w)
	case respBulk:
		err = r.encodeBulk(w)
	case respArray:
		err = r.encodeArray(w)
	}
	return
}

func (r *resp) encodePlain(w *bufio.Writer) (err error) {
	switch r.respType {
	case respInt:
		_ = w.Write(respIntBytes)
	case respError:
		_ = w.Write(respErrorBytes)
	case respString:
		_ = w.Write(respStringBytes)
	}
	if len(r.data) > 0 {
		_ = w.Write(r.data)
	}
	err = w.Write(crlfBytes) //命令以CRLF字符结尾
	return
}

func (r *resp) encodeBulk(w *bufio.Writer) (err error) {
	_ = w.Write(respBulkBytes)
	if len(r.data) > 0 {
		_ = w.Write(r.data)
	} else {
		_ = w.Write(nullDataBytes)
	}
	err = w.Write(crlfBytes)
	return
}

func (r *resp) encodeArray(w *bufio.Writer) (err error) {
	_ = w.Write(respArrayBytes)
	if len(r.data) > 0 {
		_ = w.Write(r.data)
	} else {
		_ = w.Write(nullDataBytes)
	}
	_ = w.Write(crlfBytes)
	for i := 0; i < r.arraySize; i++ {
		if err = r.array[i].encode(w); err != nil {
			return
		}
	}
	return
}
