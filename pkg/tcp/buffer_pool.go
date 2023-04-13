package tcp

import (
	"bufio"
	"bytes"
	"sync"
)

// writeBufferPool 写请求buffer pool， 只在非linux的writer goroutine使用
var writeBufferPool = sync.Pool{New: func() interface{} {
	return &bytes.Buffer{}
}}

// readerPool 读请求的readerPool，只在非linux系统下的reader goroutine中使用
var readerPool = sync.Pool{New: func() interface{} {
	return bufio.NewReader(nil)
}}

// rawBytesPool []byte池，网络请求read时复用缓冲
var rawBytesPool = sync.Pool{New: func() interface{} {
	return make([]byte, 1024)
}}
