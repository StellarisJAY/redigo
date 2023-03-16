package tcp

import (
	"bufio"
	"bytes"
	"sync"
)

var bufferPool = sync.Pool{New: func() interface{} {
	return &bytes.Buffer{}
}}

var readerPool = sync.Pool{New: func() interface{} {
	return bufio.NewReader(nil)
}}

var bytesPool = sync.Pool{New: func() interface{} {
	return make([]byte, 1024)
}}
