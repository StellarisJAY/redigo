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
