package tcp

import (
	"bytes"
	"sync"
)

var bufferPool = sync.Pool{New: func() interface{} {
	return &bytes.Buffer{}
}}
