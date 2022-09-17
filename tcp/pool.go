package tcp

import "sync"

var bytesPool = sync.Pool{New: func() interface{} {
	return make([]byte, 1024)
}}
