package buffer

type RingBuffer struct {
	buf    []byte
	cap    int // cap 是ring buffer底层数组的大小
	length int // length 是元素个数
	rIdx   int
	wIdx   int
}

func NewRingBuffer(size int) *RingBuffer {
	size = ceilPowerOfTwo(size)
	return &RingBuffer{
		buf:    make([]byte, size),
		cap:    size,
		length: 0,
		rIdx:   0,
		wIdx:   0,
	}
}

func (r *RingBuffer) Read(bytes []byte) (int, error) {
	n := len(bytes)
	if n == 0 {
		return 0, nil
	}
	if r.length < n {
		n = r.length
	}
	if r.wIdx > r.rIdx {
		copy(bytes, r.buf[r.rIdx:r.rIdx+n])
		r.rIdx += n
	} else {
		r1 := r.cap - r.rIdx
		if n <= r1 {
			copy(bytes, r.buf[r.rIdx:])
			r.rIdx += n
		} else {
			copy(bytes, r.buf[r.rIdx:])
			remain := n - r1
			copy(bytes[r1:], r.buf[0:remain])
			r.rIdx = remain
		}
	}
	if r.rIdx == r.cap {
		r.rIdx = 0
	}
	r.length -= n
	return n, nil
}

func (r *RingBuffer) Write(bytes []byte) (int, error) {
	n := len(bytes)
	if n == 0 {
		return 0, nil
	}
	freeSpace := r.Available()
	if freeSpace < n {
		r.grow(r.cap + n - freeSpace)
	}
	if r.wIdx >= r.rIdx {
		cap1 := r.cap - r.wIdx
		if cap1 >= n {
			copy(r.buf[r.wIdx:], bytes)
			r.wIdx += n
		} else {
			copy(r.buf[r.wIdx:], bytes[:cap1])
			remain := n - cap1
			copy(r.buf, bytes[cap1:])
			r.wIdx = remain
		}
	} else {
		copy(r.buf[r.wIdx:], bytes)
		r.wIdx += n
	}
	if r.wIdx == r.cap {
		r.wIdx = 0
	}
	r.length += n
	return n, nil
}

// grow buffer扩容到目标大小
func (r *RingBuffer) grow(target int) {
	var newCap int
	if n := r.cap; n == 0 {
		if target <= EmptyBufferSize {
			newCap = EmptyBufferSize
		} else {
			newCap = ceilPowerOfTwo(target)
		}
	} else {
		double := n << 1
		if double >= target {
			newCap = double
		} else {
			if target >= MaxBufferSize {
				panic("target cap too large")
			}
			for n < MaxBufferSize && n < target {
				n = n + n>>1
			}
			if n > MaxBufferSize {
				n = MaxBufferSize
			}
			newCap = n
		}
	}
	slice := getSlice(newCap)
	r.transfer(slice, newCap)
}

// transfer 数据转移，将原来buffer的数据转移到 newSlice 中
func (r *RingBuffer) transfer(newSlice []byte, newSize int) {
	old, oldSize := r.buf, r.cap
	n := r.length
	r.buf, r.cap = newSlice, newSize

	// 如果原来buffer为空，将r和w都改为0
	if n == 0 {
		r.rIdx = 0
		r.wIdx = 0
		return
	}
	// 没有出现环形，wIdx在rIdx之后，将这个范围内的数据拷贝到新的buffer
	if r.rIdx < r.wIdx {
		copy(r.buf, old[r.rIdx:r.wIdx])
		r.wIdx = r.wIdx - r.rIdx
		r.rIdx = 0
	} else {
		// 出现环形，先拷贝rIdx到oldSize，再拷贝0到wIdx
		t := oldSize - r.rIdx
		copy(r.buf, old[r.rIdx:])
		copy(r.buf[t:], old[0:r.wIdx])
		r.wIdx = n
		r.rIdx = 0
	}
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

// ceilPowerOfTwo 将给定的size规范化到2的幂次
func ceilPowerOfTwo(target int) int {
	ceil := 2
	for ceil < target {
		ceil = ceil << 1
	}
	return ceil
}

func (r *RingBuffer) Available() int {
	if r.wIdx == r.rIdx {
		return 0
	} else if r.wIdx > r.rIdx {
		return r.cap - r.wIdx
	} else {
		return r.rIdx - r.wIdx
	}
}
