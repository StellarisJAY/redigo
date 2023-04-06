package buffer

import (
	"io"
)

type RingBuffer struct {
	buf        []byte
	cap        int // cap 是ring buffer底层数组的大小
	length     int // length 是元素个数
	rIdx       int
	wIdx       int
	readMark   int
	lengthMark int
}

func NewRingBuffer(cap int) *RingBuffer {
	cap = ceilPowerOfTwo(cap)
	return &RingBuffer{
		buf:        make([]byte, cap),
		cap:        cap,
		readMark:   -1,
		lengthMark: -1,
	}
}

func (r *RingBuffer) Read(bytes []byte) (int, error) {
	if r.length == 0 {
		return 0, io.EOF
	}
	size := len(bytes)
	if r.rIdx < r.wIdx {
		copy(bytes, r.buf[r.rIdx:min(r.rIdx+size, r.wIdx)])
		readLen := min(size, r.wIdx-r.rIdx)
		r.rIdx, r.length = (r.rIdx+readLen)%r.cap, r.length-readLen
		return readLen, nil
	} else {
		if r.rIdx+size <= r.cap {
			copy(bytes, r.buf[r.rIdx:r.rIdx+size])
			r.rIdx, r.length = (r.rIdx+size)%r.cap, r.length-size
			return size, nil
		} else {
			n := r.cap - r.rIdx
			copy(bytes, r.buf[r.rIdx:r.cap])
			copy(bytes[n:], r.buf[0:min(r.wIdx, size-n)])
			readLen := min(size, n+r.wIdx)
			r.rIdx, r.length = (r.rIdx+readLen)%r.cap, r.length-readLen
			return readLen, nil
		}
	}
}

func (r *RingBuffer) ReadBytes(delim byte) ([]byte, error) {
	var bytes []byte
	for {
		if b, err := r.ReadByte(); err != nil {
			return nil, ErrUnexpectedEOF
		} else {
			bytes = append(bytes, b)
			if b == delim {
				break
			}
		}
	}
	return bytes, nil
}

func (r *RingBuffer) Write(bytes []byte) (int, error) {
	size := len(bytes)
	// ensure buffer size
	if err := r.ensureWriteSpace(size); err != nil {
		return 0, ErrBufferOverflow
	}
	if n := r.cap - r.wIdx; size <= n {
		copy(r.buf[r.wIdx:], bytes)
		r.wIdx = (r.wIdx + size) % r.cap
	} else {
		copy(r.buf[r.wIdx:], bytes[:n])
		r.wIdx = (r.wIdx + size) % r.cap
		copy(r.buf[0:r.wIdx], bytes[n:])
	}
	r.length += size
	return size, nil
}

func (r *RingBuffer) ReadIndex() int {
	return r.rIdx
}

func (r *RingBuffer) WriteIndex() int {
	return r.wIdx
}

func (r *RingBuffer) Bytes() []byte {
	return r.buf
}

func (r *RingBuffer) Len() int {
	return r.length
}

func (r *RingBuffer) Cap() int {
	return r.cap
}

func (r *RingBuffer) MarkReadIndex() {
	r.readMark = r.rIdx
}

func (r *RingBuffer) ResetReadIndex() {
	if r.lengthMark != -1 && r.readMark != -1 {
		r.rIdx, r.length = r.readMark, r.lengthMark
	}
}

func (r *RingBuffer) Reset() {
	r.rIdx = 0
	r.wIdx = 0
	r.length = 0
}

func (r *RingBuffer) ReadByte() (byte, error) {
	if r.length == 0 {
		return 0, io.EOF
	}
	b := r.buf[r.rIdx]
	r.rIdx = (r.rIdx + 1) % r.cap
	r.length--
	return b, nil
}

// ensureWriteSpace grows buffer if needed more space for writing
func (r *RingBuffer) ensureWriteSpace(writeSize int) error {
	if r.length+writeSize > r.cap {
		if err := r.grow(r.length + writeSize); err != nil {
			return err
		}
	}
	return nil
}

// grow buffer to target capacity
func (r *RingBuffer) grow(target int) error {
	newCap := ceilPowerOfTwo(target)
	if newCap > MaxBufferSize {
		return ErrBufferSizeLimit
	}
	newBuf := make([]byte, newCap)
	if r.length == 0 {
		r.buf, r.cap = newBuf, newCap
		r.wIdx, r.rIdx = 0, 0
		return nil
	}
	if r.wIdx > r.rIdx {
		copy(newBuf, r.buf[r.rIdx:r.wIdx])
	} else {
		n := r.cap - r.rIdx
		copy(newBuf[0:n], r.buf[r.rIdx:r.cap])
		copy(newBuf[n:], r.buf[0:r.wIdx])
	}
	r.rIdx, r.wIdx = 0, r.length
	r.cap, r.buf = newCap, newBuf
	return nil
}

func (r *RingBuffer) innerSlice() []byte {
	return r.buf
}

// ceilPowerOfTwo 将给定的size规范化到2的幂次
func ceilPowerOfTwo(target int) int {
	ceil := 2
	for ceil < target {
		ceil = ceil << 1
	}
	return ceil
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
