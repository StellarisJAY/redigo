package buffer

import (
	"fmt"
	"io"
	"redigo/redis"
	"testing"
)

type testCase struct {
	name    string
	write   []byte
	read    []byte
	e       expect
	initCap int
}

type expect struct {
	cap     int
	length  int
	rIdx    int
	wIdx    int
	err     error
	written int
	read    int
}

func TestRingBuffer_Write(t *testing.T) {
	testCases := []testCase{
		{name: "grow-buffer", write: []byte("helloword"), initCap: 8, e: expect{cap: 16, err: nil, written: 10}},
		{name: "grow-from-zero", write: []byte("hello"), initCap: 0, e: expect{cap: 8, err: nil, written: 10}},
		{name: "buffer-too-large", write: make([]byte, MaxBufferSize+10), initCap: 2, e: expect{err: ErrBufferOverflow, written: 0, cap: 2}},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			buf := NewRingBuffer(testCase.initCap)
			expect := testCase.e
			if n, err := buf.Write(testCase.write); err != expect.err {
				t.Logf("expect error: %v, got: %v", expect.err, err)
				t.FailNow()
			} else if err == nil && n != len(testCase.write) {
				t.Logf("expect written: %v, got: %v", len(testCase.write), n)
				t.FailNow()
			}
			if buf.Cap() != expect.cap {
				t.Logf("expect cap: %v, got: %v", expect.cap, buf.Cap())
				t.FailNow()
			}
		})
	}
}

func TestRingBuffer_Read(t *testing.T) {
	testCases := []testCase{
		{name: "empty", write: []byte{}, initCap: 8, e: expect{read: 0, err: io.EOF}},
		{name: "enough-to-read", write: []byte("12345678"), read: make([]byte, 6), initCap: 8, e: expect{read: 6, err: nil}},
		{name: "not-enough-to-read", write: []byte("12345678"), read: make([]byte, 10), initCap: 16, e: expect{read: 8, err: nil}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := NewRingBuffer(tc.initCap)
			_, _ = buf.Write(tc.write)
			if n, err := buf.Read(tc.read); err != tc.e.err {
				t.Logf("expect error: %v, got: %v", tc.e.err, err)
				t.FailNow()
			} else if n != tc.e.read {
				t.Logf("expect read len: %d, got: %d", tc.e.read, n)
				t.FailNow()
			}
		})
	}
}

func TestRingBuffer_Read2(t *testing.T) {
	t.Run("ring-enough-read", func(t *testing.T) {

	})

	t.Run("ring-not-enough-read", func(t *testing.T) {

	})
}

func TestRingBuffer_ReadBytes(t *testing.T) {
	buf := NewRingBuffer(16)
	_, _ = buf.Write([]byte("hello\nworld\n"))
	if bytes, err := buf.ReadBytes('\n'); err != nil {
		t.Error(err)
		t.FailNow()
	} else if string(bytes) != "hello\n" {
		t.Logf("expect: %s, got: %s", "hello\n", string(bytes))
	}
}

func TestRingBuffer_ReadBytes2(t *testing.T) {
	buf := NewRingBuffer(16)
	_, _ = buf.Write([]byte("*2\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n"))
	cmd, err := redis.Decode(buf)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	fmt.Println(cmd.Parts())
	_, _ = buf.Write([]byte("*3\r\n$3\r\nset\r\n$2\r\nk1\r\n$1\r\n1\r\n"))
	cmd, err = redis.Decode(buf)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	fmt.Println(cmd.Parts())
	_, _ = buf.Write([]byte("*3\r\n$3\r\nset\r\n$2\r\nk1\r\n$1\r\n1\r\n"))
	cmd, err = redis.Decode(buf)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	fmt.Println(cmd.Parts())
}
