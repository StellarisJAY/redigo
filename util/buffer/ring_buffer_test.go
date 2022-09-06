package buffer

import (
	"testing"
)

func TestRingBuffer_Write(t *testing.T) {
	buffer := NewRingBuffer(8)
	if buffer.cap != 8 {
		t.FailNow()
	}
	_, _ = buffer.Write([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	t.Logf("w: %d r: %d len: %d cap: %d", buffer.wIdx, buffer.rIdx, buffer.length, buffer.cap)
	if buffer.wIdx != 0 || buffer.cap != 8 {
		t.FailNow()
	}
	_, _ = buffer.Write([]byte{9, 10, 11, 12, 13, 14, 15, 16, 17})
	t.Logf("w: %d r: %d len: %d cap: %d", buffer.wIdx, buffer.rIdx, buffer.length, buffer.cap)
	if buffer.wIdx != 17 || buffer.cap != 32 {
		t.FailNow()
	}
}

func TestRingBuffer_ReadUntil(t *testing.T) {
	buffer := NewRingBuffer(0)
	_ = buffer.WriteString("*8\r\n")
	bytes, err := buffer.ReadUntil('\n')
	if err != nil {
		t.FailNow()
	}
	if len(bytes) != 4 {
		t.FailNow()
	}
	t.Log(bytes)
}
