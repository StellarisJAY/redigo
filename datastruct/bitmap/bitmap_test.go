package bitmap

import (
	"testing"
	"time"
)

func TestBitMap_SetBit(t *testing.T) {
	bitMap := New()
	startTime := time.Now()
	var i int64
	for i = 0; i < 1000000; i++ {
		bitMap.SetBit(i, 1)
	}
	if ori := bitMap.SetBit(0, 0); ori != 1 {
		t.Fail()
	}
	t.Log("BitMap Set time used: ", time.Since(startTime).Milliseconds(), "ms")
}

func TestBitMap_SetBit2(t *testing.T) {
	bitMap := New()
	var i int64
	startTime := time.Now()
	for i = 0; i < 1000000; i++ {
		bitMap.SetBit(i, 1)
	}
	t.Log("BitMap Set time used: ", time.Since(startTime).Milliseconds(), "ms")
	startTime = time.Now()
	for i = 0; i < 1000000; i++ {
		if bit := bitMap.GetBit(i); bit == 0 {
			t.Fail()
		}
	}
	t.Log("BitMap Get time used: ", time.Since(startTime).Milliseconds(), "ms")
}
