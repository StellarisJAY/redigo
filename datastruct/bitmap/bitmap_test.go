package bitmap

import (
	"testing"
)

func TestBitMap_SetBit(t *testing.T) {
	bitMap := New()
	var i int64
	for i = 0; i < 1000; i++ {
		bitMap.SetBit(i, 1)
	}
	if ori := bitMap.SetBit(0, 0); ori != 1 {
		t.Fail()
	}
}

func TestBitMap_SetBit2(t *testing.T) {
	bitMap := New()
	var i int64
	for i = 0; i < 1000; i++ {
		bitMap.SetBit(i, 1)
	}
	for i = 0; i < 1000; i++ {
		if bit := bitMap.GetBit(i); bit == 0 {
			t.Fail()
		}
	}
}

func BenchmarkBitMap_GetBit(b *testing.B) {
	bitMap := New()
	for i := 0; i < b.N; i++ {
		bitMap.SetBit(int64(i), 1)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = bitMap.GetBit(int64(i))
	}
}

func BenchmarkBitMap_SetBit(b *testing.B) {
	bitMap := New()
	for i := 0; i < b.N; i++ {
		bitMap.SetBit(int64(i), 1)
	}
}
