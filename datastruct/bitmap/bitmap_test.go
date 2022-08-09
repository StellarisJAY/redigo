package bitmap

import (
	"fmt"
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

func TestBitMap_BitCount(t *testing.T) {
	var b1 byte = 0x7f // 0111 1111

	fmt.Println(bitCount(b1, 0, 1))
	fmt.Println(bitCount(b1, 0, 2))
	fmt.Println(bitCount(b1, 2, 4))
	fmt.Println(bitCount(b1, 0, 7))
	fmt.Println(bitCount(b1, 2, 7))
	fmt.Println(bitCount(b1, 4, 7))
}

func TestBitMap_BitCount2(t *testing.T) {
	bitMap1 := New()
	var i int64
	for i = 0; i < 16; i++ {
		bitMap1.SetBit(i, 1)
	}
	fmt.Println(bitMap1.BitCount(0, 7))
	fmt.Println(bitMap1.BitCount(0, -1))
	fmt.Println(bitMap1.BitCount(1, -1))
	fmt.Println(bitMap1.BitCount(2, -1))
	fmt.Println(bitMap1.BitCount(3, -1))
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
