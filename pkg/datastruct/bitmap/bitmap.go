package bitmap

import "fmt"

type BitMap []byte

func New() *BitMap {
	b := BitMap(make([]byte, 0))
	return &b
}

// 每个slot 8bits
func getSlot(offset int64) int64 {
	return offset / 8
}

// 扩容bitmap
func (b *BitMap) grow(size int64) {
	i := size - int64(len(*b))
	if i <= 0 {
		return
	}
	*b = append(*b, make([]byte, i)...)
}

func (b *BitMap) SetBit(offset int64, bit byte) byte {
	slot := getSlot(offset)
	offset0 := offset % 8
	b.grow(slot + 1)
	original := (*b)[slot] >> offset0 & 0x01
	switch bit {
	case 1:
		var mask byte = 1 << offset0
		(*b)[slot] = (*b)[slot] | mask
	case 0:
		var mask byte = ^(1 << offset0)
		(*b)[slot] = (*b)[slot] & mask
	default:
		panic(fmt.Errorf("bit can only be 1 or 0"))
	}
	return original
}

func (b *BitMap) GetBit(offset int64) byte {
	slot := getSlot(offset)
	if slot >= int64(len(*b)) {
		return 0
	}
	offset0 := offset % 8
	return ((*b)[slot] >> offset0) & 0x01
}

func (b *BitMap) BitCount(start, end int64) int64 {
	length := int64(len(*b) * 8)
	if start < 0 {
		start = length + start
	}
	if end < 0 {
		end = length + end
	}
	if end >= length {
		end = length - 1
	}
	if end < start {
		return 0
	}
	startSlot := start / 8
	startOff := int(start % 8)
	endSlot := end / 8
	endOff := int(end % 8)
	var count int64 = 0
	if startSlot == endSlot {
		return int64(bitCount((*b)[startSlot], startOff, endOff))
	}
	count += int64(bitCount((*b)[startSlot], startOff, 7))
	count += int64(bitCount((*b)[endSlot], 0, endOff))
	for i := startSlot + 1; i < endSlot; i++ {
		count += int64(bitCount((*b)[i], 0, 0))
	}

	return count
}

func bitCount(slot byte, offset0, offset1 int) int {
	n := (slot >> offset0) & (1<<(offset1-offset0+1) - 1)
	count := 0
	for n > 0 {
		n = n & (n - 1)
		count++
	}
	return count
}

