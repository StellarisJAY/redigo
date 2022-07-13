package bitmap

import "fmt"

// BitMap data structure is the same with normal byte slice
type BitMap []byte

func New() *BitMap {
	b := BitMap(make([]byte, 0))
	return &b
}

// each slot contains 8 bits, this function tells the target offset's slot
func getSlot(offset int64) int64 {
	return offset / 8
}

// if offset is greater than bitMap's slice size, expand slice
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
	mask := bit << offset0
	original := (*b)[slot] >> offset0 & 0x01
	(*b)[slot] = (*b)[slot] | mask
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

func (b *BitMap) printBits() {
	for _, bit := range *b {
		fmt.Println(bit)
	}
}
