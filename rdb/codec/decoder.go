package codec

import (
	"bufio"
	"encoding/binary"
	"fmt"
)

const (
	StringType    = byte(0x00)
	ListType      = byte(0x01)
	SetType       = byte(0x02)
	SortedSetType = byte(0x03)
	HashType      = byte(0x04)
)

type Decoder struct {
	reader *bufio.Reader
}

func NewDecoder(reader *bufio.Reader) *Decoder {
	return &Decoder{reader: reader}
}

// read an int length value from reader
func (dec *Decoder) readLength() (uint64, bool, error) {
	// first byte is used to determine what kind of integer follows
	firstByte, err := dec.reader.ReadByte()
	if err != nil {
		return 0, false, fmt.Errorf("rdb read length error %v", err)
	}
	lenType := firstByte >> 6
	var length uint64
	special := false
	switch lenType {
	case 0:
		// 00 + uint6
		length = uint64(firstByte)
	case 1:
		// 01 + uint14
		secByte, err := dec.reader.ReadByte()
		if err != nil {
			return 0, false, fmt.Errorf("rdb read uint14 error %v", err)
		}
		// 01****** & 00111111 << 8
		length = uint64(firstByte&0x3f)<<8 | uint64(secByte)
	case 2:
		// 10 + uint32
		buf := make([]byte, 4)
		_, err := dec.reader.Read(buf)
		if err != nil {
			return 0, false, fmt.Errorf("rdb read uint32 error %v", err)
		}
		length = uint64(binary.BigEndian.Uint32(buf))
	case 3:
		special = true
		length = uint64(firstByte)
	}
	return length, special, nil
}
