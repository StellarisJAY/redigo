package codec

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	EOF          = byte(0xff)
	SelectDB     = byte(0xfe)
	ExpireTime   = byte(0xfd)
	ExpireTimeMs = byte(0xfc)
	ReSizeDB     = byte(0xfb)
	AUX          = byte(0xfa)

	StringType    = byte(0x00)
	ListType      = byte(0x01)
	SetType       = byte(0x02)
	SortedSetType = byte(0x03)
	HashType      = byte(0x04)
)

var (
	MagicNum = []byte{52, 45, 44, 49, 53}
	Version  = []byte("0007")
)

type Decoder struct {
	reader *bufio.Reader
}

func NewDecoder(reader *bufio.Reader) *Decoder {
	return &Decoder{reader: reader}
}

func (dec *Decoder) Read(buf []byte) error {
	_, err := io.ReadFull(dec.reader, buf)
	return err
}

// read an int length value from reader
func (dec *Decoder) readLength() (uint64, bool, error) {
	// first byte is used to determine what kind of integer follows
	firstByte, err := dec.ReadByte()
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
		secByte, err := dec.ReadByte()
		if err != nil {
			return 0, false, fmt.Errorf("rdb read uint14 error %v", err)
		}
		// 01****** & 00111111 << 8
		length = uint64(firstByte&0x3f)<<8 | uint64(secByte)
	case 2:
		// 10 + uint32
		buf := make([]byte, 4)
		err := dec.Read(buf)
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

func (dec *Decoder) ReadByte() (byte, error) {
	return dec.reader.ReadByte()
}

// ReadTTL TTL time
func (dec *Decoder) ReadTTL() (uint64, error) {
	buf := make([]byte, 8)
	err := dec.Read(buf)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(buf), nil
}

// ReadDBIndex database index
func (dec *Decoder) ReadDBIndex() (int, error) {
	length, special, err := dec.readLength()
	if err != nil {
		return 0, err
	}
	if special {
		return 0, errors.New("RDB read db index error: wrong type")
	}
	return int(length), nil
}

// ReadDBSize database size and ttl keys size
func (dec *Decoder) ReadDBSize() (uint64, uint64, error) {
	size, special, err := dec.readLength()
	if err != nil {
		return 0, 0, err
	}
	if special {
		return 0, 0, errors.New("RDB read db index error: wrong type")
	}
	ttlSize, special, err := dec.readLength()
	if err != nil {
		return 0, 0, err
	}
	if special {
		return 0, 0, errors.New("RDB read db index error: wrong type")
	}
	return size, ttlSize, nil
}
