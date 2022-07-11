package codec

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
)

type Encoder struct {
	writer io.Writer
	crc    hash.Hash64
}

func NewEncoder(writer io.Writer) *Encoder {
	crcTab := crc64.MakeTable(crc64.ISO)
	return &Encoder{writer: writer, crc: crc64.New(crcTab)}
}

func (enc *Encoder) Write(data []byte) error {
	// write data to encoder's writer
	_, err := enc.writer.Write(data)
	if err != nil {
		return fmt.Errorf("write rdb failed %v", err)
	}
	if enc.crc != nil {
		// write data to crc
		_, err = enc.crc.Write(data)
		if err != nil {
			return fmt.Errorf("rdb write crc failed %v", err)
		}
	}
	return nil
}

func (enc *Encoder) writeLength(length uint64) error {
	var buf []byte
	if length <= maxUint6 {
		// write 00 + uint6(length)
		buf = []byte{byte(length)}
	} else if length <= maxUint14 {
		buf = make([]byte, 2)
		// 01 + uint14(length)
		buf[0] = 0b01000000 | byte(length>>8)
		buf[1] = byte(length)
	} else if length <= maxUint32 {
		buf = make([]byte, 5)
		buf[0] = 1 << 7
		binary.BigEndian.PutUint32(buf[1:], uint32(length))
	} else {
		buf = make([]byte, 9)
		buf[0] = 0x81
		binary.BigEndian.PutUint64(buf[1:], length)
	}
	return enc.Write(buf)
}
