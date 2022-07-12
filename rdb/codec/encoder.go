package codec

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"redigo/datastruct/dict"
	"redigo/datastruct/list"
	"redigo/datastruct/set"
	"redigo/datastruct/zset"
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
		buf[0] = 0x40 | byte(length>>8)
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

func (enc *Encoder) WriteKeyValue(key string, value interface{}) error {
	switch value.(type) {
	case []byte:
		return enc.WriteStringObject(key, value.([]byte))
	case *list.LinkedList:
		return enc.WriteListObject(key, value.(*list.LinkedList))
	case dict.Dict:
		return enc.WriteHashObject(key, value.(dict.Dict))
	case *set.Set:
		return enc.WriteSetObject(key, value.(*set.Set))
	case *zset.SortedSet:
		return enc.WriteZSetObject(key, value.(*zset.SortedSet))
	}
	return fmt.Errorf("unknown data structure error")
}

// WriteTTL write expire time
func (enc *Encoder) WriteTTL(expireAt uint64) error {
	buf := make([]byte, 9)
	// prefix byte 0xfc
	buf[0] = ExpireTimeMs
	binary.BigEndian.PutUint64(buf[1:], expireAt)
	return enc.Write(buf)
}

// WriteDBIndex select DB index
func (enc *Encoder) WriteDBIndex(index uint64) error {
	// prefix byte 0xfe
	err := enc.Write([]byte{SelectDB})
	if err != nil {
		return err
	}
	return enc.writeLength(index)
}

// WriteDBSize write number of keys
func (enc *Encoder) WriteDBSize(size, ttls uint64) error {
	// prefix byte 0xfb
	err := enc.Write([]byte{ReSizeDB})
	if err != nil {
		return err
	}
	// write db size
	err = enc.writeLength(size)
	if err != nil {
		return err
	}
	// write ttl count
	return enc.writeLength(ttls)
}
