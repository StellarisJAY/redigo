package codec

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
)

const (
	maxUint6  = 1<<6 - 1
	maxUint14 = 1<<14 - 1
	maxUint32 = 1<<32 - 1
)

func (enc *Encoder) WriteStringObject(key string, value []byte) error {
	// write type prefix
	err := enc.Write([]byte{StringType})
	if err != nil {
		return err
	}
	// write key
	err = enc.writeString(key)
	if err != nil {
		return err
	}
	// write value
	return enc.writeString(string(value))
}

func (dec *Decoder) ReadStringObject() (key string, value []byte, err error) {
	// read key
	keyBytes, err := dec.readString()
	if err != nil {
		return "", nil, err
	}
	key = string(keyBytes)
	// read value
	value, err = dec.readString()
	return
}

func (dec *Decoder) readString() ([]byte, error) {
	// read length prefix
	length, special, err := dec.readLength()
	if err != nil {
		return nil, err
	}
	var result uint64
	// this string is number format
	if special {
		switch length {
		case 0xc1:
			// read uint8
			readBytes, err := dec.reader.ReadByte()
			if err != nil {
				return nil, err
			}
			result = uint64(readBytes)
		case 0xc2:
			// read uint16
			buf := make([]byte, 2)
			_, err = dec.reader.Read(buf)
			if err != nil {
				return nil, err
			}
			result = binary.BigEndian.Uint64(buf)
		case 0xc3:
			// read uint32
			buf := make([]byte, 4)
			_, err = dec.reader.Read(buf)
			if err != nil {
				return nil, err
			}
			result = binary.BigEndian.Uint64(buf)
		}
		// ItoA, get string
		return []byte(strconv.FormatInt(int64(result), 10)), nil
	} else {
		// read normal string
		buf := make([]byte, length)
		_, err = dec.reader.Read(buf)
		if err != nil {
			return nil, err
		}
		return buf, nil
	}
}

func (enc *Encoder) writeString(value string) error {
	// check whether value is an int, try encoding in integer way
	isInt, err := enc.tryWriteAsInt(value)
	if err != nil {
		return fmt.Errorf("rdb write string as int error %v", err)
	}
	if isInt {
		return nil
	}
	// write string length
	err = enc.writeLength(uint64(len(value)))
	if err != nil {
		return err
	}
	// write string value
	err = enc.Write([]byte(value))
	if err != nil {
		return fmt.Errorf("rdb write string value error %v", err)
	}
	return nil
}

// Encode string as a int value
func (enc *Encoder) tryWriteAsInt(value string) (bool, error) {
	// parse int, check if string is int value
	num, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return false, nil
	}
	var buf []byte
	// 11 + 01 + 8bits int
	if num >= math.MinInt8 && num <= math.MaxInt8 {
		buf = make([]byte, 2)
		buf[0] = 0b11000001
		buf[1] = byte(num)
	} else if num >= math.MinInt16 && num <= math.MaxInt16 {
		// 11 + 10 + 16bits int
		buf = make([]byte, 3)
		buf[0] = 0b11000010
		binary.BigEndian.PutUint16(buf[1:], uint16(int16(num)))
	} else if num >= math.MinInt32 && num <= math.MaxInt32 {
		// 11 + 11 + 32bits int
		buf = make([]byte, 5)
		buf[0] = 0b11000011
		binary.BigEndian.PutUint32(buf[1:], uint32(int32(num)))
	}
	err = enc.Write(buf)
	if err != nil {
		return true, err
	}
	return true, nil
}
