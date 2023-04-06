package codec

import (
	"encoding/binary"
	"errors"
	"log"
	"math"
	"redigo/pkg/datastruct/zset"
)

func (enc *Encoder) WriteZSetObject(key string, zs *zset.SortedSet) error {
	err := enc.Write([]byte{SortedSetType})
	if err != nil {
		log.Println("RDB write zset type bytes error: ", err)
		return err
	}
	// write list key
	err = enc.writeString(key)
	if err != nil {
		log.Println("RDB write zset key error: ", err)
	}
	// write list length
	err = enc.writeLength(uint64(zs.Size()))
	if err != nil {
		log.Println("RDB write zset length error: ", err)
	}
	// write all list elements
	zs.ForEach(func(score float64, value string) bool {
		fErr := enc.writeString(value)
		if fErr != nil {
			log.Println("RDB write zset member error: ", err)
			return false
		}
		fErr = enc.writeFloat64(score)
		if fErr != nil {
			log.Println("RDB write zset score error: ", err)
			return false
		}
		return true
	})
	return nil
}

func (dec *Decoder) ReadZSetObject() (string, *zset.SortedSet, error) {
	keyBytes, err := dec.readString()
	if err != nil {
		log.Println("RDB read zset key error: ", err)
		return "", nil, err
	}
	key := string(keyBytes)
	length, special, err := dec.readLength()
	if err != nil {
		log.Println("RDB read zset length error: ", err)
		return "", nil, err
	}
	if special {
		err = errors.New("wrong length bytes")
		log.Println("RDB read zset length error: ", err)
		return "", nil, err
	}
	zs := zset.NewSortedSet()
	var i uint64
	for i = 0; i < length; i++ {
		val, err := dec.readString()
		if err != nil {
			return key, nil, err
		}
		buf := make([]byte, 8)
		err = dec.Read(buf)
		if err != nil {
			return key, nil, err
		}
		bits := binary.BigEndian.Uint64(buf)
		score := math.Float64frombits(bits)
		zs.Add(string(val), score)
	}
	return key, zs, nil
}

func (enc *Encoder) writeFloat64(value float64) error {
	bits := math.Float64bits(value)
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, bits)
	return enc.Write(buf)
}
