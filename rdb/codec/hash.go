package codec

import (
	"fmt"
	"log"
	"redigo/datastruct/dict"
)

func (enc *Encoder) WriteHashObject(key string, hash dict.Dict) error {
	// write type byte
	err := enc.Write([]byte{HashType})
	if err != nil {
		log.Println("RDB write hash type prefix error: ", err)
		return err
	}
	// write key
	err = enc.writeString(key)
	if err != nil {
		log.Println("RDB write hash key error: ", err)
		return err
	}
	// write hash size
	err = enc.writeLength(uint64(hash.Len()))
	if err != nil {
		log.Println("RDB write hash length error: ", err)
	}
	// write each key-value pairs
	hash.ForEach(func(hKey string, value interface{}) bool {
		hErr := enc.writeString(hKey)
		if hErr != nil {
			log.Println("RDB write hash key-value pair error: ", hErr)
			return true
		}
		hErr = enc.writeString(string(value.([]byte)))
		if hErr != nil {
			log.Println("RDB write hash key-value pair error: ", hErr)
		}
		return true
	})
	return nil
}

func (dec *Decoder) ReadHash() (string, dict.Dict, error) {
	keyBytes, err := dec.readString()
	if err != nil {
		return "", nil, err
	}
	key := string(keyBytes)
	length, special, err := dec.readLength()
	if err != nil {
		return key, nil, err
	}
	if special {
		return "", nil, fmt.Errorf("wrong length bytes for hash structure")
	}
	hash := dict.NewSimpleDict()
	var i uint64
	for i = 0; i < length; i++ {
		hKeyBytes, err := dec.readString()
		if err != nil {
			continue
		}
		hValue, err := dec.readString()
		if err != nil {
			continue
		}
		hash.Put(string(hKeyBytes), hValue)
	}
	return key, hash, nil
}
