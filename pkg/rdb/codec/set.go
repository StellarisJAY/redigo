package codec

import (
	"errors"
	"log"
	"redigo/pkg/datastruct/set"
)

func (enc *Encoder) WriteSetObject(key string, s *set.Set) error {
	err := enc.Write([]byte{SetType})
	if err != nil {
		log.Println("RDB write set type bytes error: ", err)
		return err
	}
	// write list key
	err = enc.writeString(key)
	if err != nil {
		log.Println("RDB write set key error: ", err)
	}
	// write list length
	err = enc.writeLength(uint64(s.Len()))
	if err != nil {
		log.Println("RDB write set length error: ", err)
	}
	// write all list elements
	s.ForEach(func(member string) bool {
		fErr := enc.writeString(member)
		if fErr != nil {
			log.Println("RDB write set element error: ", err)
		}
		return true
	})
	return nil
}

func (dec *Decoder) ReadSetObject() (string, *set.Set, error) {
	keyBytes, err := dec.readString()
	if err != nil {
		log.Println("RDB read set key error: ", err)
		return "", nil, err
	}
	key := string(keyBytes)
	length, special, err := dec.readLength()
	if err != nil {
		log.Println("RDB read set length error: ", err)
		return "", nil, err
	}
	if special {
		err = errors.New("wrong length bytes")
		log.Println("RDB read set length error: ", err)
		return "", nil, err
	}
	s := set.NewSet()
	var i uint64
	for i = 0; i < length; i++ {
		element, err := dec.readString()
		if err != nil {
			continue
		}
		s.Add(string(element))
	}
	return key, s, nil
}
