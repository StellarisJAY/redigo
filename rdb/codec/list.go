package codec

import (
	"errors"
	"log"
	"redigo/datastruct/list"
)

func (enc *Encoder) WriteListObject(key string, l *list.LinkedList) error {
	err := enc.Write([]byte{ListType})
	if err != nil {
		log.Println("RDB write list type bytes error: ", err)
		return err
	}
	// write list key
	err = enc.writeString(key)
	if err != nil {
		log.Println("RDB write list key error: ", err)
	}
	// write list length
	err = enc.writeLength(uint64(l.Size()))
	if err != nil {
		log.Println("RDB write list length error: ", err)
	}
	// write all list elements
	l.ForEach(func(idx int, value []byte) bool {
		fErr := enc.writeString(string(value))
		if fErr != nil {
			log.Println("RDB write list element error: ", err)
		}
		return true
	})
	return nil
}

func (dec *Decoder) ReadListObject() (string, *list.LinkedList, error) {
	keyBytes, err := dec.readString()
	if err != nil {
		log.Println("RDB read list key error: ", err)
		return "", nil, err
	}
	key := string(keyBytes)
	length, special, err := dec.readLength()
	if err != nil {
		log.Println("RDB read list length error: ", err)
		return "", nil, err
	}
	if special {
		err = errors.New("wrong length bytes")
		log.Println("RDB read list length error: ", err)
		return "", nil, err
	}
	linkedList := list.NewLinkedList()
	var i uint64
	for i = 0; i < length; i++ {
		element, err := dec.readString()
		if err != nil {
			continue
		}
		linkedList.AddRight(element)
	}
	return key, linkedList, nil
}
