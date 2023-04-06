package rdb

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"redigo/pkg/datastruct/dict"
	"redigo/pkg/datastruct/list"
	"redigo/pkg/datastruct/set"
	"redigo/pkg/datastruct/zset"
	"redigo/pkg/interface/database"
	"redigo/pkg/rdb/codec"
	"testing"
)

func TestSerializeEntry(t *testing.T) {
	entry := &database.Entry{Data: []byte("world")}
	key := "hello"

	serialized, err := SerializeEntry(key, entry)
	if err != nil {
		log.Println(err)
		t.Fail()
	}
	fmt.Println(serialized)
	decoder := codec.NewDecoder(bufio.NewReader(bytes.NewBuffer(serialized[1:])))
	key, value, err := decoder.ReadStringObject()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("key: ", key)
	fmt.Println("value: ", string(value))
}

func TestSerializeHashEntry(t *testing.T) {
	hash := dict.NewSimpleDict()
	m := map[string]string{"xxj": "nice", "hello": "world", "1": "2", "3": "#"}
	for k, v := range m {
		hash.Put(k, []byte(v))
	}
	key := "myhash"
	entry := &database.Entry{Data: hash}

	serialized, err := SerializeEntry(key, entry)
	if err != nil {
		fmt.Println(err)
	}

	decoder := codec.NewDecoder(bufio.NewReader(bytes.NewBuffer(serialized[1:])))
	key, deserialized, err := decoder.ReadHash()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("hash key: ", key)
	fmt.Println("hash length: ", deserialized.Len())
	deserialized.ForEach(func(key string, value interface{}) bool {
		fmt.Printf("\"%s\":\"%s\"\n", key, string(value.([]byte)))
		return true
	})
}

func TestSerializeListEntry(t *testing.T) {
	l := list.NewLinkedList([][]byte{[]byte("1"), []byte("2"), []byte("hello"), []byte("world"), []byte("xxj")}...)

	key := "mylist"
	entry := &database.Entry{Data: l}

	serialized, err := SerializeEntry(key, entry)
	if err != nil {
		fmt.Println(err)
	}

	decoder := codec.NewDecoder(bufio.NewReader(bytes.NewBuffer(serialized[1:])))
	key, deserialized, err := decoder.ReadListObject()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("list key: ", key)
	fmt.Println("list length: ", deserialized.Size())
	deserialized.ForEach(func(idx int, value []byte) bool {
		fmt.Println(string(value))
		return true
	})
}

func TestSerializeSetEntry(t *testing.T) {
	s := set.NewSet()
	m := map[string]string{"xxj": "nice", "hello": "world", "1": "2", "3": "#"}
	for k, _ := range m {
		s.Add(k)
	}
	key := "myset"
	entry := &database.Entry{Data: s}

	serialized, err := SerializeEntry(key, entry)
	if err != nil {
		fmt.Println(err)
	}

	decoder := codec.NewDecoder(bufio.NewReader(bytes.NewBuffer(serialized[1:])))
	key, deserialized, err := decoder.ReadSetObject()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("set key: ", key)
	fmt.Println("set length: ", deserialized.Len())
	deserialized.ForEach(func(member string) bool {
		fmt.Println(member)
		return true
	})
}

func TestSerializeZSetEntry(t *testing.T) {
	zs := zset.NewSortedSet()
	m := map[string]float64{"xxj": 50.5, "hello": 1024.001, "1": 2.01, "3": -3000.01}
	for k, v := range m {
		zs.Add(k, v)
	}
	key := "myzset"
	entry := &database.Entry{Data: zs}

	serialized, err := SerializeEntry(key, entry)
	if err != nil {
		fmt.Println(err)
	}

	decoder := codec.NewDecoder(bufio.NewReader(bytes.NewBuffer(serialized[1:])))
	key, deserialized, err := decoder.ReadZSetObject()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("zset key: ", key)
	fmt.Println("zset length: ", deserialized.Size())
	deserialized.ForEach(func(score float64, value string) bool {
		fmt.Println("member: ", value, ", score: ", score)
		return true
	})
}
