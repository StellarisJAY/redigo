package rdb

import (
	"bytes"
	"redigo/datastruct/dict"
	"redigo/datastruct/list"
	"redigo/datastruct/set"
	"redigo/datastruct/zset"
	"redigo/interface/database"
	"redigo/rdb/codec"
)

// SerializeEntry to RDB byte stream
func SerializeEntry(key string, entry *database.Entry) ([]byte, error) {
	switch entry.Data.(type) {
	case []byte:
		return serializeString(key, entry.Data)
	case *list.LinkedList:
		return serializeList(key, entry.Data)
	case dict.Dict:
		return serializeHash(key, entry.Data)
	case *set.Set:
		return serializeSet(key, entry.Data)
	case *zset.SortedSet:
		return serializeSortedSet(key, entry.Data)
	}
	return nil, nil
}

func serializeString(key string, value interface{}) ([]byte, error) {
	result := make([]byte, 0)
	buffer := bytes.NewBuffer(result)
	encoder := codec.NewEncoder(buffer)
	err := encoder.WriteStringObject(key, value.([]byte))
	return buffer.Bytes(), err
}

func serializeHash(key string, value interface{}) ([]byte, error) {
	result := make([]byte, 0)
	buffer := bytes.NewBuffer(result)
	encoder := codec.NewEncoder(buffer)
	err := encoder.WriteHashObject(key, value.(dict.Dict))
	return buffer.Bytes(), err
}

func serializeList(key string, value interface{}) ([]byte, error) {
	result := make([]byte, 0)
	buffer := bytes.NewBuffer(result)
	encoder := codec.NewEncoder(buffer)
	err := encoder.WriteListObject(key, value.(*list.LinkedList))
	return buffer.Bytes(), err
}

func serializeSortedSet(key string, value interface{}) ([]byte, error) {
	result := make([]byte, 0)
	buffer := bytes.NewBuffer(result)
	encoder := codec.NewEncoder(buffer)
	err := encoder.WriteZSetObject(key, value.(*zset.SortedSet))
	return buffer.Bytes(), err
}

func serializeSet(key string, value interface{}) ([]byte, error) {
	result := make([]byte, 0)
	buffer := bytes.NewBuffer(result)
	encoder := codec.NewEncoder(buffer)
	err := encoder.WriteSetObject(key, value.(*set.Set))
	return buffer.Bytes(), err
}
