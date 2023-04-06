package str

import (
	"reflect"
	"unsafe"
)

func BytesToString(bytes []byte) string {
	b := (*reflect.SliceHeader)(unsafe.Pointer(&bytes))
	s0 := reflect.StringHeader{
		Data: b.Data,
		Len:  b.Len,
	}
	return *(*string)(unsafe.Pointer(&s0))
}

func StringToBytes(s string) []byte {
	s0 := (*reflect.StringHeader)(unsafe.Pointer(&s))
	sh := reflect.SliceHeader{
		Data: s0.Data,
		Len:  s0.Len,
		Cap:  s0.Len,
	}
	return *(*[]byte)(unsafe.Pointer(&sh))
}
