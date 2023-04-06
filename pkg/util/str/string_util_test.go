package str

import (
	bytes2 "bytes"
	"testing"
)

var bytes []byte
var s string

func init() {
	for i := 0; i < 16; i++ {
		bytes = append(bytes, 'a')
	}
	s = string(bytes)
}

func TestStringToBytes(t *testing.T) {
	s := "+" + "OK" + "\r\n"
	bytes := StringToBytes(s)
	b0 := []byte(s)
	if !bytes2.Equal(bytes, b0) {
		t.Fail()
	}
}

func BenchmarkBytesToStringOld(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = string(bytes)
	}
}

func BenchmarkBytesToStringNew(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = BytesToString(bytes)
	}
}

func BenchmarkStringToBytesOld(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = []byte(s)
	}
}

func BenchmarkStringToBytesNew(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = StringToBytes(s)
	}
}
