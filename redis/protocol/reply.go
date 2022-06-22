package protocol

import (
	"strconv"
	"strings"
)

var (
	OKReply  = &Reply{singleStr: "OK"}
	NilReply = &Reply{singleStr: "(nil)"}
)

type Reply struct {
	err             error
	number          int
	bulkStringArray [][]byte
	singleStr       string
}

func NewNumberReply(number int) *Reply {
	return &Reply{number: number}
}

func NewBulkValueReply(value []byte) *Reply {
	arr := make([][]byte, 0)
	arr = append(arr, value)
	return &Reply{
		err:             nil,
		number:          -1,
		bulkStringArray: arr,
		singleStr:       "",
	}
}

// NewBulkStringReply returns a reply with multi-lined Bulk String
func NewBulkStringReply(value string) *Reply {
	arr := make([][]byte, 0)
	arr = append(arr, []byte(value))
	return &Reply{
		err:             nil,
		number:          -1,
		bulkStringArray: arr,
		singleStr:       "",
	}
}

// NewSingleStringReply returns a reply with Single-lined string
func NewSingleStringReply(value string) *Reply {
	return &Reply{
		err:             nil,
		number:          -1,
		bulkStringArray: nil,
		singleStr:       value,
	}
}

func NewArrayReply(arr [][]byte) *Reply {
	return &Reply{
		err:             nil,
		number:          -1,
		bulkStringArray: arr,
		singleStr:       "",
	}
}

func NewStringArrayReply(arr []string) *Reply {
	bulkArr := make([][]byte, len(arr))
	for i, str := range arr {
		bulkArr[i] = []byte(str)
	}
	return &Reply{
		err:             nil,
		number:          -1,
		bulkStringArray: bulkArr,
		singleStr:       "",
	}
}

func NewErrorReply(err error) *Reply {
	return &Reply{
		err:             err,
		number:          -1,
		bulkStringArray: nil,
		singleStr:       "",
	}
}

/*
	Format Reply to RESP bytes
*/
func (r *Reply) ToBytes() []byte {
	if r.singleStr != "" {
		return []byte("+" + r.singleStr + CRLF)
	} else if r.bulkStringArray != nil {
		if len(r.bulkStringArray) == 1 {
			return []byte("+" + string(r.bulkStringArray[0]) + CRLF)
		} else {
			builder := strings.Builder{}
			// * length
			builder.WriteString("*" + strconv.Itoa(len(r.bulkStringArray)) + CRLF)
			for _, bulk := range r.bulkStringArray {
				// Write $(len)\r\n{string}\r\n
				builder.WriteString("$" + strconv.Itoa(len(bulk)) + CRLF)
				builder.Write(bulk)
				builder.WriteString(CRLF)
			}
			return []byte(builder.String())
		}
	} else if r.err != nil {
		return []byte("-" + r.err.Error() + CRLF)
	} else {
		return []byte(":" + strconv.Itoa(r.number) + CRLF)
	}
}

func CreateNumberReply(number int) []byte {
	return []byte(":" + strconv.Itoa(number) + CRLF)
}

func CreateSingleStringReply(value string) []byte {
	return []byte("+" + value + CRLF)
}

func CreateBulkStringArrayReply(array []string) []byte {
	builder := strings.Builder{}
	builder.WriteString("*" + strconv.Itoa(len(array)) + CRLF)
	for _, bulk := range array {
		builder.WriteString("$" + strconv.Itoa(len(bulk)) + CRLF + bulk + CRLF)
	}
	return []byte(builder.String())
}
