package protocol

import (
	"strconv"
	"strings"
)

var (
	OKReply        = &Reply{singleStr: "OK"}
	NilReply       = &Reply{singleStr: "(nil)"}
	EmptyListReply = &Reply{nilReply: true}
)

type Reply struct {
	err             error
	number          int
	bulkStringArray [][]byte
	singleStr       string
	nilReply        bool
}

func NewNumberReply(number int) *Reply {
	return &Reply{number: number, nilReply: false}
}

func NewBulkValueReply(value []byte) *Reply {
	arr := make([][]byte, 0)
	arr = append(arr, value)
	return &Reply{
		err:             nil,
		number:          -1,
		bulkStringArray: arr,
		singleStr:       "",
		nilReply:        false,
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
		nilReply:        false,
	}
}

// NewSingleStringReply returns a reply with Single-lined string
func NewSingleStringReply(value string) *Reply {
	return &Reply{
		err:             nil,
		number:          -1,
		bulkStringArray: nil,
		singleStr:       value,
		nilReply:        false,
	}
}

func NewArrayReply(arr [][]byte) *Reply {
	return &Reply{
		err:             nil,
		number:          -1,
		bulkStringArray: arr,
		singleStr:       "",
		nilReply:        false,
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
		nilReply:        false,
	}
}

func NewErrorReply(err error) *Reply {
	return &Reply{
		err:             err,
		number:          -1,
		bulkStringArray: nil,
		singleStr:       "",
		nilReply:        false,
	}
}

/*
	Format Reply to RESP bytes
*/
func (r *Reply) ToBytes() []byte {
	if r.nilReply {
		return []byte("$-1" + CRLF)
	}
	if r.singleStr != "" {
		return []byte("+" + r.singleStr + CRLF)
	} else if r.bulkStringArray != nil {
		if len(r.bulkStringArray) == 0 {
			return []byte("*0" + CRLF)
		} else if len(r.bulkStringArray) == 1 {
			return []byte("+" + string(r.bulkStringArray[0]) + CRLF)
		} else {
			builder := strings.Builder{}
			// * length
			builder.WriteString("*" + strconv.Itoa(len(r.bulkStringArray)) + CRLF)
			for _, bulk := range r.bulkStringArray {
				if bulk == nil {
					builder.WriteString("$-1" + CRLF)
					continue
				}
				// Write $(len)\r\n{string}\r\n
				builder.WriteString("$" + strconv.Itoa(len(bulk)) + CRLF)
				builder.Write(bulk)
				builder.WriteString(CRLF)
			}
			r.bulkStringArray = nil
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
