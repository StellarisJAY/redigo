package protocol

import (
	"strconv"
	"strings"
)

const (
	CommandTypeSingleLine byte = iota
	CommandTypeBulk
	CommandTypeArray
	CommandTypeNumber
	CommandTypeError
	ReplyTypeNil
	ReplyEmptyList
)

var (
	OKReply        = NewSingleStringReply("OK")
	NilReply       = &Reply{commandType: ReplyTypeNil}
	EmptyListReply = &Reply{commandType: ReplyEmptyList}
	QueuedReply    = NewSingleStringReply("QUEUED")
)

type Reply struct {
	err         error
	number      int
	stringArray [][]byte
	nested      bool
	commandType byte
}

func NewNumberReply(number int) *Reply {
	return &Reply{number: number, commandType: CommandTypeNumber}
}

func NewBulkValueReply(value []byte) *Reply {
	return &Reply{
		commandType: CommandTypeBulk,
		stringArray: [][]byte{value},
	}
}

// NewBulkStringReply returns a reply with multi-lined Bulk String
func NewBulkStringReply(value string) *Reply {
	return &Reply{
		stringArray: [][]byte{[]byte(value)},
		commandType: CommandTypeBulk,
	}
}

// NewSingleStringReply returns a reply with Single-lined string
func NewSingleStringReply(value string) *Reply {
	return &Reply{
		stringArray: [][]byte{[]byte(value)},
		commandType: CommandTypeSingleLine,
	}
}

func NewArrayReply(arr [][]byte) *Reply {
	return &Reply{
		stringArray: arr,
		commandType: CommandTypeArray,
	}
}

func NewNestedArrayReply(arr [][]byte) *Reply {
	return &Reply{
		stringArray: arr,
		nested:      true,
		commandType: CommandTypeArray,
	}
}

func NewStringArrayReply(arr []string) *Reply {
	bulkArr := make([][]byte, len(arr))
	for i, str := range arr {
		bulkArr[i] = []byte(str)
	}
	return &Reply{
		stringArray: bulkArr,
		commandType: CommandTypeArray,
	}
}

func NewErrorReply(err error) *Reply {
	return &Reply{
		err:         err,
		commandType: CommandTypeError,
	}
}

/*
	Format Reply to RESP bytes
*/
func (r *Reply) ToBytes() []byte {
	switch r.commandType {
	case CommandTypeNumber:
		return []byte(":" + strconv.Itoa(r.number) + CRLF)
	case CommandTypeSingleLine:
		return []byte("+" + string(r.stringArray[0]) + CRLF)
	case CommandTypeError:
		return []byte("-" + r.err.Error() + CRLF)
	case CommandTypeBulk:
		return []byte("$" + strconv.Itoa(len(r.stringArray[0])) + CRLF + string(r.stringArray[0]) + CRLF)
	case ReplyTypeNil:
		return []byte("$-1\r\n")
	case ReplyEmptyList:
		return []byte("*0\r\n")
	case CommandTypeArray:
		builder := strings.Builder{}
		// * length
		builder.WriteString("*" + strconv.Itoa(len(r.stringArray)) + CRLF)

		for _, bulk := range r.stringArray {
			if r.nested {
				builder.Write(bulk)
			} else {
				if bulk == nil {
					builder.WriteString("$-1" + CRLF)
					continue
				}
				// Write $(len)\r\n{string}\r\n
				builder.WriteString("$" + strconv.Itoa(len(bulk)) + CRLF)
				builder.Write(bulk)
				builder.WriteString(CRLF)
			}
		}
		return []byte(builder.String())
	}
	return nil
}
