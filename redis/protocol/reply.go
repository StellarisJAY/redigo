package protocol

import (
	"strconv"
	"strings"
)

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
