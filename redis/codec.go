package redis

import (
	"bufio"
	"errors"
	"io"
	"redigo/interface/redis"
	"strconv"
	"strings"
)

func Decode(reader *bufio.Reader) (*RespCommand, error) {
	msg, ioErr, err := readLine(reader)
	if ioErr {
		return nil, io.EOF
	}
	if err != nil {
		return nil, err
	}
	var command *RespCommand
	fromCluster := false
	if msg[0] == '!' {
		fromCluster = true
		msg = msg[1:]
	}
	switch msg[0] {
	case redis.SingleLinePrefix:
		command = NewSingleLineCommand(msg[1 : len(msg)-2])
	case redis.NumberPrefix:
		number, err := strconv.Atoi(string(msg[1 : len(msg)-2]))
		if err != nil {
			return nil, HashValueNotIntegerError
		}
		command = NewNumberCommand(number)
	case redis.ErrorPrefix:
		command = NewErrorCommand(errors.New(string(msg[1 : len(msg)-2])))
	case redis.BulkPrefix:
		bulk, err := readBulkString(reader, msg)
		if err != nil {
			return nil, err
		}
		if bulk == nil {
			command = NewNilCommand()
		} else {
			command = NewBulkStringCommand(bulk)
		}
		command.SetFromCluster(fromCluster)
		return command, nil
	case redis.ArrayPrefix:
		// get Array size
		size, err := strconv.Atoi(string(msg[1 : len(msg)-2]))
		if err != nil {
			return nil, err
		}
		if size == 0 {
			command = NewEmptyListCommand()
			command.SetFromCluster(fromCluster)
			return command, nil
		}
		// parse RESP array
		if parts, err := readArray(reader, size); err != nil {
			return nil, err
		} else {
			command = NewCommand(parts)
			command.SetFromCluster(fromCluster)
			return command, nil
		}
	}
	command.SetFromCluster(fromCluster)
	return command, nil
}

func Encode(command *RespCommand) []byte {
	switch command.commandType {
	case CommandTypeNumber:
		return []byte(":" + strconv.Itoa(command.number) + CRLF)
	case CommandTypeSingleLine:
		return []byte("+" + string(command.parts[0]) + CRLF)
	case CommandTypeError:
		return []byte("-" + command.err.Error() + CRLF)
	case CommandTypeBulk:
		return []byte("$" + strconv.Itoa(len(command.parts[0])) + CRLF + string(command.parts[0]) + CRLF)
	case ReplyTypeNil:
		return []byte("$-1\r\n")
	case ReplyEmptyList:
		return []byte("*0\r\n")
	case CommandTypeArray:
		builder := strings.Builder{}
		// * length
		builder.WriteString("*" + strconv.Itoa(len(command.parts)) + CRLF)

		for _, bulk := range command.parts {
			if command.nested {
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

/*
	Read RESP Bulk string
	${len}\r\n{content}\r\n
*/
func readBulkString(reader io.Reader, lengthStr []byte) ([]byte, error) {
	// parse array length
	length, err := strconv.Atoi(string(lengthStr[1 : len(lengthStr)-2]))
	if err != nil {
		return nil, err
	}
	// empty bulk string
	if length == -1 {
		return nil, nil
	}
	// read bulk string buffer, with \r\n
	buffer := make([]byte, length+2)
	_, err = io.ReadFull(reader, buffer)
	if err != nil {
		return nil, err
	}
	return buffer[0:length], nil
}

func readArray(reader *bufio.Reader, size int) ([][]byte, error) {
	parts := make([][]byte, size)
	for i := 0; i < size; i++ {
		// read a line
		msg, ioErr, err := readLine(reader)
		if ioErr {
			return nil, io.EOF
		} else if err != nil {
			return nil, err
		}
		// read RESP Array
		if msg[0] == '$' || msg[0] == ':' {
			bulk, err := readBulkString(reader, msg)
			if err != nil {
				return nil, err
			}
			parts[i] = bulk
		}
	}
	return parts, nil
}

func readLine(reader *bufio.Reader) ([]byte, bool, error) {
	msg, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, true, err
	}
	if len(msg) == 0 || msg[len(msg)-2] != '\r' {
		return nil, false, errors.New("protocol error: " + string(msg))
	}
	return msg, false, nil
}
