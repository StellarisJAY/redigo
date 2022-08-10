package parser

import (
	"bufio"
	"errors"
	"io"
	"log"
	"redigo/interface/redis"
	"redigo/redis/cmd"
	"strconv"
)

type Payload struct {
	Data []byte
	Err  error
}

func init() {
	log.SetFlags(log.Ldate | log.Lshortfile)
}

func Parse(reader *bufio.Reader) (*cmd.Command, error) {
	msg, ioErr, err := readLine(reader)
	if ioErr {
		return nil, io.EOF
	}
	if err != nil {
		return nil, err
	}
	var command *cmd.Command
	switch msg[0] {
	case redis.SingleLinePrefix:
		command = cmd.NewSingleLineCommand(msg[1 : len(msg)-2])
	case redis.NumberPrefix:
		command = cmd.NewNumberCommand(msg[1 : len(msg)-2])
	case redis.ErrorPrefix:
		command = cmd.NewErrorCommand(msg[1 : len(msg)-2])
	case redis.BulkPrefix:
		bulk, err := readBulkString(reader, msg)
		if err != nil {
			return nil, err
		}
		if bulk == nil {
			command = cmd.NewNilCommand()
		} else {
			command = cmd.NewBulkStringCommand(bulk)
		}
		return command, nil
	case redis.ArrayPrefix:
		// get Array size
		size, err := strconv.Atoi(string(msg[1 : len(msg)-2]))
		if err != nil {
			return nil, err
		}
		if size == 0 {
			return cmd.NewEmptyListCommand(), nil
		}
		// parse RESP array
		if parts, err := readArray(reader, size); err != nil {
			return nil, err
		} else {
			return cmd.NewCommand(parts), nil
		}
	}
	return command, nil
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
