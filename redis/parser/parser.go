package parser

import (
	"bufio"
	"errors"
	"io"
	"log"
	"redigo/redis"
	"strconv"
)

type Payload struct {
	Data []byte
	Err  error
}

func init() {
	log.SetFlags(log.Ldate | log.Lshortfile)
}

func Parse(reader *bufio.Reader) (*redis.Command, error) {
	msg, ioErr, err := readLine(reader)
	if ioErr {
		return nil, io.EOF
	}
	if err != nil {
		return nil, err
	}
	// RESP Array type
	if msg[0] == '*' {
		cmd := redis.NewCommand()
		// get Array size
		size, err := strconv.Atoi(string(msg[1 : len(msg)-2]))
		if err != nil {
			return nil, err
		}
		// parse RESP array
		if err = readArray(reader, size, cmd); err != nil {
			return nil, err
		}
		return cmd, nil

	} else if msg[0] == '$' {
		bulk, err := readBulkString(reader, msg)
		if err != nil {
			return nil, err
		}
		cmd := redis.NewCommand()
		cmd.Append(bulk)
		return cmd, nil
	} else {
		return nil, nil
	}
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

func readArray(reader *bufio.Reader, size int, cmd *redis.Command) error {
	for i := 0; i < size; i++ {
		// read a line
		msg, ioErr, err := readLine(reader)
		if ioErr {
			return io.EOF
		} else if err != nil {
			return err
		}
		// read RESP Array
		if msg[0] == '$' {
			bulk, err := readBulkString(reader, msg)
			if err != nil {
				return err
			}
			cmd.Append(bulk)
		} else if msg[0] == ':' {
			// read RESP number

		}
	}
	return nil
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
