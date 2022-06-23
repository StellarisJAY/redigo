package protocol

import (
	"errors"
	"fmt"
)

var (
	WrongArgumentNumberError = "ERR wrong number of arguments for '%s' command"
	UnknownCommandError      = "ERR unknown command '%s'"
	HashValueNotIntegerError = errors.New("ERR hash value is not an integer")
	ProtocolError            = []byte("Error Wrong protocol")
	WrongTypeOperationError  = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
)

func CreateWrongArgumentNumberError(command string) error {
	return fmt.Errorf(WrongArgumentNumberError, command)
}

func CreateUnknownCommandError(command string) error {
	return fmt.Errorf(UnknownCommandError, command)
}
