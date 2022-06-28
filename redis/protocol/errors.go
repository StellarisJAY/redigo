package protocol

import (
	"errors"
	"fmt"
)

var (
	WrongArgumentNumberError         = "ERR wrong number of arguments for '%s' command"
	UnknownCommandError              = "ERR unknown command '%s'"
	HashValueNotIntegerError         = errors.New("ERR hash value is not an integer")
	ProtocolError                    = []byte("Error Wrong protocol")
	WrongTypeOperationError          = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	ValueNotIntegerOrOutOfRangeError = errors.New("ERR value is not an integer or out of range")
	InvalidDBIndexError              = errors.New("ERR invalid DB index")
	DBIndexOutOfRangeError           = errors.New("ERR DB index is out of range")
	ValueNotFloatError               = errors.New("ERR value is not a valid float")
	SyntaxError                      = errors.New("ERR syntax error")
)

func CreateWrongArgumentNumberError(command string) error {
	return fmt.Errorf(WrongArgumentNumberError, command)
}

func CreateUnknownCommandError(command string) error {
	return fmt.Errorf(UnknownCommandError, command)
}
