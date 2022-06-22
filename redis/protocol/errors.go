package protocol

import "fmt"

var (
	WrongArgumentNumberError = "-ERR wrong number of arguments for '%s' command"
	UnknownCommandError      = "-ERR unknown command '%s'"
	ProtocolError            = []byte("-Error Wrong protocol")
)

func CreateWrongArgumentNumberError(command string) error {
	return fmt.Errorf(WrongArgumentNumberError, command)
}

func CreateUnknownCommandError(command string) error {
	return fmt.Errorf(UnknownCommandError, command)
}
