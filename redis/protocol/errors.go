package protocol

var (
	ProtocolError = []byte("-Error Wrong protocol")
)

func CreateWrongArgumentNumberError(command string) []byte {
	err := "-ERR wrong number of arguments for '" + command + "' command" + CRLF
	return []byte(err)
}

func CreateUnknownCommandError(command string) []byte {
	return []byte("-ERR unknown command '" + command + "'" + CRLF)
}
