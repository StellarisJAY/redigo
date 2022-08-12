package redis

var (
	NullBulkStringReplyBytes = []byte("$-1\r\n")
	CRLF                     = "\r\n"
	OKReplyBytes             = []byte("+OK\r\n")
	NumberReplyPrefix        = ':'
)
