package redis

import (
	"errors"
	"fmt"
)

// Redis 中的错误声明
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
	AppendOnlyRewriteInProgressError = errors.New("ERR Background append only file rewriting already in progress")
	BackgroundSaveInProgressError    = errors.New("ERR Background save already in progress")
	NestedMultiCallError             = errors.New("ERR MULTI calls can not be nested")
	CommandCannotUseInMultiError     = errors.New("ERR Command can't be used in MULTI")
	ExecWithoutMultiError            = errors.New("ERR EXEC without MULTI")
	DiscardWithoutMultiError         = errors.New("ERR DISCARD without MULTI")
	NoSuchKeyError                   = errors.New("ERR no such key")
	ClusterPeerNotFoundError         = errors.New("ERR cluster peer not found")
	ClusterPeerUnreachableError      = errors.New("ERR can't reach cluster peer")
	MovedError                       = "MOVED %s"
	WatchInsideMultiError            = errors.New("ERR WATCH inside MULTI is not allowed")
	InvalidCoordinatePairError       = "ERR invalid longitude,latitude pair %.6f,%.6f"
	DistanceUnitError                = errors.New("ERR unsupported unit provided. please use m, km, ft, mi")
)

func CreateWrongArgumentNumberError(command string) error {
	return fmt.Errorf(WrongArgumentNumberError, command)
}

func CreateUnknownCommandError(command string) error {
	return fmt.Errorf(UnknownCommandError, command)
}

func CreateMovedError(targetAddr string) error {
	return fmt.Errorf(MovedError, targetAddr)
}

func CreateInvalidCoordinatePairError(longitude, latitude float64) error {
	return fmt.Errorf(InvalidCoordinatePairError, longitude, latitude)
}
