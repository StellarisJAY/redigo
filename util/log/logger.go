package log

import (
	"errors"
	"io"
	"io/ioutil"
	"log"
	"os"
)

const (
	LevelInfo = iota
	LevelWarn
	LevelError
	LevelDebug

	PrefixError = "\033[31m[ERROR]\033[0m \u001B[34m"
	PrefixWarn  = "\033[33m[WARN]\033[0m \u001B[34m"
	PrefixInfo  = "\033[32m[INFO]\033[0m \u001B[34m"
	PrefixDebug = "\033[36m[DEBUG]\033[0m \u001B[34m"
)

var (
	prefixs      = []string{PrefixInfo, PrefixWarn, PrefixError, PrefixDebug}
	globalLogger = NewLogger(LevelDebug, os.Stdout)
)

type Logger struct {
	loggers []*log.Logger
}

func NewLogger(level int, out io.Writer) *Logger {
	if level < 0 {
		panic(errors.New("invalid log level"))
	}
	if level > LevelDebug {
		level = LevelDebug
	}
	l := new(Logger)
	l.loggers = make([]*log.Logger, LevelDebug+1)
	i := 0
	for ; i <= level; i++ {
		if i == LevelInfo {
			l.loggers[i] = log.New(out, prefixs[i], log.LstdFlags)
		} else {
			l.loggers[i] = log.New(out, prefixs[i], log.LstdFlags|log.Lshortfile)
		}
	}
	for ; i <= LevelDebug; i++ {
		l.loggers[i] = log.New(ioutil.Discard, "", 0)
	}
	return l
}

func (l *Logger) Info(format string, args ...interface{}) {
	l.loggers[LevelInfo].Printf("\033[0m"+format, args...)
}

func (l *Logger) Warn(format string, args ...interface{}) {
	l.loggers[LevelWarn].Printf("\033[0m"+format, args...)
}

func (l *Logger) Error(err error) {
	l.loggers[LevelError].Printf("\033[0m" + err.Error())
}

func (l *Logger) Errorf(format string, args ...interface{}) {
	l.loggers[LevelError].Printf("\033[0m"+format, args...)
}

func (l *Logger) Debug(format string, args ...interface{}) {
	l.loggers[LevelDebug].Printf("\033[0m"+format, args...)
}

func (l *Logger) SetOutput(out io.Writer) {
	for _, logger := range l.loggers {
		logger.SetOutput(out)
	}
}

func Info(format string, args ...interface{}) {
	globalLogger.Info(format, args...)
}

func Warn(format string, args ...interface{}) {
	globalLogger.Warn(format, args...)
}

func Error(err error) {
	globalLogger.Error(err)
}

func Errorf(format string, args ...interface{}) {
	globalLogger.Errorf(format, args...)
}

func Debug(format string, args ...interface{}) {
	globalLogger.Debug(format, args...)
}

func SetOutput(out io.Writer) {
	globalLogger.SetOutput(out)
}
