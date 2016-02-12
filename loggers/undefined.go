package loggers

import (
	"errors"

	"github.com/twitchscience/scoop_protocol/spade"
)

type UndefinedLogger struct {
}

var (
	ErrUndefined = errors.New("Logger not defined")
)

func (UndefinedLogger) Log(event *spade.Event) error {
	return ErrUndefined
}

func (UndefinedLogger) Close() {}
