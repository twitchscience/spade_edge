package loggers

import (
	"errors"

	"github.com/twitchscience/scoop_protocol/spade"
)

// UndefinedLogger is a struct that implements the SpadeEdgeLogger interface with nop
// implementations
type UndefinedLogger struct {
}

var (
	// ErrUndefined tells the caller that the logger they tried to log to is not defined
	ErrUndefined = errors.New("Logger not defined")
)

// Log is a nop implementation
func (UndefinedLogger) Log(event *spade.Event) error {
	return ErrUndefined
}

// Close doesn't have anything to do
func (UndefinedLogger) Close() {}
