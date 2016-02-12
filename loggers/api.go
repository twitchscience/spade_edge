package loggers

import (
	"github.com/twitchscience/scoop_protocol/spade"
)

type SpadeEdgeLogger interface {
	Log(event *spade.Event) error
	Close()
}
