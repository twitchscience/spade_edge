package loggers

import (
	"github.com/twitchscience/scoop_protocol/spade"
)

// A SpadeEdgeLogger stores events that are received at the spade edge
type SpadeEdgeLogger interface {
	Log(event *spade.Event) error
	Close()
}
