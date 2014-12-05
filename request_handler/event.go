package request_handler

import (
	"fmt"

	"github.com/twitchscience/scoop_protocol/spade"
)

func auditTrail(e *spade.Event) string {
	return fmt.Sprintf("[%d] %s", e.ReceivedAt.Unix(), e.Uuid)
}
