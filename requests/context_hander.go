package requests

import (
	"strconv"
	"strings"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/twitchscience/spade_edge/loggers"
)

// TODO naming?
type timerInstance struct {
	start time.Time
}

func newTimerInstance() *timerInstance {
	return &timerInstance{
		start: time.Now().UTC(),
	}
}

func (t *timerInstance) stopTiming() (r time.Duration) {
	r = time.Now().Sub(t.start)
	t.start = time.Now().UTC()
	return
}

type requestContext struct {
	Now           time.Time
	Method        string
	IPHeader      string
	Endpoint      string
	Timers        map[string]time.Duration
	FailedLoggers []string
	Status        int
	BadClient     bool
}

func (r *requestContext) setStatus(s int) *requestContext {
	r.Status = s
	return r
}

func (r *requestContext) recordLoggerAttempt(err error, name string) {
	if err != nil && err != loggers.ErrUndefined {
		r.FailedLoggers = append(r.FailedLoggers, name)
	}
}

func (r *requestContext) recordStats(statter statsd.Statter) {
	prefix := strings.Join([]string{
		r.Method,
		strings.Replace(r.Endpoint, ".", "_", -1),
		strconv.Itoa(r.Status),
	}, ".")
	for stat, duration := range r.Timers {
		_ = statter.Timing(strings.Join([]string{prefix, stat}, "."), duration.Nanoseconds(), 0.1)
	}
	for _, logger := range r.FailedLoggers {
		_ = statter.Inc(strings.Join([]string{prefix, logger, "failed"}, "."), 1, 1.0)
	}
	if r.BadClient {
		// We expect these to be infrequent. We may want to decreate this
		// if it turns out not to be the case
		_ = statter.Inc("bad_client", 1, 1.0)
	}
}
