package requests

import (
	"strconv"
	"strings"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/twitchscience/spade_edge/loggers"
)

// TimerInstance returns the time since the start or last time it was stopped.
type TimerInstance struct {
	start time.Time
}

// NewTimerInstance builds a TimerInstance.
func NewTimerInstance() *TimerInstance {
	return &TimerInstance{
		start: time.Now().UTC(),
	}
}

// StopTiming returns the time since start/the last StopTiming call and starts a new timer.
func (t *TimerInstance) StopTiming() (r time.Duration) {
	r = time.Since(t.start)
	t.start = time.Now().UTC()
	return
}

// RequestContext is contextual information for a request.
type RequestContext struct {
	Now           time.Time
	Method        string
	IPHeader      string
	Endpoint      string
	Timers        map[string]time.Duration
	FailedLoggers []string
	Status        int
	BadClient     bool
}

// RecordLoggerAttempt records failed logging attempts for later reporting.
func (r *RequestContext) RecordLoggerAttempt(err error, name string) {
	if err != nil && err != loggers.ErrUndefined {
		r.FailedLoggers = append(r.FailedLoggers, name)
	}
}

// RecordStats sends the request's stats to the statter.
func (r *RequestContext) RecordStats(statter statsd.StatSender) {
	prefix := strings.Join([]string{
		r.Method,
		strings.Replace(r.Endpoint, ".", "_", -1),
		strconv.Itoa(r.Status),
	}, ".")
	for stat, duration := range r.Timers {
		_ = statter.Timing(strings.Join([]string{prefix, stat}, "."), duration.Nanoseconds(), 0.1)
	}
	for _, logger := range r.FailedLoggers {
		_ = statter.Inc(strings.Join([]string{prefix, logger, "failed"}, "."), 1, 0.1)
	}
	if r.BadClient {
		_ = statter.Inc("bad_client", 1, 0.1)
	}
}
