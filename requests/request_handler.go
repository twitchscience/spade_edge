package requests

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"mime"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/scoop_protocol/spade"
	"github.com/twitchscience/spade_edge/loggers"
)

var (
	xDomainContents = func() (b []byte) {
		filename := os.Getenv("CROSS_DOMAIN_LOCATION")
		if filename == "" {
			filename = "../build/config/crossdomain.xml"
		}
		b, err := ioutil.ReadFile(filename)
		if err != nil {
			logger.WithError(err).Fatal("Cross domain file not found")
		}
		return
	}()
	xmlApplicationType = mime.TypeByExtension(".xml")
	xarth              = []byte("XARTH")
	dataFlag           = []byte("data=")
)

const corsMaxAge = "86400" // One day

// EdgeLoggers represent the different kind of loggers for Spade events
type EdgeLoggers struct {
	sync.WaitGroup
	closed             chan struct{}
	S3AuditLogger      loggers.SpadeEdgeLogger
	S3EventLogger      loggers.SpadeEdgeLogger
	KinesisEventLogger loggers.SpadeEdgeLogger
}

// NewEdgeLoggers returns a new instance of an EdgeLoggers struct pre-filled
// wuth UndefinedLogger logger instances
func NewEdgeLoggers() *EdgeLoggers {
	return &EdgeLoggers{
		closed:             make(chan struct{}),
		S3AuditLogger:      loggers.UndefinedLogger{},
		S3EventLogger:      loggers.UndefinedLogger{},
		KinesisEventLogger: loggers.UndefinedLogger{},
	}
}

func (e *EdgeLoggers) log(event *spade.Event, context *requestContext) error {
	e.Add(1)
	defer e.Done()

	// If reading from the `closed` channel succeeds, the logger is closed.
	select {
	case <-e.closed:
		return errors.New("Loggers are shutting down")
	default: // Make this a non-blocking select
	}

	auditErr := e.S3AuditLogger.Log(event)
	eventErr := e.S3EventLogger.Log(event)
	kinesisErr := e.KinesisEventLogger.Log(event)

	context.recordLoggerAttempt(auditErr, "audit")
	context.recordLoggerAttempt(eventErr, "event")
	context.recordLoggerAttempt(kinesisErr, "kinesis")

	if eventErr != nil &&
		kinesisErr != nil {
		return errors.New("Failed to store the event in any of the loggers")
	}

	return nil
}

// Close closes the loggers
func (e *EdgeLoggers) Close() {
	close(e.closed)
	e.Wait()

	e.KinesisEventLogger.Close()
	e.S3AuditLogger.Close()
	e.S3EventLogger.Close()
}

// SpadeHandler handles http requests and forwards them to the EdgeLoggers
type SpadeHandler struct {
	StatLogger  statsd.Statter
	EdgeLoggers *EdgeLoggers
	Time        func() time.Time // Defaults to time.Now
	instanceID  string
	corsOrigins map[string]bool

	// eventCount counts the number of event requests handled. It is used in
	// uuid generation. eventCount is read and written from multiple go routines
	// so any access to it should go through sync/atomic
	eventCount uint64
}

// NewSpadeHandler returns a new instance of SpadeHandler
func NewSpadeHandler(stats statsd.Statter, loggers *EdgeLoggers, instanceID string, CORSOrigins []string) *SpadeHandler {
	h := &SpadeHandler{
		StatLogger:  stats,
		EdgeLoggers: loggers,
		Time:        time.Now,
		instanceID:  instanceID,
		corsOrigins: make(map[string]bool),
	}

	for _, origin := range CORSOrigins {
		trimmedOrigin := strings.TrimSpace(origin)
		if trimmedOrigin != "" {
			h.corsOrigins[trimmedOrigin] = true
		}
	}
	return h
}

func parseLastForwarder(header string) net.IP {
	var clientIP string
	comma := strings.LastIndex(header, ",")
	if comma > -1 && comma < len(header)+1 {
		clientIP = header[comma+1:]
	} else {
		clientIP = header
	}

	return net.ParseIP(strings.TrimSpace(clientIP))
}

const (
	ipForwardHeader      = "X-Forwarded-For"
	badEndpoint          = "FourOhFour"
	nTimers              = 5
	maxBytesPerRequest   = 500 * 1024
	largeBodyErrorString = "http: request body too large" // Magic error string from the http pkg
)

var allowedMethods = map[string]bool{
	"GET":     true,
	"POST":    true,
	"OPTIONS": true,
}
var allowedMethodsHeader string // Comma-separated version of allowedMethods

func (s *SpadeHandler) logLargeRequestError(r *http.Request, data string) {
	_ = s.StatLogger.Inc("large_request", 1, 0.1)
	head := data
	if len(head) > 100 {
		head = head[:100]
	}
	logger.WithField("sent_from", r.Header.Get("X-Forwarded-For")).
		WithField("user_agent", r.Header.Get("User-Agent")).
		WithField("content_length", r.ContentLength).
		WithField("data_head", head).
		Warn("Request larger than 500KB, rejecting.")
}

func (s *SpadeHandler) handleSpadeRequests(r *http.Request, context *requestContext) int {
	statTimer := newTimerInstance()

	xForwardedFor := r.Header.Get(context.IPHeader)
	clientIP := parseLastForwarder(xForwardedFor)

	context.Timers["ip"] = statTimer.stopTiming()

	err := r.ParseForm()
	if err != nil {
		if err.Error() == largeBodyErrorString {
			s.logLargeRequestError(r, "")
			return http.StatusRequestEntityTooLarge
		}
		return http.StatusBadRequest
	}

	data := r.Form.Get("data")
	if data == "" && r.Method == "POST" {
		// if we're here then our clients have POSTed us something weird,
		// for example, something that maybe
		// application/x-www-form-urlencoded but with the Content-Type
		// header set incorrectly... best effort here on out

		var b []byte
		b, err = ioutil.ReadAll(r.Body)
		if err != nil {
			if err.Error() == largeBodyErrorString {
				s.logLargeRequestError(r, string(b))
				return http.StatusRequestEntityTooLarge
			}
			return http.StatusBadRequest
		}
		if bytes.Equal(b[:5], dataFlag) {
			context.BadClient = true
			b = b[5:]
		}
		data = string(b)

	}
	if data == "" {
		return http.StatusBadRequest
	}

	bData := []byte(data)
	if len(bData) > maxBytesPerRequest {
		s.logLargeRequestError(r, data)
		return http.StatusRequestEntityTooLarge
	}

	context.Timers["data"] = statTimer.stopTiming()

	count := atomic.AddUint64(&s.eventCount, 1)
	uuid := fmt.Sprintf("%s-%08x-%08x", s.instanceID, context.Now.Unix(), count)
	context.Timers["uuid"] = statTimer.stopTiming()

	event := spade.NewEvent(
		context.Now,
		clientIP,
		xForwardedFor,
		uuid,
		data,
	)

	defer func() {
		context.Timers["write"] = statTimer.stopTiming()
	}()

	err = s.EdgeLoggers.log(event, context)

	if err != nil {
		return http.StatusBadRequest
	}
	return http.StatusNoContent
}

func (s *SpadeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !allowedMethods[r.Method] {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.Header().Set("Vary", "Origin")

	origin := r.Header.Get("Origin")
	if s.corsOrigins[origin] {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", allowedMethodsHeader)
	}

	if r.Method == "OPTIONS" {
		w.Header().Set("Access-Control-Max-Age", corsMaxAge)
		w.WriteHeader(http.StatusOK)
		return
	}

	context := &requestContext{
		Now:       s.Time(),
		Method:    r.Method,
		Endpoint:  r.URL.Path,
		IPHeader:  ipForwardHeader,
		Timers:    make(map[string]time.Duration, nTimers),
		BadClient: false,
	}
	timer := newTimerInstance()
	context.setStatus(s.serve(w, r, context))
	context.Timers["http"] = timer.stopTiming()

	context.recordStats(s.StatLogger)
}

func (s *SpadeHandler) serve(w http.ResponseWriter, r *http.Request, context *requestContext) int {
	var status int
	switch r.URL.Path {
	case "/crossdomain.xml":
		w.Header().Add("Content-Type", xmlApplicationType)
		_, err := w.Write(xDomainContents)
		if err != nil {
			logger.WithError(err).Error("Unable to write crossdomain.xml contents")
			return http.StatusInternalServerError
		}
		return http.StatusOK
	case "/healthcheck":
		status = http.StatusOK
	case "/xarth":
		_, err := w.Write(xarth)
		if err != nil {
			logger.WithError(err).Error("Error writing XARTH response")
			return http.StatusInternalServerError
		}
		return http.StatusOK
	// Accepted tracking endpoints.
	case "/", "/track", "/track/":
		status = s.handleSpadeRequests(r, context)
	// dont track everything else
	default:
		context.Endpoint = badEndpoint
		status = http.StatusNotFound
	}
	w.WriteHeader(status)
	return status
}

func init() {
	var allowedMethodsList []string
	for k := range allowedMethods {
		allowedMethodsList = append(allowedMethodsList, k)
	}
	allowedMethodsHeader = strings.Join(allowedMethodsList, ", ")
}
