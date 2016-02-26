package requests

import (
	"bytes"
	"errors"
	"io/ioutil"
	"log"
	"mime"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/twitchscience/scoop_protocol/spade"
	"github.com/twitchscience/spade_edge/loggers"
	"github.com/twitchscience/spade_edge/uuid"
)

var (
	xDomainContents    []byte
	xmlApplicationType = mime.TypeByExtension(".xml")
	xarth              = []byte("XARTH")
	dataFlag           = []byte("data=")
)

func init() {
	f := os.Getenv("CROSS_DOMAIN_LOCATION")
	if len(f) == 0 {
		f = "../build/config/crossdomain.xml"
	}
	var err error
	xDomainContents, err = ioutil.ReadFile(f)
	if err != nil {
		log.Fatalln("Cross domain file not found: ", err)
	}
}

const corsMaxAge = "86400" // One day

// EdgeLoggers represent the different kind of loggers for Spade events
type EdgeLoggers struct {
	S3AuditLogger      loggers.SpadeEdgeLogger
	S3EventLogger      loggers.SpadeEdgeLogger
	KinesisEventLogger loggers.SpadeEdgeLogger
}

// NewEdgeLoggers returns a new instance of an EdgeLoggers struct pre-filled
// wuth UndefinedLogger logger instances
func NewEdgeLoggers() *EdgeLoggers {
	return &EdgeLoggers{
		loggers.UndefinedLogger{},
		loggers.UndefinedLogger{},
		loggers.UndefinedLogger{},
	}
}

func (e *EdgeLoggers) log(event *spade.Event, context *requestContext) error {
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
	e.KinesisEventLogger.Close()
	e.S3AuditLogger.Close()
	e.S3EventLogger.Close()
}

// SpadeHandler handles http requets and forwards them to the EdgeLoggers
type SpadeHandler struct {
	StatLogger  statsd.Statter
	EdgeLoggers *EdgeLoggers
	Assigner    uuid.Assigner
	Time        func() time.Time // Defaults to time.Now
	corsOrigins map[string]bool
}

// NewSpadeHandler returns a new instance of SpadeHandler
func NewSpadeHandler(stats statsd.Statter, loggers *EdgeLoggers, assigner uuid.Assigner, CORSOrigins []string) *SpadeHandler {
	h := &SpadeHandler{
		StatLogger:  stats,
		EdgeLoggers: loggers,
		Assigner:    assigner,
		Time:        time.Now,
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

func (s *SpadeHandler) handleSpadeRequests(r *http.Request, context *requestContext) int {
	statTimer := newTimerInstance()

	xForwardedFor := r.Header.Get(context.IPHeader)
	clientIP := parseLastForwarder(xForwardedFor)

	context.Timers["ip"] = statTimer.stopTiming()

	err := r.ParseForm()
	if err != nil {
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

	context.Timers["data"] = statTimer.stopTiming()

	// // get event
	uuid := s.Assigner.Assign()
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

const (
	ipForwardHeader = "X-Forwarded-For"
	badEndpoint     = "FourOhFour"
	nTimers         = 5
)

var allowedMethods = map[string]bool{
	"GET":     true,
	"POST":    true,
	"OPTIONS": true,
}
var allowedMethodsHeader string // Comma-separated version of allowedMethods

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
		w.Write(xDomainContents)
		return http.StatusOK
	case "/healthcheck":
		status = http.StatusOK
	case "/xarth":
		w.Write(xarth)
		return http.StatusOK
	// Accepted tracking endpoints.
	case "/":
		status = s.handleSpadeRequests(r, context)
	case "/track":
		status = s.handleSpadeRequests(r, context)
	case "/track/":
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
