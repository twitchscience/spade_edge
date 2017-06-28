package requests

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"mime"
	"net"
	"net/http"
	"net/url"
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
	hostSamplingRate = float32(0.01)
	xDomainContents  = func() (b []byte) {
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
	// from https://commons.wikimedia.org/wiki/File:Transparent.gif
	transparentPixel = []byte{
		71, 73, 70, 56, 57, 97, 1, 0, 1, 0,
		128, 0, 0, 0, 0, 0, 255, 255, 255,
		33, 249, 4, 1, 0, 0, 0, 0, 44, 0,
		0, 0, 0, 1, 0, 1, 0, 0, 2, 1, 68, 0, 59,
	}
)

const corsMaxAge = "86400" // One day

// EdgeLoggers represent the different kind of loggers for Spade events
type EdgeLoggers struct {
	sync.WaitGroup
	closed             chan struct{}
	S3EventLogger      loggers.SpadeEdgeLogger
	KinesisEventLogger loggers.SpadeEdgeLogger
}

// NewEdgeLoggers returns a new instance of an EdgeLoggers struct pre-filled
// wuth UndefinedLogger logger instances
func NewEdgeLoggers() *EdgeLoggers {
	return &EdgeLoggers{
		closed:             make(chan struct{}),
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

	eventErr := e.S3EventLogger.Log(event)
	kinesisErr := e.KinesisEventLogger.Log(event)

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
	e.S3EventLogger.Close()
}

// SpadeHandler handles http requests and forwards them to the EdgeLoggers
type SpadeHandler struct {
	StatLogger  statsd.Statter
	EdgeLoggers *EdgeLoggers
	Time        func() time.Time // Defaults to time.Now
	EdgeType    string
	corsOrigins map[string]bool
	instanceID  string

	// eventCount counts the number of event requests handled. It is used in
	// uuid generation. eventCount is read and written from multiple go routines
	// so any access to it should go through sync/atomic
	eventCount             uint64
	eventInURISamplingRate float32
}

// NewSpadeHandler returns a new instance of SpadeHandler
func NewSpadeHandler(stats statsd.Statter, loggers *EdgeLoggers, instanceID string, CORSOrigins []string, eventInURISamplingRate float32, edgeType string) *SpadeHandler {
	h := &SpadeHandler{
		StatLogger:             stats,
		EdgeLoggers:            loggers,
		Time:                   time.Now,
		EdgeType:               edgeType,
		instanceID:             instanceID,
		corsOrigins:            make(map[string]bool),
		eventInURISamplingRate: eventInURISamplingRate,
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
	maxUserAgentBytes    = 1024
)

var allowedMethods = map[string]bool{
	"GET":     true,
	"POST":    true,
	"OPTIONS": true,
}
var allowedMethodsHeader string // Comma-separated version of allowedMethods

func (s *SpadeHandler) logLargeRequestError(r *http.Request, data string) {
	_ = s.StatLogger.Inc("large_request", 1, 0.1)
	head := truncate(data, 100)
	logger.WithField("sent_from", r.Header.Get("X-Forwarded-For")).
		WithField("user_agent", r.Header.Get("User-Agent")).
		WithField("content_length", r.ContentLength).
		WithField("data_head", head).
		Warn("Request larger than 500KB, rejecting.")
}

func (s *SpadeHandler) logLargeUserAgentError(r *http.Request, data string) {
	_ = s.StatLogger.Inc("large_user_agent", 1, 0.1)
	head := truncate(data, 100)
	userAgent := truncate(r.Header.Get("User-Agent"), 100)
	logger.WithField("user_agent", userAgent).
		WithField("data_head", head).
		Warn(fmt.Sprintf("User agent larger than %d bytes, dropping.", maxUserAgentBytes))
}

func truncate(s string, max int) string {
	if len(s) > max {
		return s[:max]
	}

	return s
}

func sanitizeHostValue(host string) string {
	if host == "" {
		return ""
	}
	hostWithoutPort := strings.Split(strings.ToLower(strings.TrimSpace(host)), ":")[0]
	return strings.Replace(hostWithoutPort, ".", "_", -1)
}

func (s *SpadeHandler) handleSpadeRequests(r *http.Request, values url.Values, context *requestContext) int {
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
		_ = s.StatLogger.Inc("bad_request.parse_form", 1, 0.01)
		return http.StatusBadRequest
	}

	if _, ok := values["data"]; ok {
		_ = s.StatLogger.Inc("event_in_URI", 1, s.eventInURISamplingRate)
	}

	if len(r.RequestURI) > 8192 {
		_ = s.StatLogger.Inc("large_URI", 1, 1)
	}

	if host := sanitizeHostValue(r.Host); len(host) > 0 {
		_ = s.StatLogger.Inc(fmt.Sprintf("requests.hosts.%s", host), 1, hostSamplingRate)
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
			_ = s.StatLogger.Inc("bad_request.read_data", 1, 0.01)
			return http.StatusBadRequest
		}
		if bytes.Equal(b[:5], dataFlag) {
			context.BadClient = true
			b = b[5:]
		}
		data = string(b)

	}
	if data == "" {
		_ = s.StatLogger.Inc("bad_request.empty", 1, 0.01)
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

	var userAgent string
	if values.Get("ua") == "1" {
		userAgent = r.Header.Get("User-Agent")
		// anything over the max is likely garbage data
		if len(userAgent) > maxUserAgentBytes {
			s.logLargeUserAgentError(r, data)
			userAgent = ""
		}
	}

	event := spade.NewEvent(
		context.Now,
		clientIP,
		xForwardedFor,
		uuid,
		data,
		userAgent,
		s.EdgeType,
	)

	defer func() {
		context.Timers["write"] = statTimer.stopTiming()
	}()

	err = s.EdgeLoggers.log(event, context)

	if err != nil {
		_ = s.StatLogger.Inc("bad_request.write_failure", 1, 0.01)
		return http.StatusBadRequest
	}

	if shouldWritePixel(values) {
		return http.StatusOK
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
	status := s.serve(w, r, context)
	_ = s.StatLogger.Inc(fmt.Sprintf("status_code.%d", status), 1, 0.001)
	context.setStatus(status)
	context.Timers["http"] = timer.stopTiming()

	context.recordStats(s.StatLogger)
}

func (s *SpadeHandler) serve(w http.ResponseWriter, r *http.Request, context *requestContext) int {
	var status int
	path := r.URL.Path
	if strings.HasPrefix(path, "/v1/") {
		path = "/track"
	}
	switch path {
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
		values := r.URL.Query()
		status = s.handleSpadeRequests(r, values, context)

		if shouldWritePixel(values) {
			if err := writePixel(w); err != nil {
				logger.WithError(err).Error("Error writing transparent pixel response")
				status = http.StatusInternalServerError
			} else {
				// header and body have already been written
				return http.StatusOK
			}
		}
	// dont track everything else
	default:
		context.Endpoint = badEndpoint
		status = http.StatusNotFound
	}
	w.WriteHeader(status)
	return status
}

func shouldWritePixel(values url.Values) bool {
	return values.Get("img") == "1"
}

func writePixel(w http.ResponseWriter) error {
	w.Header().Set("Content-Type", "image/gif")
	w.Header().Set("Cache-Control", "no-cache, max-age=0")
	_, err := w.Write(transparentPixel)
	return err
}

func init() {
	var allowedMethodsList []string
	for k := range allowedMethods {
		allowedMethodsList = append(allowedMethodsList, k)
	}
	allowedMethodsHeader = strings.Join(allowedMethodsList, ", ")
}
