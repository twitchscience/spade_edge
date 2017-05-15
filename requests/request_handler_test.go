package requests

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/cactus/go-statsd-client/statsd/statsdtest"
	"github.com/twitchscience/scoop_protocol/spade"
)

const (
	instanceID     = "i-test"
	eventInURIStat = "event_in_URI"
	hostStatPrefix = "requests.hosts."
)

type testEdgeLogger struct {
	events [][]byte
}

type testRequest struct {
	Endpoint    string
	Verb        string
	ContentType string
	Body        string
	UserAgent   string
}

type testHeader struct {
	Header string
	Value  string
}

type testResponse struct {
	Code    int
	Body    string
	Headers []testHeader
}

type testTuple struct {
	DataExpectation      string
	UserAgentExpectation string
	Request              testRequest
	Response             testResponse
}

var epoch = time.Unix(0, 0)

func (t *testEdgeLogger) Log(e *spade.Event) error {
	logLine, err := spade.Marshal(e)
	if err != nil {
		return err
	}
	t.events = append(t.events, logLine)
	return nil
}

func (t *testEdgeLogger) Close() {}

func TestTooBigRequest(t *testing.T) {
	s, _ := statsd.NewNoop()
	spadeHandler := makeSpadeHandler(s)
	testrecorder := httptest.NewRecorder()
	req, err := http.NewRequest(
		"POST",
		"http://spade.example.com/",
		strings.NewReader(fmt.Sprintf("data=%s", longJSON)),
	)
	if err != nil {
		t.Fatalf("Failed to build request: %s error: %s\n", "/", err)
	}
	req.Header.Add("X-Forwarded-For", "222.222.222.222")
	req.Header.Add("Host", "spade.twitch.tv:80")
	spadeHandler.ServeHTTP(testrecorder, req)

	if testrecorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("%s expected code %d not %d\n", "/", http.StatusRequestEntityTooLarge, testrecorder.Code)
	}
}

func TestParseLastForwarder(t *testing.T) {
	var testHeaders = []struct {
		input    string
		expected net.IP
	}{
		{"a, b, 192.168.1.1", net.ParseIP("192.168.1.1")},
		{"a, b,192.168.1.1 ", net.ParseIP("192.168.1.1")},
		{"a, 10.1.1.1,", nil},
		{" 192.168.1.1", net.ParseIP("192.168.1.1")},
	}

	for _, h := range testHeaders {
		output := parseLastForwarder(h.input)
		if !h.expected.Equal(output) {
			t.Fatalf("%s -> %s instead of expected %s", h.input, output, h.expected)
		}
	}
}

var fixedTime = time.Date(2014, 5, 2, 19, 34, 1, 0, time.UTC)

func makeSpadeHandler(s statsd.Statter) *SpadeHandler {
	c := s
	loggers := NewEdgeLoggers()
	loggers.S3EventLogger = &testEdgeLogger{}
	spadeHandler := NewSpadeHandler(c, loggers, instanceID, []string{""}, 1)
	spadeHandler.Time = func() time.Time { return fixedTime }
	return spadeHandler
}

func TestEndPoints(t *testing.T) {
	s, _ := statsd.NewNoop()
	spadeHandler := makeSpadeHandler(s)
	var expectedEvents []spade.Event
	fixedIP := net.ParseIP("222.222.222.222")

	uuidCounter := 1

	for _, tt := range testRequests {
		testrecorder := httptest.NewRecorder()
		req, err := http.NewRequest(
			tt.Request.Verb,
			"http://spade.twitch.tv/"+tt.Request.Endpoint,
			strings.NewReader(tt.Request.Body),
		)
		if err != nil {
			t.Fatalf("Failed to build request: %s error: %s\n", tt.Request.Endpoint, err)
		}
		req.Header.Add("X-Forwarded-For", "222.222.222.222")
		req.Header.Add("Host", "spade.twitch.tv:80")
		if tt.Request.ContentType != "" {
			req.Header.Add("Content-Type", tt.Request.ContentType)
		}
		if tt.Request.UserAgent != "" {
			req.Header.Add("User-Agent", tt.Request.UserAgent)
		}
		spadeHandler.ServeHTTP(testrecorder, req)
		if testrecorder.Code != tt.Response.Code {
			t.Fatalf("%s expected code %d not %d\n", tt.Request.Endpoint, tt.Response.Code, testrecorder.Code)
		}
		if testrecorder.Body.String() != tt.Response.Body {
			t.Fatalf("%s expected body %s not %s\n", tt.Request.Endpoint, tt.Response.Body, testrecorder.Body.String())
		}

		for _, expectedHeader := range tt.Response.Headers {
			val := testrecorder.Header().Get(expectedHeader.Header)
			if expectedHeader.Value != val {
				t.Fatalf("%[1]s expected header '%[2]s: %[3]s' not '%[2]s: %[4]s'\n", tt.Request.Endpoint, expectedHeader.Header, expectedHeader.Value, val)
			}
		}

		if tt.DataExpectation != "" {
			expectedEvents = append(expectedEvents, spade.Event{
				ReceivedAt:    fixedTime.UTC(),
				ClientIp:      fixedIP,
				XForwardedFor: fixedIP.String(),
				Uuid:          fmt.Sprintf("%s-%08x-%08x", instanceID, fixedTime.UTC().Unix(), uuidCounter),
				Data:          tt.DataExpectation,
				UserAgent:     tt.UserAgentExpectation,
				Version:       spade.PROTOCOL_VERSION,
			})
			uuidCounter++
		}
	}

	logger := spadeHandler.EdgeLoggers.S3EventLogger.(*testEdgeLogger)
	for idx, byteLog := range logger.events {
		var ev spade.Event
		err := spade.Unmarshal(byteLog, &ev)
		if err != nil {
			t.Errorf("Expected Unmarshal to work, input: %s, err: %s", byteLog, err)
		}
		if !reflect.DeepEqual(ev, expectedEvents[idx]) {
			t.Errorf("Event processed incorrectly: expected: %v got: %v", expectedEvents[idx], ev)
		}
	}
}

func TestHandle(t *testing.T) {
	s, _ := statsd.NewNoop()
	spadeHandler := makeSpadeHandler(s)
	for _, tt := range testRequests {
		testrecorder := httptest.NewRecorder()
		req, err := http.NewRequest(
			tt.Request.Verb,
			"http://spade.example.com/"+tt.Request.Endpoint,
			strings.NewReader(tt.Request.Body),
		)
		if err != nil {
			t.Fatalf("Failed to build request: %s error: %s\n", tt.Request.Endpoint, err)
		}
		req.Header.Add("X-Forwarded-For", "222.222.222.222")
		if tt.Request.ContentType != "" {
			req.Header.Add("Content-Type", tt.Request.ContentType)
		}
		context := &requestContext{
			Now:      epoch,
			Method:   req.Method,
			Endpoint: req.URL.Path,
			IPHeader: ipForwardHeader,
			Timers:   make(map[string]time.Duration, nTimers),
		}
		status := spadeHandler.serve(testrecorder, req, context)

		if status != tt.Response.Code {
			t.Fatalf("%s expected code %d not %d\n", tt.Request.Endpoint, tt.Response.Code, testrecorder.Code)
		}
	}
}

func expectURICountStat(tt *testTuple) bool {
	return strings.Contains(tt.Request.Endpoint, "data")
}

func hasURICountStat(rs *statsdtest.RecordingSender) bool {
	for _, stat := range rs.GetSent() {
		if stat.Stat == eventInURIStat {
			return true
		}
	}
	return false
}

func hasHostCountStat(rs *statsdtest.RecordingSender) bool {
	for _, stat := range rs.GetSent() {
		if strings.HasPrefix(stat.Stat, hostStatPrefix) {
			return true
		}
	}
	return false
}

func TestURICounting(t *testing.T) {
	rs := statsdtest.NewRecordingSender()
	statter, _ := statsd.NewClientWithSender(rs, "") // error is only for nil sender
	spadeHandler := makeSpadeHandler(statter)
	for idx, tt := range testRequests {
		rs.ClearSent()

		testRecorder := httptest.NewRecorder()
		req, err := http.NewRequest(
			tt.Request.Verb,
			"http://spade.example.com/"+tt.Request.Endpoint,
			strings.NewReader(tt.Request.Body))
		if err != nil {
			t.Fatalf("Failed to build request %s; error: %s", tt.Request.Endpoint, err)
		}
		req.Header.Add("X-Forwarded-For", "222.222.222.222")
		spadeHandler.ServeHTTP(testRecorder, req)

		if expectURICountStat(&tt) {
			if !hasURICountStat(rs) {
				t.Errorf("Expected count stat for %s request %d with body %#v, but none was made",
					tt.Request.Verb, idx, tt.Request.Body)
			}
		} else {
			if hasURICountStat(rs) {
				t.Errorf("Unexpected count stat for %s request %d with body %#v",
					tt.Request.Verb, idx, tt.Request.Body)
			}
		}
	}
}

func TestHostCounting(t *testing.T) {
	rs := statsdtest.NewRecordingSender()
	statter, _ := statsd.NewClientWithSender(rs, "") // error is only for nil sender
	spadeHandler := makeSpadeHandler(statter)

	hostSamplingRate = float32(1.0)
	testRecorder := httptest.NewRecorder()
	req, err := http.NewRequest(
		"POST",
		"http://spade.example.com/",
		strings.NewReader("blag"))
	if err != nil {
		t.Fatalf("Failed to build request: %s error: %s\n", "/", err)
	}

	req.Header.Add("X-Forwarded-For", "222.222.222.222")
	spadeHandler.ServeHTTP(testRecorder, req)
	if hasHostCountStat(rs) {
		t.Errorf("Expected no statsd metrics sent for an empty host")
	}

	req.Header.Add("Host", "spade.twitch.tv:80")
	spadeHandler.ServeHTTP(testRecorder, req)
	if !hasHostCountStat(rs) {
		t.Errorf("Expected statsd metrics sent when a host is provided")
	}

}

func BenchmarkRequests(b *testing.B) {
	s, _ := statsd.NewNoop()
	spadeHandler := makeSpadeHandler(s)
	reqGet, err := http.NewRequest("GET", "http://spade.twitch.tv/?data=blah", nil)
	if err != nil {
		b.Fatalf("Failed to build request error: %s\n", err)
	}
	reqGet.Header.Add("X-Forwarded-For", "222.222.222.222")

	reqPost, err := http.NewRequest("POST", "http://spade.twitch.tv/", strings.NewReader("data=blah"))
	if err != nil {
		b.Fatalf("Failed to build request error: %s\n", err)
	}
	reqPost.Header.Add("X-Forwarded-For", "222.222.222.222")
	testrecorder := httptest.NewRecorder()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			spadeHandler.ServeHTTP(testrecorder, reqPost)
		} else {
			spadeHandler.ServeHTTP(testrecorder, reqGet)
		}
	}
	b.ReportAllocs()
}

var (
	longJSON      = `{"event":"` + strings.Repeat("BigData", 700000) + `"}`
	longUserAgent = strings.Repeat("BigUserAgent", maxUserAgentBytes)

	testRequests = []testTuple{
		testTuple{
			Request: testRequest{
				Endpoint: "crossdomain.xml",
				Verb:     "GET",
			},
			Response: testResponse{
				Code: http.StatusOK,
				Body: string(xDomainContents),
			},
		},
		testTuple{
			Request: testRequest{
				Endpoint: "healthcheck",
				Verb:     "GET",
			},
			Response: testResponse{
				Code: http.StatusOK,
			},
		},
		testTuple{
			DataExpectation: "blah",
			Request: testRequest{
				Endpoint: "track?data=blah",
				Verb:     "GET",
			},
			Response: testResponse{
				Code: http.StatusNoContent,
			},
		},
		testTuple{
			DataExpectation: "blah",
			Request: testRequest{
				Endpoint: "track/?data=blah",
				Verb:     "GET",
			},
			Response: testResponse{
				Code: http.StatusNoContent,
			},
		},
		testTuple{
			DataExpectation: "eyJldmVudCI6ImhlbGxvIn0",
			Request: testRequest{
				Endpoint: "track/?data=eyJldmVudCI6ImhlbGxvIn0&ip=1",
				Verb:     "GET",
			},
			Response: testResponse{
				Code: http.StatusNoContent,
			},
		},
		testTuple{
			Request: testRequest{
				Endpoint: "track",
				Verb:     "GET",
			},
			Response: testResponse{
				Code: http.StatusBadRequest,
			},
		},
		testTuple{
			DataExpectation: "blat",
			Request: testRequest{
				Endpoint: "?data=blat",
				Verb:     "GET",
			},
			Response: testResponse{
				Code: http.StatusNoContent,
			},
		},
		testTuple{
			DataExpectation: "blag",
			Request: testRequest{
				Verb:        "POST",
				ContentType: "application/x-randomfoofoo",
				Body:        "blag",
			},
			Response: testResponse{
				Code: http.StatusNoContent,
			},
		},
		// The next request is a bad client that passes our tests,
		// hopefully these should be incredibly rare. They do not parse at
		// our processor level
		testTuple{
			DataExpectation: "ip=&data=blagi",
			Request: testRequest{
				Verb:        "POST",
				ContentType: "application/x-randomfoofoo",
				Body:        "ip=&data=blagi",
			},
			Response: testResponse{
				Code: http.StatusNoContent,
			},
		},
		testTuple{
			DataExpectation: "bleck",
			Request: testRequest{
				Verb:        "POST",
				ContentType: "application/x-www-form-urlencoded",
				Body:        "data=bleck",
			},
			Response: testResponse{
				Code: http.StatusNoContent,
			},
		},
		testTuple{
			DataExpectation: "blog",
			Request: testRequest{
				Verb:        "POST",
				ContentType: "application/x-www-form-urlencoded",
				Body:        "ip=&data=blog",
			},
			Response: testResponse{
				Code: http.StatusNoContent,
			},
		},
		testTuple{
			DataExpectation: "blem",
			Request: testRequest{
				Verb:     "POST",
				Endpoint: "track?ip=&data=blem",
			},
			Response: testResponse{
				Code: http.StatusNoContent,
			},
		},
		testTuple{
			DataExpectation: "blamo",
			Request: testRequest{
				Verb:     "GET",
				Endpoint: "track?ip=&data=blamo",
			},
			Response: testResponse{
				Code: http.StatusNoContent,
			},
		},
		testTuple{
			Request: testRequest{
				Endpoint: "/spam/spam",
				Verb:     "POST",
				Body:     "data=bleck",
			},
			Response: testResponse{
				Code: http.StatusNotFound,
			},
		},
		testTuple{
			DataExpectation: "eyJldmVudCI6ImVtYWlsX29wZW4iLCJwcm9wZXJ0aWVzIjp7Im5vdGlmaWNhdGlvbl9pZCI6ImFiY2RlZmdoaWprbG1ub3BxcnVzdHZ3eXh6In19",
			Request: testRequest{
				Endpoint: "track/?data=eyJldmVudCI6ImVtYWlsX29wZW4iLCJwcm9wZXJ0aWVzIjp7Im5vdGlmaWNhdGlvbl9pZCI6ImFiY2RlZmdoaWprbG1ub3BxcnVzdHZ3eXh6In19&img=1",
				Verb:     "GET",
			},
			Response: testResponse{
				Code: http.StatusOK,
				Body: string(transparentPixel),
				Headers: []testHeader{
					{
						Header: "Cache-Control",
						Value:  "no-cache, max-age=0",
					},
					{
						Header: "Content-Type",
						Value:  "image/gif",
					},
				},
			},
		},
		testTuple{
			DataExpectation:      "eyJldmVudCI6ImhlbGxvIn0",
			UserAgentExpectation: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36",
			Request: testRequest{
				Endpoint:  "track/?data=eyJldmVudCI6ImhlbGxvIn0&ua=1",
				Verb:      "GET",
				UserAgent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36",
			},
			Response: testResponse{
				Code: http.StatusNoContent,
			},
		},
		testTuple{
			DataExpectation:      "eyJldmVudCI6ImhlbGxvIn0",
			UserAgentExpectation: "",
			Request: testRequest{
				Endpoint:  "track/?data=eyJldmVudCI6ImhlbGxvIn0",
				Verb:      "GET",
				UserAgent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36",
			},
			Response: testResponse{
				Code: http.StatusNoContent,
			},
		},
		testTuple{
			DataExpectation:      "eyJldmVudCI6ImhlbGxvIn0",
			UserAgentExpectation: "",
			Request: testRequest{
				Endpoint:  "track/?data=eyJldmVudCI6ImhlbGxvIn0&ua=1",
				Verb:      "GET",
				UserAgent: longUserAgent,
			},
			Response: testResponse{
				Code: http.StatusNoContent,
			},
		},
		testTuple{
			DataExpectation:      "eyJldmVudCI6ImVtYWlsX29wZW4iLCJwcm9wZXJ0aWVzIjp7Im5vdGlmaWNhdGlvbl9pZCI6ImFiY2RlZmdoaWprbG1ub3BxcnVzdHZ3eXh6In19",
			UserAgentExpectation: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36",
			Request: testRequest{
				Endpoint:  "track/?data=eyJldmVudCI6ImVtYWlsX29wZW4iLCJwcm9wZXJ0aWVzIjp7Im5vdGlmaWNhdGlvbl9pZCI6ImFiY2RlZmdoaWprbG1ub3BxcnVzdHZ3eXh6In19&img=1&ua=1",
				Verb:      "GET",
				UserAgent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36",
			},
			Response: testResponse{
				Code: http.StatusOK,
				Body: string(transparentPixel),
				Headers: []testHeader{
					{
						Header: "Cache-Control",
						Value:  "no-cache, max-age=0",
					},
					{
						Header: "Content-Type",
						Value:  "image/gif",
					},
				},
			},
		},
	}
)
