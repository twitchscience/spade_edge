package main

import (
	"flag"
	"fmt"

	"github.com/afex/hystrix-go/hystrix"
	"github.com/twitchscience/scoop_protocol/spade"

	"github.com/twitchscience/spade_edge/loggers"
	"github.com/twitchscience/spade_edge/request_handler"

	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
)

var (
	stats_prefix = flag.String("stat_prefix", "edge", "statsd prefix")
	logging_dir  = flag.String("log_dir", ".", "The directory to log these files to")
	listen_port  = flag.String(
		"port",
		":8888",
		"Which port are we listenting on form: ':<port>' e.g. :8888",
	)
	cors_origins = flag.String("cors_origins", "",
		`Which origins should we advertise accepting POST and GET from.
Example: http://www.twitch.tv https://www.twitch.tv
Empty ignores CORS.`)

	event_log_name      = flag.String("event_log_name", "", "Name of the event log (or none)")
	audit_log_name      = flag.String("audit_log_name", "", "Name of the audit log (or none)")
	kinesis_stream_name = flag.String("kinesis_stream_name", "", "Name of kinesis stream (or none)")

	maxLogLines = int(getInt64FromEnv("MAX_LOG_LINES", 1000000))                          // default 1 million
	maxLogAge   = time.Duration(getInt64FromEnv("MAX_LOG_AGE_SECS", 10*60)) * time.Second // default 10 mins

	auditMaxLogLines = int(getInt64FromEnv("MAX_AUDIT_LOG_LINES", 1000000))                          // default 1 million
	auditMaxLogAge   = time.Duration(getInt64FromEnv("MAX_AUDIT_LOG_AGE_SECS", 10*60)) * time.Second // default 10 mins

)

func getInt64FromEnv(target string, def int64) int64 {
	env := os.Getenv(target)
	if env == "" {
		return def
	}
	i, err := strconv.ParseInt(env, 10, 64)
	if err != nil {
		return def
	}
	return i
}

func initStatsd(statsPrefix, statsdHostport string) (stats statsd.Statter, err error) {
	if statsdHostport == "" {
		stats, _ = statsd.NewNoop()
	} else {
		if stats, err = statsd.New(statsdHostport, *stats_prefix); err != nil {
			log.Fatalf("Statsd configuration error: %v\n", err)
		}
	}
	return
}

func main() {
	flag.Parse()

	stats, err := initStatsd(*stats_prefix, os.Getenv("STATSD_HOSTPORT"))
	if err != nil {
		log.Fatalf("Statsd configuration error: %v\n", err)
	}

	auth, err := aws.GetAuth("", "", "", time.Now())
	if err != nil {
		log.Fatalln("Failed to recieve auth from env")
	}
	s3Connection := s3.New(
		auth,
		aws.USWest2,
	)

	var allLoggers []request_handler.SpadeEdgeLogger

	if len(*event_log_name) > 0 {
		eventLogger, err := loggers.NewS3Logger(
			s3Connection,
			*event_log_name,
			maxLogLines,
			maxLogAge,
			true,
			*logging_dir,
			func(e *spade.Event) (string, error) {
				b, err := spade.Marshal(e)
				if err != nil {
					return "", err
				}
				return fmt.Sprintf("%s", b), nil
			})
		if err != nil {
			log.Fatalf("Error creating event logger: %v\n", err)
		}
		allLoggers = append(allLoggers, eventLogger)
	} else {
		log.Println("WARNING: No event logger specified!")
	}

	if len(*audit_log_name) > 0 {
		auditLogger, err := loggers.NewS3Logger(
			s3Connection,
			*audit_log_name,
			auditMaxLogLines,
			auditMaxLogAge,
			false,
			*logging_dir,
			func(e *spade.Event) (string, error) {
				return fmt.Sprintf("[%d] %s", e.ReceivedAt.Unix(), e.Uuid), nil
			})
		if err != nil {
			log.Fatalf("Error creating audit logger: %v\n", err)
		}
		allLoggers = append(allLoggers, auditLogger)
	} else {
		log.Println("WARNING: No audit logger specified!")
	}

	if len(*kinesis_stream_name) > 0 {
		kinesisLogger, err := loggers.NewKinesisLogger("us-west-2", *kinesis_stream_name)
		if err != nil {
			log.Fatalf("Error creating KinesisLogger %v\n", err)
		}
		allLoggers = append(allLoggers, kinesisLogger)
	} else {
		log.Println("WARNING: No kinesis logger specified!")
	}

	if len(allLoggers) == 0 {
		log.Fatalf("No loggers specified!")
	}

	// Trigger close on receipt of SIGINT
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		syscall.SIGINT)
	go func() {
		<-sigc
		// Cause flush
		for _, logger := range allLoggers {
			logger.Close()
		}
		os.Exit(0)
	}()

	hystrixStreamHandler := hystrix.NewStreamHandler()
	hystrixStreamHandler.Start()
	go http.ListenAndServe(net.JoinHostPort("", "81"), hystrixStreamHandler)

	go http.ListenAndServe(net.JoinHostPort("", "8082"), http.DefaultServeMux)

	// setup server and listen
	server := &http.Server{
		Addr:           *listen_port,
		Handler:        request_handler.NewSpadeHandler(stats, logger, request_handler.Assigner, *cors_origins),
		ReadTimeout:    5 * time.Second,
		WriteTimeout:   5 * time.Second,
		MaxHeaderBytes: 1 << 20, // 0.5MB
	}
	if err := server.ListenAndServe(); err != nil {
		log.Fatalln(err)
	}
}
