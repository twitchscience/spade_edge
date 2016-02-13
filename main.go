package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/afex/hystrix-go/hystrix"

	"github.com/twitchscience/scoop_protocol/spade"
	"github.com/twitchscience/spade_edge/loggers"
	"github.com/twitchscience/spade_edge/request_handler"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
)

var (
	statsPrefix = flag.String("stat_prefix", "edge", "statsd prefix")
	loggingDir  = flag.String("log_dir", ".", "The directory to log these files to")
	listenPort  = flag.String(
		"port",
		":8888",
		"Which port are we listenting on form: ':<port>' e.g. :8888",
	)
	corsOrigins = flag.String("cors_origins", "",
		`Which origins should we advertise accepting POST and GET from.
Example: http://www.twitch.tv https://www.twitch.tv
Empty ignores CORS.`)

	eventLogName      = flag.String("eventLogName", "", "Name of the event log (or none)")
	eventErrorQueue   = flag.String("eventErrorQueue", "", "SQS queue to log event log uploader errors (or none)")
	auditLogName      = flag.String("auditLogName", "", "Name of the audit log (or none)")
	auditErrorQueue   = flag.String("auditErrorQueue", "", "SQS queue to log audit log uploader errors (or none)")
	kinesisStreamName = flag.String("kinesisStreamName", "", "Name of kinesis stream (or none)")

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

func initStatsd(statsdHostport string) (stats statsd.Statter, err error) {
	if statsdHostport == "" {
		stats, _ = statsd.NewNoop()
	} else {
		if stats, err = statsd.New(statsdHostport, *statsPrefix); err != nil {
			log.Fatalf("Statsd configuration error: %v\n", err)
		}
	}
	return
}

func main() {
	flag.Parse()

	stats, err := initStatsd(os.Getenv("STATSD_HOSTPORT"))
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

	edgeLoggers := request_handler.NewEdgeLoggers()

	if len(*eventLogName) > 0 {
		edgeLoggers.S3EventLogger, err = loggers.NewS3Logger(
			s3Connection,
			*eventLogName,
			*eventErrorQueue,
			maxLogLines,
			maxLogAge,
			true,
			*loggingDir,
			func(e *spade.Event) (string, error) {
				var b []byte
				b, err = spade.Marshal(e)
				if err != nil {
					return "", err
				}
				return fmt.Sprintf("%s", b), nil
			})
		if err != nil {
			log.Fatalf("Error creating event logger: %v\n", err)
		}
	} else {
		log.Println("WARNING: No event logger specified!")
	}

	if len(*auditLogName) > 0 {
		edgeLoggers.S3AuditLogger, err = loggers.NewS3Logger(
			s3Connection,
			*auditLogName,
			*auditErrorQueue,
			auditMaxLogLines,
			auditMaxLogAge,
			false,
			*loggingDir,
			func(e *spade.Event) (string, error) {
				return fmt.Sprintf("[%d] %s", e.ReceivedAt.Unix(), e.Uuid), nil
			})
		if err != nil {
			log.Fatalf("Error creating audit logger: %v\n", err)
		}
	} else {
		log.Println("WARNING: No audit logger specified!")
	}

	if len(*kinesisStreamName) > 0 {
		edgeLoggers.KinesisEventLogger, err = loggers.NewKinesisLogger("us-west-2", *kinesisStreamName)
		if err != nil {
			log.Fatalf("Error creating KinesisLogger %v\n", err)
		}
	} else {
		log.Println("WARNING: No kinesis logger specified!")
	}

	// Trigger close on receipt of SIGINT
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		syscall.SIGINT)
	go func() {
		<-sigc
		edgeLoggers.Close()
		os.Exit(0)
	}()

	hystrixStreamHandler := hystrix.NewStreamHandler()
	hystrixStreamHandler.Start()
	go func() { _ = http.ListenAndServe(net.JoinHostPort("", "81"), hystrixStreamHandler) }()

	go func() { _ = http.ListenAndServe(net.JoinHostPort("", "8082"), http.DefaultServeMux) }()

	// setup server and listen
	server := &http.Server{
		Addr:           *listenPort,
		Handler:        request_handler.NewSpadeHandler(stats, edgeLoggers, request_handler.Assigner, *corsOrigins),
		ReadTimeout:    5 * time.Second,
		WriteTimeout:   5 * time.Second,
		MaxHeaderBytes: 1 << 20, // 0.5MB
	}
	if err := server.ListenAndServe(); err != nil {
		log.Fatalln(err)
	}
}
