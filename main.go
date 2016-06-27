package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/afex/hystrix-go/hystrix"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/sqs"

	_ "github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/scoop_protocol/spade"
	"github.com/twitchscience/spade_edge/loggers"
	"github.com/twitchscience/spade_edge/requests"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/twitchscience/aws_utils/logger"
)

//spadeEdgeAuditLog defines struct of audit log in spade-edge
type edgeAuditLog struct {
	UUID       string
	ReceivedAt time.Time
}

var (
	configFilename = flag.String("config", "conf.json", "name of config file")
	statsdPrefix   = flag.String("stat_prefix", "", "statsd prefix")
)

func initStatsd(statsdHostport, prefix string) (statsd.Statter, error) {
	switch {
	case len(statsdHostport) == 0:
		logger.Warn("No statsd host:port specified, disabling metric statsd")
		return statsd.NewNoop()
	case len(prefix) == 0:
		logger.Warn("No statsd prefix specified, disabling metric statsd")
		return statsd.NewNoop()
	default:
		return statsd.New(statsdHostport, prefix)
	}
}

func marshallingLoggingFunc(e *spade.Event) (str string, err error) {
	b, err := spade.Marshal(e)
	if err == nil {
		str = string(b)
	}
	return
}

func edgeAuditLogFunc(e *spade.Event) (string, error) {
	newAuditLog := edgeAuditLog{
		UUID:       e.Uuid,
		ReceivedAt: e.ReceivedAt,
	}
	jsonBytes, err := json.Marshal(newAuditLog)
	if err != nil { // create string explicitly given marshall error
		return fmt.Sprintf("{\"UUID\":\"%s\", \"ReceivedAt\":\"%v\"}", e.Uuid, e.ReceivedAt), err
	}
	return string(jsonBytes), nil
}

func main() {
	logger.Init("info")
	flag.Parse()
	err := loadConfig(*configFilename)
	if err != nil {
		logger.WithError(err).Fatal("Error loading config")
	}

	stats, err := initStatsd(os.Getenv("STATSD_HOSTPORT"), *statsdPrefix)
	if err != nil {
		logger.WithError(err).Fatal("Statsd configuration error")
	}

	session := session.New()
	sqs := sqs.New(session)
	kinesis := kinesis.New(session)
	s3Uploader := s3manager.NewUploader(session)
	metadata := ec2metadata.New(session)
	instanceID, err := metadata.GetMetadata("instance-id")
	if err != nil {
		logger.WithError(err).Fatal("Error retrieving instance-id from metadata service")
	}

	edgeLoggers := requests.NewEdgeLoggers()
	if config.EventsLogger == nil {
		logger.Warn("No event logger specified")
	} else {
		edgeLoggers.S3EventLogger, err = loggers.NewS3Logger(
			*config.EventsLogger,
			config.LoggingDir,
			marshallingLoggingFunc,
			sqs,
			s3Uploader)
		if err != nil {
			logger.WithError(err).Fatal("Error creating event logger")
		}
	}

	if config.AuditsLogger == nil {
		logger.Warn("No audit logger specified")
	} else {
		edgeLoggers.S3AuditLogger, err = loggers.NewS3Logger(
			*config.AuditsLogger,
			config.LoggingDir,
			edgeAuditLogFunc,
			sqs,
			s3Uploader)
		if err != nil {
			logger.WithError(err).Fatal("Error creating audit logger")
		}
	}

	if config.EventStream == nil {
		logger.Warn("No kinesis logger specified")
	} else {
		var fallbackLogger loggers.SpadeEdgeLogger = loggers.UndefinedLogger{}
		if config.FallbackLogger == nil {
			logger.Warn("No fallback logger specified")
		} else {
			fallbackLogger, err = loggers.NewS3Logger(
				*config.FallbackLogger,
				config.LoggingDir,
				marshallingLoggingFunc,
				sqs,
				s3Uploader)
			if err != nil {
				logger.WithError(err).Fatal("Error creating fallback logger")
			}
		}

		edgeLoggers.KinesisEventLogger, err =
			loggers.NewKinesisLogger(kinesis, *config.EventStream, fallbackLogger, stats)
		if err != nil {
			logger.WithError(err).Fatal("Error creating Kinesis logger")
		}
	}

	// Trigger close on receipt of SIGINT
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT)
	go func() {
		<-sigc
		edgeLoggers.Close()
		os.Exit(0)
	}()

	hystrixStreamHandler := hystrix.NewStreamHandler()
	hystrixStreamHandler.Start()
	go func() {
		err := http.ListenAndServe(net.JoinHostPort("", "81"), hystrixStreamHandler)
		if err != nil {
			logger.WithError(err).Error("Error listening to port 81 with hystrixStreamHandler")
		}
	}()

	go func() {
		err := http.ListenAndServe(net.JoinHostPort("", "8082"), http.DefaultServeMux)
		if err != nil {
			logger.WithError(err).Error("Error listening to port 8082 with http.DefaultServeMux")
		}
	}()

	// setup server and listen
	server := &http.Server{
		Addr:           config.Port,
		Handler:        requests.NewSpadeHandler(stats, edgeLoggers, instanceID, config.CorsOrigins),
		ReadTimeout:    5 * time.Second,
		WriteTimeout:   5 * time.Second,
		MaxHeaderBytes: 1 << 20, // 1MB
	}

	err = server.ListenAndServe()	// always err != nil
	logger.WithError(err).Fatal("Error serving")
}
