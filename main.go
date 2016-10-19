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

	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/scoop_protocol/spade"
	"github.com/twitchscience/spade_edge/loggers"
	"github.com/twitchscience/spade_edge/requests"

	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/cactus/go-statsd-client/statsd"
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

func newS3Logger(loggerType string,
	cfg *loggers.S3LoggerConfig,
	loggingFunc loggers.EventToStringFunc,
	sqs sqsiface.SQSAPI,
	s3Uploader s3manageriface.UploaderAPI) loggers.SpadeEdgeLogger {
	if cfg == nil {
		logger.Warnf("No %s logger specified", loggerType)
		return loggers.UndefinedLogger{}
	}

	s3Logger, err := loggers.NewS3Logger(*cfg, config.LoggingDir, loggingFunc, sqs, s3Uploader)
	if err != nil {
		logger.WithError(err).Fatalf("Error creating %s logger", loggerType)
	}
	return s3Logger
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
	s3Uploader := s3manager.NewUploader(session)
	instanceID, err := ec2metadata.New(session).GetMetadata("instance-id")
	if err != nil {
		logger.WithError(err).Fatal("Error retrieving instance-id from metadata service")
	}

	edgeLoggers := requests.NewEdgeLoggers()
	edgeLoggers.S3EventLogger = newS3Logger("event", config.EventsLogger, marshallingLoggingFunc, sqs, s3Uploader)
	edgeLoggers.S3AuditLogger = newS3Logger("audit", config.AuditsLogger, edgeAuditLogFunc, sqs, s3Uploader)

	if config.EventStream == nil {
		logger.Warn("No kinesis logger specified")
	} else {
		fallbackLogger :=
			newS3Logger("fallback", config.FallbackLogger, marshallingLoggingFunc, sqs, s3Uploader)
		edgeLoggers.KinesisEventLogger, err =
			loggers.NewKinesisLogger(kinesis.New(session), *config.EventStream, fallbackLogger, stats)
		if err != nil {
			logger.WithError(err).Fatal("Error creating Kinesis logger")
		}
	}

	// Trigger close on receipt of SIGINT
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT)
	go func() {
		<-sigc
		logger.Info("Sigint received -- shutting down")
		edgeLoggers.Close()
		logger.Info("Exiting main cleanly.")
		os.Exit(0)
	}()

	hystrixStreamHandler := hystrix.NewStreamHandler()
	hystrixStreamHandler.Start()
	go func() {
		hystrixErr := http.ListenAndServe(net.JoinHostPort("", "81"), hystrixStreamHandler)
		logger.WithError(hystrixErr).Error("Error listening to port 81 with hystrixStreamHandler")
	}()

	go func() {
		defaultErr := http.ListenAndServe(net.JoinHostPort("", "8082"), http.DefaultServeMux)
		logger.WithError(defaultErr).Error("Error listening to port 8082 with http.DefaultServeMux")
	}()

	// setup server and listen
	server := &http.Server{
		Addr:           config.Port,
		Handler:        requests.NewSpadeHandler(stats, edgeLoggers, instanceID, config.CorsOrigins),
		ReadTimeout:    5 * time.Second,
		WriteTimeout:   5 * time.Second,
		MaxHeaderBytes: 1 << 20, // 1MB
	}

	err = server.ListenAndServe()
	logger.WithError(err).Fatal("Error serving")
}
