/*
blahPackage spade_edge provides a write-only API server for data ingest into
the Spade pipeline. It performs light validation, annotation, and manages
writes to Kinesis and S3. The service is typically behind an Elastic Load
Balancer, which handles concerns such as HTTPS. Standard requests result in a
204 No Content, and the persisted event is annotated with source IP, a
generated UUID, and server time.
*/
package main

import (
	"flag"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"
	"time"

	"golang.org/x/net/netutil"

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

var (
	configFilename = flag.String("config", "conf.json", "name of config file")
	statsdPrefix   = flag.String("stat_prefix", "", "statsd prefix")
	edgeType       = flag.String("edge_type", "", "edge type (internal/external)")
)

const maxConnections = 16000

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
	flag.Parse()
	err := loadConfig(*configFilename)
	if err != nil {
		logger.WithError(err).Fatal("Error loading config")
	}

	logger.InitWithRollbar("info", config.RollbarToken, config.RollbarEnvironment)
	logger.Info("Starting edge")
	logger.CaptureDefault()
	defer logger.LogPanic()

	numCPU := runtime.NumCPU()
	logger.Info("NUMCPU: %v", numCPU)

	runtime.GOMAXPROCS(numCPU * 2)

	stats, err := initStatsd(os.Getenv("STATSD_HOSTPORT"), *statsdPrefix)
	if err != nil {
		logger.WithError(err).Fatal("Statsd configuration error")
	}

	session, err := session.NewSession()
	if err != nil {
		logger.WithError(err).Fatal("Session not created")
	}
	sqs := sqs.New(session)
	s3Uploader := s3manager.NewUploader(session)
	instanceID, err := ec2metadata.New(session).GetMetadata("instance-id")
	if err != nil {
		logger.WithError(err).Fatal("Error retrieving instance-id from metadata service")
	}

	edgeLoggers := requests.NewEdgeLoggers()
	edgeLoggers.S3EventLogger = newS3Logger("event", config.EventsLogger, marshallingLoggingFunc, sqs, s3Uploader)

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

	if *edgeType != spade.INTERNAL_EDGE && *edgeType != spade.EXTERNAL_EDGE {
		logger.WithField("edgeType", *edgeType).Fatal("Invalid edge type")
	}

	// Trigger close on receipt of SIGINT
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT)
	logger.Go(func() {
		<-sigc
		logger.Info("Sigint received -- shutting down")
		edgeLoggers.Close()
		logger.Info("Exiting main cleanly.")
		logger.Wait()
		os.Exit(0)
	})

	hystrixStreamHandler := hystrix.NewStreamHandler()
	hystrixStreamHandler.Start()
	logger.Go(func() {
		hystrixErr := http.ListenAndServe(":81", hystrixStreamHandler)
		logger.WithError(hystrixErr).Error("Error listening to port 81 with hystrixStreamHandler")
	})

	logger.Go(func() {
		logger.WithError(http.ListenAndServe(":7766", http.DefaultServeMux)).
			Error("Serving pprof failed")
	})

	f, err := os.Create("/tmp/block")
	if err != nil {
		logger.WithError(err).Error("opening block file failed")
	}
	runtime.SetBlockProfileRate(100000)
	tick := time.NewTicker(time.Second)
	go func() {
		for range tick.C {
			pprof.Lookup("block").WriteTo(f, 1)
		}
	}()

	l, err := net.Listen("tcp", config.Port)

	if err != nil {
		logger.Errorf("Error creating listener: %v", err)
		return
	}
	ll := netutil.LimitListener(l, maxConnections)
	defer func() {
		if cerr := ll.Close(); cerr != nil {
			logger.WithError(cerr).Error("Error closing listener")
		}
	}()

	// setup server and listen
	server := &http.Server{
		Addr: config.Port,
		Handler: requests.NewSpadeHandler(
			stats,
			edgeLoggers,
			instanceID,
			config.CorsOrigins,
			config.EventInURISamplingRate,
			*edgeType),
		ReadTimeout:    15 * time.Second,
		WriteTimeout:   5 * time.Second,
		MaxHeaderBytes: 1 << 20, // 1MB
	}

	err = server.Serve(ll)
	logger.WithError(err).Error("Error serving")
}
