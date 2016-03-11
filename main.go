package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/afex/hystrix-go/hystrix"

	"github.com/twitchscience/scoop_protocol/spade"
	"github.com/twitchscience/spade_edge/loggers"
	"github.com/twitchscience/spade_edge/requests"
	"github.com/twitchscience/spade_edge/uuid"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"

	"github.com/tylerb/graceful"
)

var (
	configFilename = flag.String("config", "conf.json", "name of config file")
	statsdPrefix   = flag.String("stat_prefix", "", "statsd prefix")
)

func initStatsd(statsdHostport, prefix string) (stats statsd.Statter, err error) {
	if len(statsdHostport) == 0 {
		stats, _ = statsd.NewNoop()
		log.Println("WARNING: No statsd host:port specified, disabling metric statsd!")
	} else if len(prefix) == 0 {
		stats, _ = statsd.NewNoop()
		log.Println("WARNING: No statsd prefix specified, disabling metric statsd!")
	} else {
		if stats, err = statsd.New(statsdHostport, prefix); err != nil {
			log.Fatalf("Statsd configuration error: %v\n", err)
		}
	}
	return
}

func marshallingLoggingFunc(e *spade.Event) (string, error) {
	var b []byte
	b, err := spade.Marshal(e)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s", b), nil
}

func main() {
	flag.Parse()
	err := loadConfig(*configFilename)
	if err != nil {
		log.Fatalln("Error loading config", err)
	}

	stats, err := initStatsd(os.Getenv("STATSD_HOSTPORT"), *statsdPrefix)
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

	edgeLoggers := requests.NewEdgeLoggers()
	if config.EventsLogger != nil {
		edgeLoggers.S3EventLogger, err = loggers.NewS3Logger(
			s3Connection,
			*config.EventsLogger,
			config.LoggingDir,
			marshallingLoggingFunc)
		if err != nil {
			log.Fatalf("Error creating event logger: %v\n", err)
		}
	} else {
		log.Println("WARNING: No event logger specified!")
	}

	if config.AuditsLogger != nil {
		edgeLoggers.S3AuditLogger, err = loggers.NewS3Logger(
			s3Connection,
			*config.AuditsLogger,
			config.LoggingDir,
			func(e *spade.Event) (string, error) {
				return fmt.Sprintf("[%d] %s", e.ReceivedAt.Unix(), e.Uuid), nil
			})
		if err != nil {
			log.Fatalf("Error creating audit logger: %v\n", err)
		}
	} else {
		log.Println("WARNING: No audit logger specified!")
	}

	if config.EventStream != nil {
		var fallbackLogger loggers.SpadeEdgeLogger = loggers.UndefinedLogger{}
		if config.FallbackLogger != nil {
			fallbackLogger, err = loggers.NewS3Logger(
				s3Connection,
				*config.FallbackLogger,
				config.LoggingDir,
				marshallingLoggingFunc)
			if err != nil {
				log.Fatalf("Error creating fallback logger: %v\n", err)
			}
		} else {
			log.Println("WARNING: No fallback logger specified!")
		}

		edgeLoggers.KinesisEventLogger, err = loggers.NewKinesisLogger(*config.EventStream, fallbackLogger, stats)
		if err != nil {
			log.Fatalf("Error creating KinesisLogger %v\n", err)
		}
	} else {
		log.Println("WARNING: No kinesis logger specified!")
	}

	hystrixStreamHandler := hystrix.NewStreamHandler()
	hystrixStreamHandler.Start()
	go func() {
		err := http.ListenAndServe(net.JoinHostPort("", "81"), hystrixStreamHandler)
		if err != nil {
			log.Printf("Error listening to port 81 with hystrixStreamHandler %v\n", err)
		}
	}()

	go func() {
		err := http.ListenAndServe(net.JoinHostPort("", "8082"), http.DefaultServeMux)
		if err != nil {
			log.Printf("Error listening to port 8082 with http.DefaultServeMux %v\n", err)
		}
	}()

	uuidAssigner := uuid.StartUUIDAssigner(
		os.Getenv("HOST"),
		os.Getenv("CLOUD_CLUSTER"),
	)

	// setup server and listen
	server := &graceful.Server{
		NoSignalHandling: true,
		Server: &http.Server{
			Addr:           config.Port,
			Handler:        requests.NewSpadeHandler(stats, edgeLoggers, uuidAssigner, config.CorsOrigins),
			ReadTimeout:    5 * time.Second,
			WriteTimeout:   5 * time.Second,
			MaxHeaderBytes: 1 << 20, // 0.5MB
		},
	}

	if err := server.ListenAndServe(); err != nil {
		log.Fatalln(err)
	}

	edgeLoggers.Close()
	os.Exit(0)
}
