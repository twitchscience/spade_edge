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
	configFilename = flag.String("config", "conf.json", "name of config file")
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
		if stats, err = statsd.New(statsdHostport, config.StatsdPrefix); err != nil {
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

		edgeLoggers.KinesisEventLogger, err = loggers.NewKinesisLogger(*config.EventStream, fallbackLogger)
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

	// setup server and listen
	server := &http.Server{
		Addr:           config.Port,
		Handler:        request_handler.NewSpadeHandler(stats, edgeLoggers, request_handler.Assigner, config.CorsOrigins),
		ReadTimeout:    5 * time.Second,
		WriteTimeout:   5 * time.Second,
		MaxHeaderBytes: 1 << 20, // 0.5MB
	}
	if err := server.ListenAndServe(); err != nil {
		log.Fatalln(err)
	}
}
