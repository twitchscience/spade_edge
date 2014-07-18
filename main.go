package main

import (
	"errors"
	"flag"
	"fmt"

	"github.com/TwitchScience/aws_utils/environment"
	"github.com/TwitchScience/aws_utils/notifier"
	"github.com/TwitchScience/aws_utils/uploader"

	"github.com/TwitchScience/gologging/gologging"
	gen "github.com/TwitchScience/gologging/key_name_generator"
	"github.com/TwitchScience/spade_edge/request_handler"

	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
)

const (
	auditBucketName = "spade-audits"
	eventBucketName = "spade-edge"
)

var (
	CLOUD_ENV    = environment.GetCloudEnv()
	stats_prefix = flag.String("stat_prefix", "edge", "statsd prefix")
	logging_dir  = flag.String("log_dir", ".", "The directory to log these files to")
	listen_port  = flag.String(
		"port",
		":8888",
		"Which port are we listenting on form: ':<port>' e.g. :8888",
	)
	VERSION, _ = strconv.Atoi(os.Getenv("EDGE_VERSION"))
)

type DummyNotifierHarness struct {
}
type SQSNotifierHarness struct {
	qName    string
	version  int
	notifier *notifier.SQSClient
}
type SQSErrorHarness struct {
	qName    string
	notifier *notifier.SQSClient
}

func (d *DummyNotifierHarness) SendMessage(r *uploader.UploadReceipt) error {
	return nil
}

func BuildSQSNotifierHarness() *SQSNotifierHarness {
	client := notifier.DefaultClient
	client.Signer.RegisterMessageType("edge", func(args ...interface{}) (string, error) {
		if len(args) < 2 {
			return "", errors.New("Missing correct number of args ")
		}
		return fmt.Sprintf("{\"version\":%d,\"keyname\":%q}", args...), nil
	})
	return &SQSNotifierHarness{
		qName:    fmt.Sprintf("spade-edge-%s", CLOUD_ENV),
		version:  VERSION,
		notifier: client,
	}
}

func BuildSQSErrorHarness() *SQSErrorHarness {
	return &SQSErrorHarness{
		qName:    fmt.Sprintf("uploader-error-spade-edge-%s", CLOUD_ENV),
		notifier: notifier.DefaultClient,
	}
}

func (s *SQSErrorHarness) SendError(er error) {
	err := s.notifier.SendMessage("error", s.qName, er)
	if err != nil {
		log.Println(err)
	}
}

func (s *SQSNotifierHarness) SendMessage(message *uploader.UploadReceipt) error {
	return s.notifier.SendMessage("edge", s.qName, s.version, message.KeyName)
}

const MAX_LINES_PER_LOG = 1000000 // 1 million

func main() {
	flag.Parse()

	statsdHostport := os.Getenv("STATSD_HOSTPORT")
	var stats statsd.Statter
	var err error
	if statsdHostport == "" {
		stats, _ = statsd.NewNoop()
	} else {
		if stats, err = statsd.New(statsdHostport, *stats_prefix); err != nil {
			log.Fatalf("Statsd configuration error: %v", err)
		}
		log.Printf("Connected to statsd at %s\n", statsdHostport)
	}

	auth, err := aws.GetAuth("", "", "", time.Now())
	if err != nil {
		log.Fatalln("Failed to recieve auth from env")
	}
	awsConnection := s3.New(
		auth,
		aws.USWest2,
	)

	auditBucket := awsConnection.Bucket(auditBucketName + "-" + CLOUD_ENV)
	auditBucket.PutBucket(s3.BucketOwnerFull)
	eventBucket := awsConnection.Bucket(eventBucketName + "-" + CLOUD_ENV)
	eventBucket.PutBucket(s3.BucketOwnerFull)

	auditInfo := gen.BuildInstanceInfo(&gen.EnvInstanceFetcher{}, "spade_edge_audit", *logging_dir)
	loggingInfo := gen.BuildInstanceInfo(&gen.EnvInstanceFetcher{}, "spade_edge", *logging_dir)

	auditRotateCoordinator := gologging.NewRotateCoordinator(MAX_LINES_PER_LOG, time.Minute*10)
	loggingRotateCoordinator := gologging.NewRotateCoordinator(MAX_LINES_PER_LOG, time.Minute*10)

	auditLogger, err := gologging.StartS3Logger(
		auditRotateCoordinator,
		auditInfo,
		&DummyNotifierHarness{},
		&uploader.S3UploaderBuilder{
			Bucket:           auditBucket,
			KeyNameGenerator: &gen.EdgeKeyNameGenerator{auditInfo},
		},
		BuildSQSErrorHarness(),
		2,
	)
	if err != nil {
		log.Fatalf("Got Error while building audit: %s\n", err)
	}

	spadeEventLogger, err := gologging.StartS3Logger(
		loggingRotateCoordinator,
		loggingInfo,
		BuildSQSNotifierHarness(),
		&uploader.S3UploaderBuilder{
			Bucket:           eventBucket,
			KeyNameGenerator: &gen.EdgeKeyNameGenerator{loggingInfo},
		},
		BuildSQSErrorHarness(),
		2,
	)
	if err != nil {
		log.Fatalf("Got Error while building logger: %s\n", err)
	}

	logger := &request_handler.FileAuditLogger{
		AuditLogger: auditLogger,
		SpadeLogger: spadeEventLogger,
	}

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		syscall.SIGINT)
	go func() {
		<-sigc
		// Cause flush
		logger.Close()
		os.Exit(0)
	}()

	// setup server and listen
	server := &http.Server{
		Addr: *listen_port,
		Handler: &request_handler.SpadeHandler{
			StatLogger: stats,
			EdgeLogger: logger,
			Assigner:   request_handler.Assigner,
		},
		ReadTimeout:    5 * time.Second,
		WriteTimeout:   5 * time.Second,
		MaxHeaderBytes: 1 << 20, // 0.5MB
	}
	if err := server.ListenAndServe(); err != nil {
		log.Fatalln(err)
	}
}
