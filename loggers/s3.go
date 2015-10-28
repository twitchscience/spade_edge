package loggers

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/crowdmob/goamz/s3"

	"github.com/twitchscience/aws_utils/notifier"
	"github.com/twitchscience/aws_utils/uploader"
	"github.com/twitchscience/gologging/gologging"
	"github.com/twitchscience/gologging/key_name_generator"
	"github.com/twitchscience/scoop_protocol/spade"
	"github.com/twitchscience/spade_edge/request_handler"
)

var (
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

func BuildSQSNotifierHarness(name string) *SQSNotifierHarness {
	client := notifier.DefaultClient
	client.Signer.RegisterMessageType("edge", func(args ...interface{}) (string, error) {
		if len(args) < 2 {
			return "", errors.New("Missing correct number of args ")
		}
		return fmt.Sprintf("{\"version\":%d,\"keyname\":%q}", args...), nil
	})
	return &SQSNotifierHarness{
		qName:    name,
		version:  VERSION,
		notifier: client,
	}
}

func BuildSQSErrorHarness(name string) *SQSErrorHarness {
	return &SQSErrorHarness{
		qName:    fmt.Sprintf("uploader-error-%s", name),
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

type EventToStringFunc func(*spade.Event) (string, error)

type s3Logger struct {
	*gologging.UploadLogger
	EventToStringFunc
}

func NewS3Logger(
	s3Connection *s3.S3,
	name string,
	maxLogLines int,
	maxLogAge time.Duration,
	notify bool,
	loggingDir string,
	printFunc EventToStringFunc,
) (request_handler.SpadeEdgeLogger, error) {

	rotateCoordinator := gologging.NewRotateCoordinator(maxLogLines, maxLogAge)
	loggingInfo := key_name_generator.BuildInstanceInfo(&key_name_generator.EnvInstanceFetcher{}, name, loggingDir)
	var notifierHarness uploader.NotifierHarness = &DummyNotifierHarness{}
	if notify {
		notifierHarness = BuildSQSNotifierHarness(name)
	}

	eventBucket := s3Connection.Bucket(name)
	eventBucket.PutBucket(s3.BucketOwnerFull)

	uploadLogger, err := gologging.StartS3Logger(
		rotateCoordinator,
		loggingInfo,
		notifierHarness,
		&uploader.S3UploaderBuilder{
			Bucket:           eventBucket,
			KeyNameGenerator: &key_name_generator.EdgeKeyNameGenerator{Info: loggingInfo},
		},
		BuildSQSErrorHarness(name),
		2,
	)

	if err != nil {
		return nil, err
	}

	s3l := &s3Logger{
		UploadLogger:      uploadLogger,
		EventToStringFunc: printFunc,
	}

	return s3l, nil
}

func (s3l *s3Logger) Log(e *spade.Event) error {
	s, err := s3l.EventToStringFunc(e)
	if err != nil {
		return err
	}
	s3l.UploadLogger.Log(s)
	return nil
}

func (s3l *s3Logger) Close() {
	s3l.UploadLogger.Close()
}
