package loggers

import (
	"fmt"
	"time"

	"github.com/crowdmob/goamz/s3"

	"github.com/twitchscience/aws_utils/uploader"
	"github.com/twitchscience/gologging/gologging"
	"github.com/twitchscience/gologging/key_name_generator"
	"github.com/twitchscience/scoop_protocol/spade"
)

type EventToStringFunc func(*spade.Event) (string, error)

type s3Logger struct {
	uploadLogger      *gologging.UploadLogger
	eventToStringFunc EventToStringFunc
}

type S3LoggerConfig struct {
	Bucket       string
	SuccessQueue string `json:",omitempty"`
	ErrorQueue   string `json:",omitempty"`
	MaxLines     int
	MaxAge       string
}

func NewS3Logger(
	s3Connection *s3.S3,
	config S3LoggerConfig,
	loggingDir string,
	printFunc EventToStringFunc,
) (SpadeEdgeLogger, error) {
	var (
		successNotifier uploader.NotifierHarness      = &DummyNotifierHarness{}
		errorNotifier   uploader.ErrorNotifierHarness = &DummyNotifierHarness{}
	)

	maxAge, err := time.ParseDuration(config.MaxAge)
	if err != nil {
		return nil, fmt.Errorf("error parsing %s as a time.Duration: %v", config.MaxAge, err)
	}

	if len(config.SuccessQueue) > 0 {
		successNotifier = BuildSQSNotifierHarness(config.SuccessQueue)
	}

	if len(config.ErrorQueue) > 0 {
		errorNotifier = BuildSQSErrorHarness(config.ErrorQueue)
	}

	rotateCoordinator := gologging.NewRotateCoordinator(config.MaxLines, maxAge)
	loggingInfo := key_name_generator.BuildInstanceInfo(&key_name_generator.EnvInstanceFetcher{}, config.Bucket, loggingDir)

	eventBucket := s3Connection.Bucket(config.Bucket)
	eventBucket.PutBucket(s3.BucketOwnerFull)

	s3Uploader := &uploader.S3UploaderBuilder{
		Bucket:           eventBucket,
		KeyNameGenerator: &key_name_generator.EdgeKeyNameGenerator{Info: loggingInfo},
	}

	uploadLogger, err := gologging.StartS3Logger(
		rotateCoordinator,
		loggingInfo,
		successNotifier,
		s3Uploader,
		errorNotifier,
		2,
	)

	if err != nil {
		return nil, err
	}

	s3l := &s3Logger{
		uploadLogger:      uploadLogger,
		eventToStringFunc: printFunc,
	}

	return s3l, nil
}

func (s3l *s3Logger) Log(e *spade.Event) error {
	s, err := s3l.eventToStringFunc(e)
	if err != nil {
		return err
	}
	s3l.uploadLogger.Log(s)
	return nil
}

func (s3l *s3Logger) Close() {
	s3l.uploadLogger.Close()
}
