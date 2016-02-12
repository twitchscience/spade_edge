package loggers

import (
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

func NewS3Logger(
	s3Connection *s3.S3,
	name string,
	errorQueueName string,
	maxLogLines int,
	maxLogAge time.Duration,
	notify bool,
	loggingDir string,
	printFunc EventToStringFunc,
) (SpadeEdgeLogger, error) {

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
		BuildSQSErrorHarness(errorQueueName),
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
