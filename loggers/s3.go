package loggers

import (
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"

	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/aws_utils/uploader"
	"github.com/twitchscience/gologging/gologging"
	"github.com/twitchscience/gologging/key_name_generator"
	"github.com/twitchscience/scoop_protocol/spade"
)

// An EventToStringFunc takes a spade event and converts it
//to a string for logging into a line oriented file on s3
type EventToStringFunc func(*spade.Event) (string, error)

type s3Logger struct {
	uploadLoggers     []*gologging.UploadLogger
	eventToStringFunc EventToStringFunc
	messages          chan<- string
	wg                *sync.WaitGroup
}

// S3LoggerConfig configures a new SpadeEdgeLogger that writes
// lines of text to AWS S3
type S3LoggerConfig struct {
	Bucket       string
	SuccessQueue string
	ErrorQueue   string
	MaxLines     int
	MaxAge       string
	NumS3Loggers int
}

// NewS3Logger returns a new SpadeEdgeLogger that events to S3 after
// transforming the events into lines of text using the printFunc
func NewS3Logger(
	config S3LoggerConfig,
	loggingDir string,
	printFunc EventToStringFunc,
	sqs sqsiface.SQSAPI,
	S3Uploader s3manageriface.UploaderAPI,
) (SpadeEdgeLogger, error) {
	var (
		successNotifier uploader.NotifierHarness      = &DummyNotifierHarness{}
		errorNotifier   uploader.ErrorNotifierHarness = &DummyNotifierHarness{}
	)

	maxAge, err := time.ParseDuration(config.MaxAge)
	if err != nil {
		return nil, fmt.Errorf("parsing %s as a time.Duration: %v", config.MaxAge, err)
	}

	numLoggers := config.NumS3Loggers
	if numLoggers <= 0 {
		return nil, fmt.Errorf("number of S3 loggers (%d) is not set or not positive", numLoggers)
	}

	if len(config.SuccessQueue) > 0 {
		successNotifier = buildSQSNotifierHarness(sqs, config.SuccessQueue)
	}

	if len(config.ErrorQueue) > 0 {
		errorNotifier = buildSQSErrorHarness(sqs, config.ErrorQueue)
	}

	loggingInfo := key_name_generator.BuildInstanceInfo(&key_name_generator.EnvInstanceFetcher{}, config.Bucket, loggingDir)

	s3UploaderFactory := uploader.NewFactory(
		config.Bucket,
		&key_name_generator.EdgeKeyNameGenerator{Info: loggingInfo},
		S3Uploader,
	)

	wg := &sync.WaitGroup{}
	wg.Add(numLoggers)
	messages := make(chan string)
	uploadLoggers := make([]*gologging.UploadLogger, 0, numLoggers)
	for i := 0; i < numLoggers; i++ {
		uploadLogger, err := gologging.StartS3Logger(
			gologging.NewRotateCoordinator(config.MaxLines, maxAge),
			loggingInfo,
			successNotifier,
			s3UploaderFactory,
			errorNotifier,
			2,
		)

		if err != nil {
			return nil, err
		}

		logger.Go(func() {
			for s := range messages {
				uploadLogger.Log(s)
			}
			wg.Done()
		})

		uploadLoggers = append(uploadLoggers, uploadLogger)
	}

	s3l := &s3Logger{
		uploadLoggers:     uploadLoggers,
		eventToStringFunc: printFunc,
		messages:          messages,
		wg:                wg,
	}
	return s3l, nil
}

func (s3l *s3Logger) Log(e *spade.Event) error {
	s, err := s3l.eventToStringFunc(e)
	if err != nil {
		return err
	}
	s3l.messages <- s
	return nil
}

func (s3l *s3Logger) Close() {
	close(s3l.messages)
	s3l.wg.Wait()
	for _, u := range s3l.uploadLoggers {
		u.Close()
	}
}
