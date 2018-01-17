package loggers

import (
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/aws_utils/notifier"
	"github.com/twitchscience/aws_utils/uploader"
)

// DummyNotifierHarness is a struct that implements the uploader.NotifierHarness
// and uploader.NotifierHarness with nop implementations.
//
// It exists because the code that uses the Harnesses doesn't accept nil for cases
// where we do not want to notify at all.
type DummyNotifierHarness struct {
}

// SendMessage - nop implementation
func (d *DummyNotifierHarness) SendMessage(r *uploader.UploadReceipt) error {
	return nil
}

// SendError - nop implementation
func (d *DummyNotifierHarness) SendError(error) {
}

// SQSNotifierHarness sends SQS noticies when a file is complete and rotated
type SQSNotifierHarness struct {
	qName    string
	version  int
	notifier *notifier.SQSClient
}

// SQSErrorHarness sends SQS messages if errors occur in the process of uploading files to s3
type SQSErrorHarness struct {
	qName    string
	notifier *notifier.SQSClient
}

// SendError writes an SQS message of type 'error' to the SQS queue stored in the SQSErrorHarness
func (s *SQSErrorHarness) SendError(er error) {
	err := s.notifier.SendMessage("error", s.qName, er)
	if err != nil {
		logger.WithError(err).WithField("sent_error", er).Error("Error sending error")
	}
}

// SendMessage writes an SQS message of type 'edge' to the SQS queue stored in the SQSNotifierHarness
func (s *SQSNotifierHarness) SendMessage(message *uploader.UploadReceipt) error {
	return s.notifier.SendMessage("edge", s.qName, s.version, message.KeyName)
}

func buildSQSErrorHarness(sqs sqsiface.SQSAPI, name string) uploader.ErrorNotifierHarness {
	if len(name) > 0 {
		return &SQSErrorHarness{
			qName:    name,
			notifier: notifier.BuildSQSClient(sqs),
		}
	}
	return nil
}
