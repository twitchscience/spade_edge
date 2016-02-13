package loggers

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/twitchscience/aws_utils/notifier"
	"github.com/twitchscience/aws_utils/uploader"
)

var (
	version, _ = strconv.Atoi(os.Getenv("EDGE_VERSION"))
)

type DummyNotifierHarness struct {
}

func (d *DummyNotifierHarness) SendMessage(r *uploader.UploadReceipt) error {
	return nil
}

func (d *DummyNotifierHarness) SendError(error) {
}

type SQSNotifierHarness struct {
	qName    string
	version  int
	notifier *notifier.SQSClient
}

type SQSErrorHarness struct {
	qName    string
	version  int
	notifier *notifier.SQSClient
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

func BuildSQSNotifierHarness(name string) uploader.NotifierHarness {
	if len(name) > 0 {
		client := notifier.DefaultClient
		client.Signer.RegisterMessageType("edge", func(args ...interface{}) (string, error) {
			if len(args) < 2 {
				return "", errors.New("Missing correct number of args ")
			}
			return fmt.Sprintf("{\"version\":%d,\"keyname\":%q}", args...), nil
		})
		return &SQSNotifierHarness{
			qName:    name,
			version:  version,
			notifier: client,
		}
	} else {
		return &DummyNotifierHarness{}
	}
}

func BuildSQSErrorHarness(name string) uploader.ErrorNotifierHarness {
	if len(name) > 0 {
		return &SQSErrorHarness{
			qName:    name,
			notifier: notifier.DefaultClient,
		}
	} else {
		return &DummyNotifierHarness{}
	}
}
