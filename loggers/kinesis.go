package loggers

import (
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/sendgridlabs/go-kinesis"
	"github.com/sendgridlabs/go-kinesis/batchproducer"
	"github.com/twitchscience/scoop_protocol/spade"
)

type kinesisLogger struct {
	client    *kinesis.Kinesis
	producer  batchproducer.Producer
	channel   chan *spade.Event
	errors    chan error
	waitGroup *sync.WaitGroup
	stats     *kinesisStats
	fallback  SpadeEdgeLogger
}

// KinesisLoggerConfig is used to configure a new SpadeEdgeLogger that writes to
// an AWS Kinesis stream
type KinesisLoggerConfig struct {
	Region               string
	StreamName           string
	BatchSize            int
	BufferSize           int
	FlushInterval        string
	MaxAttemptsPerRecord int
}

// NewKinesisLogger creates a new SpadeEdgeLogger that writes to an AWS Kinesis stream
func NewKinesisLogger(config KinesisLoggerConfig, fallback SpadeEdgeLogger) (SpadeEdgeLogger, error) {
	flushInterval, err := time.ParseDuration(config.FlushInterval)
	if err != nil {
		return nil, fmt.Errorf("error parsing %s as a time.Duration: %v", config.FlushInterval, err)
	}

	auth, err := kinesis.NewAuthFromMetadata()
	if err != nil {
		auth, err = kinesis.NewAuthFromEnv()
		if err != nil {
			return nil, err
		}
	}

	stats := &kinesisStats{}
	client := kinesis.New(auth, config.Region)
	producerConfig := batchproducer.Config{
		AddBlocksWhenBufferFull: true,
		BufferSize:              config.BufferSize,
		FlushInterval:           flushInterval,
		BatchSize:               config.BatchSize,
		MaxAttemptsPerRecord:    config.MaxAttemptsPerRecord,
		Logger:                  log.New(os.Stderr, "", log.LstdFlags),
		StatReceiver:            stats,
		StatInterval:            1 * time.Second,
	}
	producer, err := batchproducer.New(client, config.StreamName, producerConfig)
	if err != nil {
		return nil, err
	}
	waitGroup := &sync.WaitGroup{}

	err = producer.Start()
	if err != nil {
		return nil, fmt.Errorf("Failed to start the kinesis producer: %v", err)
	}

	channel := make(chan *spade.Event)
	errors := make(chan error)

	kl := &kinesisLogger{
		client:    client,
		producer:  producer,
		channel:   channel,
		errors:    errors,
		waitGroup: waitGroup,
		stats:     stats,
		fallback:  fallback,
	}

	kl.start()

	return kl, nil
}

func (kl *kinesisLogger) logToKinesis(e *spade.Event) error {
	data, err := spade.Marshal(e)
	if err != nil {
		return fmt.Errorf("error marshaling spade event: %v", err)
	}

	err = kl.producer.Add(data, e.Uuid)
	if err != nil {
		return fmt.Errorf("error submitting event to kinesis producer: %v", err)
	}

	return nil
}

func (kl *kinesisLogger) logToFallback(e *spade.Event) error {
	err := kl.fallback.Log(e)
	if err != nil {
		return fmt.Errorf("error logging to fallback logger %v", err)
	}
	return nil
}

func (kl *kinesisLogger) start() {
	go func() {
		kl.waitGroup.Add(1)
		defer kl.waitGroup.Done()

		defer func() {
			_, _, err := kl.producer.Flush(time.Second, false)
			if err != nil {
				log.Println("Error flushing kinesis producer", err)
			}
		}()

		defer close(kl.errors)

		for e := range kl.channel {
			err := kl.logToKinesis(e)
			if err != nil {
				log.Println(err)
				kl.errors <- err

				err = kl.logToFallback(e)
				if err != nil {
					log.Println(err)
					kl.errors <- err
				}

				// Only accept one error
				return
			}
		}
	}()
}

func (kl *kinesisLogger) Log(e *spade.Event) error {
	select {
	case err, ok := <-kl.errors:
		if ok {
			return err
		}
		return errors.New("Processing halted")

	case kl.channel <- e:
	}

	return nil
}

func (kl *kinesisLogger) Close() {
	kl.fallback.Close()

	close(kl.channel)
	kl.waitGroup.Wait()
	kl.stats.log()

	err := kl.producer.Stop()
	if err != nil {
		log.Printf("Error stopping kinesis producer: %v", err)
	}
}
