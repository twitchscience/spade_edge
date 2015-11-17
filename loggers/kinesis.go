package loggers

import (
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/sendgridlabs/go-kinesis"
	"github.com/sendgridlabs/go-kinesis/batchproducer"
	"github.com/twitchscience/scoop_protocol/spade"
	"github.com/twitchscience/spade_edge/request_handler"
)

type kinesisLogger struct {
	client    *kinesis.Kinesis
	producer  batchproducer.Producer
	channel   chan []byte
	waitGroup *sync.WaitGroup
}

func NewKinesisLogger(region string, streamName string) (request_handler.SpadeEdgeLogger, error) {
	auth, err := kinesis.NewAuthFromMetadata()
	if err != nil {
		auth, err = kinesis.NewAuthFromEnv()
		if err != nil {
			return nil, err
		}
	}

	client := kinesis.New(auth, region)
	config := batchproducer.Config{
		AddBlocksWhenBufferFull: true,
		BufferSize:              10000,
		FlushInterval:           1 * time.Second,
		BatchSize:               400,
		MaxAttemptsPerRecord:    10,
		Logger:                  log.New(os.Stderr, "", log.LstdFlags),
	}
	producer, err := batchproducer.New(client, streamName, config)
	if err != nil {
		return nil, err
	}
	waitGroup := &sync.WaitGroup{}

	producer.Start()

	channel := make(chan []byte)

	ks := &kinesisLogger{
		client:    client,
		producer:  producer,
		channel:   channel,
		waitGroup: waitGroup,
	}

	ks.start()

	return ks, nil
}

func (ks *kinesisLogger) start() {
	go func() {
		ks.waitGroup.Add(1)
		defer ks.waitGroup.Done()

		r := rand.New(rand.NewSource(time.Now().UnixNano()))

		defer ks.producer.Flush(time.Second, false)

		for msg := range ks.channel {
			key := strconv.FormatUint(uint64(r.Uint32()), 16)
			err := ks.producer.Add(msg, key)
			if err != nil {
				log.Printf("start Error %v", err)
				return
			}
		}
	}()
}

func (ks *kinesisLogger) Log(e *spade.Event) error {
	c, err := spade.Marshal(e)
	if err != nil {
		return err
	}
	ks.channel <- c
	return nil
}

func (ks *kinesisLogger) Close() {
	close(ks.channel)
	ks.waitGroup.Wait()

	ks.producer.Stop()
}
