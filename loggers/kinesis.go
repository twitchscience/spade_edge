package loggers

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/cactus/go-statsd-client/statsd"
	"github.com/twitchscience/scoop_protocol/spade"
)

const (
	maxBatchLength = 500
	maxBatchSize   = 5 * 1024 * 1024
	maxEventSize   = 1 * 1024 * 1024
)

var (
	kinesisStatsPrefix = "logger.kinesis."
)

// KinesisLoggerConfig is used to configure a new SpadeEdgeLogger that writes to
// an AWS Kinesis stream. There are no default values and all fields are required.
type KinesisLoggerConfig struct {
	// StreamName is the name of the Kinesis stream we are producing events into
	StreamName string

	// BatchLength is the max amount of events per batch sent to Kinesis
	BatchLength int

	// BatchSize is the max size in bytes per batch sent to Kinesis
	BatchSize int

	// BatchAge is the max age of the oldest record in the batch
	BatchAge string

	// BufferLength is the length of the buffer in front of the kinesis production code. If the buffer fills
	// up events will be written to the fallback logger
	BufferLength uint

	// MaxAttemptsPerRecord is the maximum amounts an event will be resent to Kinesis on failure
	// before it is written to the fallback logger
	MaxAttemptsPerRecord int

	// RetryDelay is how long to delay between retries on failed attempts to write to kinesis
	RetryDelay string
}

// Validate verifies that a KinesisLoggerConfig is valid, and updates any internal members
func (c *KinesisLoggerConfig) Validate() error {
	batchAge, err := time.ParseDuration(c.BatchAge)
	if err != nil {
		return fmt.Errorf("error parsing %s as a time.Duration: %v", c.BatchAge, err)
	}

	if batchAge <= 0 {
		return errors.New("BatchAge must be greater than 0")
	}

	if len(c.RetryDelay) == 0 {
		return errors.New("RetryDelay is required")
	}

	_, err = time.ParseDuration(c.RetryDelay)
	if err != nil {
		return fmt.Errorf("error parsing %s as a time.Duration: %v", c.RetryDelay, err)
	}

	if len(c.StreamName) == 0 {
		return errors.New("Stream name is required")
	}

	if c.BatchLength <= 0 || c.BatchLength > maxBatchLength {
		return fmt.Errorf("BatchLength must be between 1 and %d", maxBatchLength)
	}

	if c.BatchSize <= 0 || c.BatchSize > maxBatchSize {
		return fmt.Errorf("BatchSize must be between 1 and %d", maxBatchSize)
	}

	if c.MaxAttemptsPerRecord <= 0 {
		return errors.New("MaxAttemptsPerRecord must be a positive value")
	}

	return nil
}

type kinesisBatchEntry struct {
	data    []byte
	distkey string
}

type kinesisLogger struct {
	client    *kinesis.Kinesis
	incoming  chan kinesisBatchEntry
	batch     []kinesisBatchEntry
	batchSize int
	statter   statsd.Statter
	fallback  SpadeEdgeLogger
	config    KinesisLoggerConfig
	sync.WaitGroup
}

// NewKinesisLogger creates a new SpadeEdgeLogger that writes to an AWS Kinesis stream and starts the main loop
func NewKinesisLogger(client *kinesis.Kinesis, config KinesisLoggerConfig, fallback SpadeEdgeLogger, statter statsd.Statter) (SpadeEdgeLogger, error) {
	err := config.Validate()
	if err != nil {
		return nil, err
	}

	kl := &kinesisLogger{
		client:   client,
		incoming: make(chan kinesisBatchEntry, config.BufferLength),
		batch:    make([]kinesisBatchEntry, 0, config.BatchLength),
		config:   config,
		fallback: fallback,
		statter:  statter,
	}

	kl.Add(1)
	go kl.mainLoop()
	return kl, nil
}

// addToBatch adds an event to the current batch if there is space, or flushes
// the current batch and starts a new one.
func (kl *kinesisLogger) addToBatch(e kinesisBatchEntry) {
	s := len(e.data)
	if s+kl.batchSize > kl.config.BatchSize || len(kl.batch) == kl.config.BatchLength {
		kl.flush()
	}
	kl.batch = append(kl.batch, e)
	kl.batchSize += s
}

func (kl *kinesisLogger) mainLoop() {
	batchAge, _ := time.ParseDuration(kl.config.BatchAge)
	flushTimer := time.NewTimer(batchAge)

	defer kl.Done()
	defer flushTimer.Stop()
	defer kl.flush()

	for {
		select {
		case <-flushTimer.C:
			kl.flush()
		case e, ok := <-kl.incoming:
			if !ok {
				return
			}
			kl.addToBatch(e)
			if len(kl.batch) == 1 {
				flushTimer.Reset(batchAge)
			}
		}
	}
}

func (kl *kinesisLogger) flush() {
	if len(kl.batch) == 0 {
		return
	}

	records := make([]*kinesis.PutRecordsRequestEntry, len(kl.batch))
	for i, r := range kl.batch {
		records[i] = &kinesis.PutRecordsRequestEntry{
			PartitionKey: aws.String(r.distkey),
			Data:         r.data,
		}
	}
	kl.batch = kl.batch[:0]
	kl.batchSize = 0

	kl.Add(1)
	go kl.putRecords(records)
}

func (kl *kinesisLogger) putRecords(records []*kinesis.PutRecordsRequestEntry) {
	retryDelay, _ := time.ParseDuration(kl.config.RetryDelay)

	defer kl.Done()

	args := &kinesis.PutRecordsInput{
		StreamName: aws.String(kl.config.StreamName),
		Records:    records,
	}

	for attempt := 1; attempt <= kl.config.MaxAttemptsPerRecord; attempt++ {
		_ = kl.statter.Inc(kinesisStatsPrefix+"putrecords.attempted", 1, 1)
		_ = kl.statter.Inc(kinesisStatsPrefix+"putrecords.length", int64(len(records)), 1)

		t0 := time.Now()
		res, err := kl.client.PutRecords(args)
		_ = kl.statter.TimingDuration(kinesisStatsPrefix+"putrecords", time.Now().Sub(t0), 1)

		if err != nil {
			log.Printf("PutRecords failure attempt number %d / %d: %v", attempt, kl.config.MaxAttemptsPerRecord, err)
			_ = kl.statter.Inc(kinesisStatsPrefix+"putrecords.errors", 1, 1)
			time.Sleep(retryDelay)
			continue
		}

		// Find all failed records and update the slice to contain only failures
		i := 0
		for j, result := range res.Records {
			shard := aws.StringValue(result.ShardId)
			if shard == "" {
				shard = "unknown"
			}

			if aws.StringValue(result.ErrorCode) != "" {
				switch aws.StringValue(result.ErrorCode) {
				case "ProvisionedThroughputExceededException":
					_ = kl.statter.Inc(kinesisStatsPrefix+"records_failed.throttled", 1, 1)
					_ = kl.statter.Inc(kinesisStatsPrefix+fmt.Sprintf("byshard.%s.records_failed.throttled", shard), 1, 1)
				case "InternalFailure":
					_ = kl.statter.Inc(kinesisStatsPrefix+"records_failed.internal_error", 1, 1)
					_ = kl.statter.Inc(kinesisStatsPrefix+fmt.Sprintf("byshard.%s.records_failed.internal_error", shard), 1, 1)
				default:
					// Something undocumented
					_ = kl.statter.Inc(kinesisStatsPrefix+"records_failed.unknown_reason", 1, 1)
					_ = kl.statter.Inc(kinesisStatsPrefix+fmt.Sprintf("byshard.%s.records_failed.unknown_reason", shard), 1, 1)
				}
				args.Records[i] = args.Records[j]
				i++
			} else {
				_ = kl.statter.Inc(kinesisStatsPrefix+"records_succeeded", 1, 1)
				_ = kl.statter.Inc(kinesisStatsPrefix+fmt.Sprintf("byshard.%s.records_succeeded", shard), 1, 1)
			}
		}
		args.Records = args.Records[:i]

		if len(args.Records) == 0 {
			// No records need to be retried.
			return
		}

		time.Sleep(retryDelay)
	}

	// We ran out of retries for some records, write them to fallback logger
	log.Printf("Failed sending %d out of %d records to kinesis", len(args.Records), len(records))

	// Failed records written to the fallback log. We know we can UnMarshal back into spade.Event
	// because that is what we started with. This is potentially wasteful but this should be the
	// rare case, so the code optimized for the common case
	for _, record := range args.Records {
		e, err := spade.Decompress(record.Data)
		if err != nil {
			log.Printf("Error calling DecompressEvent: %s", err)
			continue
		}
		err = kl.logToFallback(e)
		if err != nil {
			log.Printf("Error logging failed kinesis event to fallback logger %s", err)
		}
	}
}

func (kl *kinesisLogger) addToChannel(e *spade.Event) error {
	data, err := spade.Compress(e)
	if err != nil {
		_ = kl.statter.Inc(kinesisStatsPrefix+"caller.fail.compress", 1, 1.0)
		return err
	}

	if len(data) > maxEventSize {
		_ = kl.statter.Inc(kinesisStatsPrefix+"caller.fail.maxeventsize", 1, 1.0)
		return fmt.Errorf("Event larger than Max Event Size (%d)", maxEventSize)
	}

	if len(data) > kl.config.BatchSize {
		_ = kl.statter.Inc(kinesisStatsPrefix+"caller.fail.toobigforbatch", 1, 1.0)
		return fmt.Errorf("Event larger than Batch Size (%d)", kl.config.BatchSize)
	}

	select {
	// Submit event to channel
	case kl.incoming <- kinesisBatchEntry{
		data:    data,
		distkey: e.Uuid,
	}:
		_ = kl.statter.Inc(kinesisStatsPrefix+"caller.submitted", 1, 1.0)
	default:
		_ = kl.statter.Inc(kinesisStatsPrefix+"caller.fail.buffer_full", 1, 1.0)
		return errors.New("Channel full")
	}

	return nil
}

func (kl *kinesisLogger) logToFallback(e *spade.Event) error {
	err := kl.fallback.Log(e)
	_ = kl.statter.Inc(kinesisStatsPrefix+"fallback.added", 1, 1.0)
	if err != nil {
		_ = kl.statter.Inc(kinesisStatsPrefix+"fallback.errors", 1, 1.0)
		return fmt.Errorf("error logging to fallback logger %v", err)
	}
	return nil
}

// Log will attempt to queue up an event to be published into Kinesis.
// If an error is returned, the caller should assume the event was dropped
func (kl *kinesisLogger) Log(e *spade.Event) error {
	err := kl.addToChannel(e)
	if err != nil {
		fallbackErr := kl.logToFallback(e)
		if err != nil {
			return fmt.Errorf("submitting to channel failed with `%s` and fallback logger failed with `%s`", err, fallbackErr)
		}
	}

	return nil
}

func (kl *kinesisLogger) Close() {
	close(kl.incoming)
	kl.Wait()

	kl.fallback.Close()
}
