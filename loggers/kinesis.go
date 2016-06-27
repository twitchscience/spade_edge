package loggers

import (
	"bytes"
	"compress/flate"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/cactus/go-statsd-client/statsd"
	"github.com/twitchscience/aws_utils/logger"
	"github.com/twitchscience/scoop_protocol/spade"
)

const (
	maxBatchLength          = 500
	maxBatchSize            = 5 * 1024 * 1024
	compressionVersion byte = 1
)

var (
	kinesisStatsPrefix = "logger.kinesis."
)

// KinesisLoggerConfig is used to configure a new SpadeEdgeLogger that writes to
// an AWS Kinesis stream. There are no default values and all fields are required.
type KinesisLoggerConfig struct {
	// StreamName is the name of the Kinesis stream we are producing events into
	StreamName string

	// BatchLength is the max amount of globs per batch sent to Kinesis
	BatchLength int

	// BatchSize is the max size in bytes per batch sent to Kinesis
	BatchSize int

	// BatchAge is the max age of the oldest glob in the batch
	BatchAge string

	// GlobLength is the max amount of events per glob
	GlobLength int

	// GlobSize is the max size in bytes per glob
	GlobSize int

	// GlobAge is the max age of the oldest record in the glob
	GlobAge string

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
	client     *kinesis.Kinesis
	incoming   chan *spade.Event
	batch      []kinesisBatchEntry
	compressed chan kinesisBatchEntry
	glob       []*spade.Event
	globSize   int
	batchSize  int
	statter    statsd.Statter
	fallback   SpadeEdgeLogger
	config     KinesisLoggerConfig
	compressor *flate.Writer
	sync.WaitGroup
}

// NewKinesisLogger creates a new SpadeEdgeLogger that writes to an AWS Kinesis stream and starts the main loop
func NewKinesisLogger(client *kinesis.Kinesis, config KinesisLoggerConfig, fallback SpadeEdgeLogger, statter statsd.Statter) (SpadeEdgeLogger, error) {
	err := config.Validate()
	if err != nil {
		return nil, err
	}

	kl := &kinesisLogger{
		client:     client,
		incoming:   make(chan *spade.Event, config.BufferLength),
		compressed: make(chan kinesisBatchEntry),
		batch:      make([]kinesisBatchEntry, 0, config.BatchLength),
		config:     config,
		fallback:   fallback,
		statter:    statter,
	}

	kl.Add(2)
	go kl.compressLoop()
	go kl.submitLoop()
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

// addToGlob adds an event to the current glob if there is space, or submits
// the current glob to be batched
func (kl *kinesisLogger) addToGlob(e *spade.Event) {
	s := len(e.Data)
	if s+kl.globSize > kl.config.GlobSize || len(kl.glob) == kl.config.GlobLength {
		kl.compress()
	}
	kl.glob = append(kl.glob, e)
	kl.globSize += s
}

func (kl *kinesisLogger) compress() {
	err := kl._compress()
	if err != nil {
		for _, e := range kl.glob {
			_ = kl.logToFallback(e)
		}
	}
	kl.glob = kl.glob[:0]
	kl.globSize = 0
}

func (kl *kinesisLogger) _compress() (err error) {
	var buffer bytes.Buffer

	if len(kl.glob) == 0 {
		return
	}

	_ = buffer.WriteByte(compressionVersion)
	kl.compressor.Reset(&buffer)

	start := time.Now()
	uncompressed, err := json.Marshal(kl.glob)
	if err != nil {
		return
	}

	_, err = kl.compressor.Write(uncompressed)
	if err != nil {
		return
	}

	err = kl.compressor.Close()
	if err != nil {
		return
	}

	_ = kl.statter.TimingDuration(kinesisStatsPrefix+"compress.duration", time.Now().Sub(start), 1)

	compressed := buffer.Bytes()
	kl.compressed <- kinesisBatchEntry{
		data:    compressed,
		distkey: kl.glob[0].Uuid,
	}

	_ = kl.statter.Inc(kinesisStatsPrefix+"compress.uncompressed_size", int64(len(uncompressed)), 1)
	_ = kl.statter.Inc(kinesisStatsPrefix+"compress.compressed_size", int64(len(compressed)), 1)

	return
}

func (kl *kinesisLogger) compressLoop() {
	globAge, _ := time.ParseDuration(kl.config.GlobAge)
	timer := time.NewTimer(globAge)

	defer kl.Done()
	defer timer.Stop()
	defer close(kl.compressed)

	// We reset kl.compressor with a buffer to write to when we compress, but
	// we need some buffer for kl.compressor.Close() to write to in the case
	// that we never ever called Reset. So we use a scratch buffer
	var scratch bytes.Buffer
	kl.compressor, _ = flate.NewWriter(&scratch, flate.BestSpeed)
	defer func() {
		err := kl.compressor.Close()
		if err != nil {
			logger.WithError(err).Error("Failed to close compressor")
		}
	}()

	defer kl.compress()

	for {
		select {
		case <-timer.C:
			kl.compress()
		case e, ok := <-kl.incoming:
			if !ok {
				return
			}
			kl.addToGlob(e)
			if len(kl.glob) == 1 {
				timer.Reset(globAge)
			}
		}
	}
}

func (kl *kinesisLogger) submitLoop() {
	batchAge, _ := time.ParseDuration(kl.config.BatchAge)
	flushTimer := time.NewTimer(batchAge)

	defer kl.Done()
	defer flushTimer.Stop()
	defer kl.flush()

	for {
		select {
		case <-flushTimer.C:
			kl.flush()
		case e, ok := <-kl.compressed:
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
	for i, e := range kl.batch {
		records[i] = &kinesis.PutRecordsRequestEntry{
			PartitionKey: aws.String(e.distkey),
			Data:         e.data,
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
			logger.WithError(err).
				WithField("attempt", attempt).
				WithField("max_attempts", kl.config.MaxAttemptsPerRecord).
				Error("PutRecords failure")
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
	logger.WithField("num_errors", len(args.Records)).
		WithField("num_attempts", len(records)).
		Error("Failed sending records to kinesis")

	// Failed records written to the fallback log. We know we can UnMarshal back into spade.Event
	// because that is what we started with. This is potentially wasteful but this should be the
	// rare case, so the code optimized for the common case
	for _, record := range args.Records {
		e, err := spade.Decompress(record.Data)
		if err != nil {
			logger.WithError(err).Error("Error calling DecompressEvent")
			continue
		}
		err = kl.logToFallback(e)
		if err != nil {
			logger.WithError(err).Error("Error logging failed kinesis event to fallback logger")
		}
	}
}

func (kl *kinesisLogger) addToChannel(e *spade.Event) error {
	select {
	case kl.incoming <- e:
		_ = kl.statter.Inc(kinesisStatsPrefix+"caller.submitted", 1, 1.0)
		return nil
	default:
		_ = kl.statter.Inc(kinesisStatsPrefix+"caller.fail.buffer_full", 1, 1.0)
		return errors.New("Channel full")
	}
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
	if err == nil {
		return nil
	}

	fallbackErr := kl.logToFallback(e)
	if fallbackErr == nil {
		return nil
	}

	return fmt.Errorf("submitting to channel failed with `%s` and fallback logger failed with `%s`", err, fallbackErr)
}

func (kl *kinesisLogger) Close() {
	close(kl.incoming)
	kl.Wait()

	kl.fallback.Close()
}
