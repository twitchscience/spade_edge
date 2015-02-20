package kafka_logger

import (
	"time"

	"github.com/shopify/sarama"
	env "github.com/twitchscience/aws_utils/environment"
	"github.com/twitchscience/scoop_protocol/spade"
	"github.com/twitchscience/spade_edge/request_handler"

	"github.com/afex/hystrix-go/hystrix"
)

const (
	hystrixCommandName      = "KafkaEdgeLog"
	hystrixConcurrencyLevel = 5000
)

// KafkaLogger wraps a Producer and provides sane defaults.
// It also exposes a log method to send messages to kafka in a
// hystrix-wrapped call.
type KafkaLogger struct {
	Producer *Producer
}

func GetTopic() string {
	return "spade-edge-" + env.GetCloudEnv()
}

// An initialize function for the KafkaLogger.
// This MUST be called prior to any calls to Log(...)
func (l *KafkaLogger) Init() {
	go l.Producer.MatchResponses()
}

// Creates a KafkaLogger for a given kafka cluster. We identify ourselves with clientId.
func NewKafkaLogger(clientId string, brokers []string) (request_handler.SpadeEdgeLogger, error) {
	c, err := sarama.NewClient(clientId, brokers, sarama.NewClientConfig())
	if err != nil {
		return nil, err
	}

	config := sarama.NewProducerConfig()
	config.Partitioner = sarama.NewRoundRobinPartitioner
	config.FlushFrequency = 500 * time.Millisecond
	config.FlushMsgCount = 1000
	// Might want to try out compression
	config.Compression = sarama.CompressionNone
	config.AckSuccesses = true

	p, err := NewProducer(c, GetTopic(), config)
	if err != nil {
		return nil, err
	}

	k := &KafkaLogger{
		Producer: p,
	}
	hystrix.ConfigureCommand(hystrixCommandName, hystrix.CommandConfig{
		Timeout:               1000,
		MaxConcurrentRequests: hystrixConcurrencyLevel,
		ErrorPercentThreshold: 10,
	})
	return k, nil
}

// Sends a spade Event to kafka.
func (l *KafkaLogger) Log(e *spade.Event) error {
	c, err := spade.Marshal(e)
	if err != nil {
		return err
	}
	hystrix.Go(hystrixCommandName, func() error {
		return l.Producer.SendMessage(sarama.StringEncoder(e.Uuid), sarama.ByteEncoder(c))
	}, nil)
	return nil
}

// Causes the writer to safely close.
func (l *KafkaLogger) Close() {
	l.Producer.Close()
}
