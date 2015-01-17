package k_writer

import (
	"log"
	"time"

	"github.com/shopify/sarama"
	env "github.com/twitchscience/aws_utils/environment"
	"github.com/twitchscience/scoop_protocol/spade"
	"github.com/twitchscience/spade_edge/request_handler"

	"github.com/afex/hystrix-go/hystrix"
)

const (
	HystrixCommandName      = "KafkaEdgeLog"
	hystrixConcurrencyLevel = 5000
)

type KWriter struct {
	Producer *Producer
}

func GetTopic() string {
	return "spade-edge-" + env.GetCloudEnv()
}

func (l *KWriter) Init() {
	go l.Producer.MatchResponses()
}

func NewKWriter(clientId string, brokers []string) (request_handler.SpadeEdgeLogger, error) {
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

	p, err := NewProducer(c, GetTopic(), config)
	if err != nil {
		return nil, err
	}

	k := &KWriter{
		Producer: p,
	}
	hystrix.SetConcurrency(HystrixCommandName, hystrixConcurrencyLevel)
	return k, nil
}

func (l *KWriter) Log(e *spade.Event) error {
	c, err := spade.Marshal(e)
	if err != nil {
		return err
	}
	hystrix.Go(HystrixCommandName, func() error {
		return l.Producer.SendMessage(sarama.StringEncoder(e.Uuid), sarama.ByteEncoder(c))
	}, func(err error) error {
		log.Printf("Got Error while sending to kafka: %s\n", err)
		return nil
	})
	return nil
}

func (l *KWriter) Close() {
	l.Producer.Close()
}
