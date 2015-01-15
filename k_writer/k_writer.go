package k_writer

import (
	"github.com/shopify/sarama"
	env "github.com/twitchscience/aws_utils/environment"
	"github.com/twitchscience/scoop_protocol/spade"
	"github.com/twitchscience/spade_edge/request_handler"

	"github.com/afex/hystrix-go/hystrix"
)

const (
	HystrixCommandName = "KafkaEdgeLog"
)

type KWriter struct {
	Producer *sarama.SimpleProducer
}

func GetTopic() string {
	return "spade-edge-" + env.GetCloudEnv()
}

func NewKWriter(clientId string, brokers []string) (request_handler.SpadeEdgeLogger, error) {
	c, err := sarama.NewClient(clientId, brokers, sarama.NewClientConfig())
	if err != nil {
		return nil, err
	}

	p, err := sarama.NewSimpleProducer(c, GetTopic(), sarama.NewRoundRobinPartitioner)
	if err != nil {
		return nil, err
	}

	k := &KWriter{
		Producer: p,
	}
	return k, nil
}

func (l *KWriter) Log(e *spade.Event) error {
	c, err := spade.Marshal(event)
	if err != nil {
		return err
	}
	hystrix.Go(HystrixCommandName, func() error {
		return l.Producer.SendMessage(sarama.StringEncoder(e.Uuid), sarama.ByteEncoder(c))
	}, nil)
}

func (l *KWriter) Close() {
	l.Producer.Close()
}
