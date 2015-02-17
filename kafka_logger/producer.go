package kafka_logger

import (
	"github.com/shopify/sarama"
)

// Producer publishes Kafka messages.
// It routes messages to the correct broker, refreshing metadata as appropriate,
// and parses responses for errors. You must call Close() on a producer.
type Producer struct {
	producer        *sarama.Producer
	topic           string
	newExpectations chan *producerExpect
	client          *sarama.Client
}

type producerExpect struct {
	msg    *sarama.MessageToSend
	result chan error
}

// NewProducer creates a new Producer using the given client, topic and configuration.
func NewProducer(client *sarama.Client, topic string, config *sarama.ProducerConfig) (*Producer, error) {
	if topic == "" {
		return nil, sarama.ConfigurationError("Empty topic")
	}
	prod, err := sarama.NewProducer(client, config)
	if err != nil {
		return nil, err
	}

	sp := &Producer{
		producer:        prod,
		topic:           topic,
		newExpectations: make(chan *producerExpect), // this must be unbuffered
		client:          client,
	}

	return sp, nil
}

func (p *Producer) SendMessage(key, value sarama.Encoder) error {
	msg := &sarama.MessageToSend{Topic: p.topic, Key: key, Value: value}
	expectation := &producerExpect{msg: msg, result: make(chan error)}
	// TODO:
	// may want to pull these messages and assorted accounting from a pool.

	p.newExpectations <- expectation
	p.producer.Input() <- msg

	return <-expectation.result
}

// This is a gnarly function. It manages accounting for unacked batches queued to send to kafka.
// unmatched is a table for managing which request is outstanding. If sends and error on the
// result chan if an Error occurs.
//
// An alternative is to make a go proc for each request to handle that requests accounting.
func (p *Producer) MatchResponses() {
	newExpectations := p.newExpectations
	unmatched := make(map[*sarama.MessageToSend]chan error)
	unmatched[nil] = nil // prevent it from emptying entirely

	for len(unmatched) > 0 {
		select {
		case expectation, ok := <-newExpectations:
			if !ok {
				delete(unmatched, nil)
				newExpectations = nil
				continue
			}
			unmatched[expectation.msg] = expectation.result
		case err := <-p.producer.Errors():
			unmatched[err.Msg] <- err.Err
			delete(unmatched, err.Msg)
		case msg := <-p.producer.Successes():
			close(unmatched[msg])
			delete(unmatched, msg)
		}
	}
}

// Closes the producer and the underlying client.
func (p *Producer) Close() error {
	close(p.newExpectations)
	err := p.producer.Close()
	if err != nil {
		return err
	}
	return p.client.Close()
}
