package k_writer

import (
	"github.com/shopify/sarama"
)

// SimpleProducer publishes Kafka messages. It routes messages to the correct broker, refreshing metadata as appropriate,
// and parses responses for errors. You must call Close() on a producer to avoid leaks, it may not be garbage-collected automatically when
// it passes out of scope (this is in addition to calling Close on the underlying client, which is still necessary).
type Producer struct {
	producer        *sarama.Producer
	topic           string
	newExpectations chan *producerExpect
}

type producerExpect struct {
	msg    *sarama.MessageToSend
	result chan error
}

// NewSimpleProducer creates a new SimpleProducer using the given client, topic and partitioner. If the
// partitioner is nil, messages are partitioned by the hash of the key
// (or randomly if there is no key).
func NewProducer(client *sarama.Client, topic string, config *sarama.ProducerConfig) (*Producer, error) {
	if topic == "" {
		return nil, sarama.ConfigurationError("Empty topic")
	}

	config.AckSuccesses = true

	prod, err := sarama.NewProducer(client, config)
	if err != nil {
		return nil, err
	}

	sp := &Producer{
		producer:        prod,
		topic:           topic,
		newExpectations: make(chan *producerExpect), // this must be unbuffered
	}

	return sp, nil
}

func (p *Producer) SendMessage(key, value sarama.Encoder) error {
	msg := &sarama.MessageToSend{Topic: p.topic, Key: key, Value: value}
	expectation := &producerExpect{msg: msg, result: make(chan error)}
	p.newExpectations <- expectation
	p.producer.Input() <- msg

	return <-expectation.result
}

func (p *Producer) MatchResponses() {
	newExpectations := p.newExpectations
	unmatched := make(map[*sarama.MessageToSend]chan error)
	unmatched[nil] = nil // prevent it from emptying entirely

	for len(unmatched) > 0 {
		select {
		case expectation, ok := <-newExpectations:
			if !ok {
				delete(unmatched, nil) // let us exit when we've processed the last message
				newExpectations = nil  // prevent spinning on a closed channel until that happens
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

func (p *Producer) Close() error {
	close(p.newExpectations)
	return p.producer.Close()
}
