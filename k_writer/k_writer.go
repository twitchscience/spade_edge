package k_writer

import (
	"log"

	"github.com/shopify/sarama"
	"github.com/twitchscience/spade_edge/request_handler"
)

const BYPASS_THRESHOLD = 15

type KWriter struct {
	Producer       *sarama.Producer
	stop           chan bool
	sendChan       chan request_handler.EventRecord
	errorsOccurred int
}

func NewKWriter(brokers []string) (request_handler.SpadeEdgeLogger, error) {
	c, err := sarama.NewClient("edge", brokers, sarama.NewClientConfig())
	if err != nil {
		return nil, err
	}
	p, err := sarama.NewProducer(c, sarama.NewProducerConfig())
	if err != nil {
		return nil, err
	}
	k := &KWriter{
		Producer:       p,
		stop:           make(chan bool),
		sendChan:       make(chan request_handler.EventRecord),
		errorsOccurred: 0,
	}
	go k.Listen()
	return k, nil
}

func (l *KWriter) Listen() {
	for {
		select {
		case e := <-l.Producer.Errors():
			if e == nil {
				continue
			}
			l.errorsOccurred++
			log.Printf("Got Error while sending message: %+v\n", e)
		case event := <-l.sendChan:
			// Were going to use a ghetto circuit breaker (aka a fuse) until we have a real impl...
			if l.errorsOccurred > BYPASS_THRESHOLD {
				continue
			}
			err := l.Producer.QueueMessage("edge", sarama.StringEncoder(event.GetId()), event)
			if err != nil {
				l.errorsOccurred++
				log.Printf("GotError: %v\n", err)
			}
		case <-l.stop:
			return
		}
	}
}

func (l *KWriter) Log(e request_handler.EventRecord) {
	l.sendChan <- e
}

func (l *KWriter) Close() {
	l.Producer.Close()
	l.stop <- true
}
