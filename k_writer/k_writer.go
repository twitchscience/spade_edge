package k_writer

import (
	"log"
	"sync/atomic"

	"github.com/shopify/sarama"
	"github.com/twitchscience/spade_edge/request_handler"
)

const BYPASS_THRESHOLD = 15

type KWriter struct {
	Producer       *sarama.Producer
	stop           chan bool
	errorsOccurred uint32
}

func NewKWriter(brokers []string) (*KWriter, error) {
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
		errorsOccurred: uint32(0),
	}
	go k.ListenForErrors()
	return k, nil
}

func (l *KWriter) ListenForErrors() {
	for {
		select {
		case e := <-l.Producer.Errors():
			l.errorsOccurred = atomic.AddUint32(&l.errorsOccurred, uint32(1))
			log.Printf("Got Error while sending message: %+v\n", e)
		case <-l.stop:
			return
		}
	}
}

func (l *KWriter) Log(e request_handler.EventRecord) {
	// Were going to use a ghetto circuit breaker (aka a fuse) until we have a real impl...
	if l.errorsOccurred > uint32(BYPASS_THRESHOLD) || l == nil {
		return
	}
	err := l.Producer.QueueMessage("edge", sarama.StringEncoder(e.GetId()), e)
	if err != nil {
		l.errorsOccurred = atomic.AddUint32(&l.errorsOccurred, uint32(1))
		log.Printf("GotError: %v\n", err)
	}
}

func (l *KWriter) Close() {
	l.Producer.Close()
	l.stop <- true
}
