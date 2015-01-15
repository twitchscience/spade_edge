package k_writer

import (
	"log"
	"sync"
	"time"

	"github.com/shopify/sarama"
	env "github.com/twitchscience/aws_utils/environment"
	"github.com/twitchscience/scoop_protocol/spade"
	"github.com/twitchscience/spade_edge/request_handler"
)

const (
	BYPASS_THRESHOLD = 15
	ERROR_BUFFER     = 100000
)

type KWriter struct {
	Producer       *sarama.Producer
	Topic          string
	stopped        chan bool
	sendChan       chan *spade.Event
	errorsOccurred int
}

func NewKWriter(clientId string, brokers []string) (request_handler.SpadeEdgeLogger, error) {
	c, err := sarama.NewClient(clientId, brokers, sarama.NewClientConfig())
	if err != nil {
		return nil, err
	}
	p, err := sarama.NewProducer(c, &sarama.ProducerConfig{
		Partitioner:    sarama.NewRoundRobinPartitioner,
		RequiredAcks:   sarama.WaitForLocal,
		FlushFrequency: 50 * time.Millisecond,
		FlushByteCount: 4096,
	})
	if err != nil {
		return nil, err
	}
	k := &KWriter{
		Producer:       p,
		Topic:          "spade-edge-" + env.GetCloudEnv(),
		stopped:        make(chan bool),
		sendChan:       make(chan *spade.Event, ERROR_BUFFER),
		errorsOccurred: 0,
	}
	go k.Listen()
	return k, nil
}

func (l *KWriter) Listen() {
	var notifyFuseBroke sync.Once
	for {
		select {
		case e := <-l.Producer.Errors():
			if e == nil {
				continue
			}
			l.errorsOccurred++
			log.Printf("Got Error while sending message: %+v\n", e)
		case event, ok := <-l.sendChan:
			// Were going to use a ghetto circuit breaker (aka a fuse) until we have a real impl...
			if !ok {
				break
			}
			if l.errorsOccurred > BYPASS_THRESHOLD {
				notifyFuseBroke.Do(func() {
					log.Println("Fuse Broke!")
				})
				continue
			}
			c, err := spade.Marshal(event)
			if err != nil {
				l.errorsOccurred++
				log.Printf("Got Error: %v\n", err)
			}
			l.Producer.Input() <- &sarama.MessageToSend{
				Topic: l.Topic,
				Key:   sarama.StringEncoder(event.Uuid),
				Value: sarama.ByteEncoder(c),
			}
			if err != nil {
				l.errorsOccurred++
				log.Printf("Got Error: %v\n", err)
			}
		}
	}
	l.stopped <- true
}

func (l *KWriter) Log(e *spade.Event) error {
	l.sendChan <- e
	return nil
}

func (l *KWriter) Close() {
	// Drain send Channel
	close(l.sendChan)
	<-l.stopped
	l.Producer.Close()
}
