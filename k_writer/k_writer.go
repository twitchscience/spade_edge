package k_writer

import (
	"log"
	"sync"
	"time"

	"github.com/shopify/sarama"
	env "github.com/twitchscience/aws_utils/environment"
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
	sendChan       chan request_handler.EventRecord
	errorsOccurred int
}

func NewKWriter(clientId string, brokers []string) (request_handler.SpadeEdgeLogger, error) {
	c, err := sarama.NewClient(clientId, brokers, sarama.NewClientConfig())
	if err != nil {
		return nil, err
	}
	p, err := sarama.NewProducer(c, &sarama.ProducerConfig{
		Partitioner:                &sarama.RoundRobinPartitioner{},
		RequiredAcks:               sarama.WaitForLocal,
		MaxBufferTime:              50 * time.Millisecond,
		MaxBufferedBytes:           4096,
		BackPressureThresholdBytes: 50 * 1024 * 1024,
	})
	if err != nil {
		return nil, err
	}
	k := &KWriter{
		Producer:       p,
		Topic:          "spade-edge-" + env.GetCloudEnv(),
		stopped:        make(chan bool),
		sendChan:       make(chan request_handler.EventRecord, ERROR_BUFFER),
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
			err := l.Producer.QueueMessage(l.Topic, sarama.StringEncoder(event.GetId()), event)
			if err != nil {
				l.errorsOccurred++
				log.Printf("Got Error: %v\n", err)
			}
		}
	}
	l.stopped <- true
}

func (l *KWriter) Log(e request_handler.EventRecord) {
	l.sendChan <- e
}

func (l *KWriter) Close() {
	// Drain send Channel
	close(l.sendChan)
	<-l.stopped
	l.Producer.Close()
}
