package uuid

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"log"
	"strconv"
	"time"
)

// Assigner is an interface used to assign UUIDs
type Assigner interface {
	Assign() string
}

// SpadeUUIDAssigner implements the Assigner interface to create
// uuids for spade events
type SpadeUUIDAssigner struct {
	host         string
	cluster      string
	secondTicker <-chan time.Time
	assign       chan string
	count        uint64
	fixedString  string
}

// StartUUIDAssigner starts up a go routine that creates UUIDs
func StartUUIDAssigner(host string, cluster string) Assigner {
	h := md5.New()
	_, err := h.Write([]byte(host))
	if err != nil {
		log.Printf("Error writing to md5 hash: %v", err)
		return nil
	}

	host = fmt.Sprintf("%08x", h.Sum(nil)[:4])

	h = md5.New()
	_, err = h.Write([]byte(cluster))
	if err != nil {
		log.Printf("Error writing to md5 hash: %v", err)
		return nil
	}

	cluster = fmt.Sprintf("%08x", h.Sum(nil)[:4])

	a := &SpadeUUIDAssigner{
		host:         host,
		cluster:      cluster,
		secondTicker: time.Tick(1 * time.Second),
		assign:       make(chan string),
		count:        0,
		fixedString:  fmt.Sprintf("%s-%s-", host, cluster),
	}
	go a.crank()
	return a
}

func (a *SpadeUUIDAssigner) makeID(currentTimeHex, countHex string, buf *bytes.Buffer) (err error) {
	buf.Reset()
	_, err = buf.WriteString(a.fixedString)
	if err == nil {
		_, err = buf.WriteString(currentTimeHex)
	}
	if err == nil {
		_, err = buf.WriteString("-")
	}
	if err == nil {
		_, err = buf.WriteString(countHex)
	}

	return err
}

func (a *SpadeUUIDAssigner) crank() {
	currentTimeHex := strconv.FormatInt(time.Now().Unix(), 16)
	countHex := strconv.FormatUint(a.count, 16)
	buf := bytes.NewBuffer(make([]byte, 0, 34))
	err := a.makeID(currentTimeHex, countHex, buf)
	if err != nil {
		log.Printf("Error creating UUID %v", err)
	}

	for {
		select {
		case <-a.secondTicker:
			currentTimeHex = strconv.FormatInt(time.Now().Unix(), 16)
			a.count = 0
		case a.assign <- buf.String():
			a.count++
			countHex := strconv.FormatUint(a.count, 16)
			err = a.makeID(currentTimeHex, countHex, buf)
			if err != nil {
				log.Printf("Error creating UUID %v", err)
			}
		}
	}
}

// Assign a UUID from the SpadeUUIDAssigner go routine
func (a *SpadeUUIDAssigner) Assign() string {
	return <-a.assign
}
