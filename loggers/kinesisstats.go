package loggers

import (
	"log"

	"github.com/sendgridlabs/go-kinesis/batchproducer"
)

type kinesisStats struct {
	totalKinesisErrors           int
	totalRecordsSentSuccessfully int
	totalRecordsDropped          int
}

func (ks *kinesisStats) Receive(sb batchproducer.StatsBatch) {
	ks.totalKinesisErrors += sb.KinesisErrorsSinceLastStat
	ks.totalRecordsSentSuccessfully += sb.RecordsSentSuccessfullySinceLastStat
	ks.totalRecordsDropped += sb.RecordsDroppedSinceLastStat
}

func (ks *kinesisStats) log() {
	log.Println("KinesisErrors:", ks.totalKinesisErrors)
	log.Println("Records Sent:", ks.totalRecordsSentSuccessfully)
	log.Println("Records Dropped:", ks.totalRecordsDropped)
}
