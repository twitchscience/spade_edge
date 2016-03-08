package loggers

import (
	"log"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/sendgridlabs/go-kinesis/batchproducer"
)

type kinesisStats struct {
	statter                      statsd.Statter
	totalKinesisErrors           int
	totalRecordsSentSuccessfully int
	totalRecordsDropped          int
}

func (ks *kinesisStats) Receive(sb batchproducer.StatsBatch) {
	_ = ks.statter.Inc(kinesisStatsPrefix+"errors", int64(sb.KinesisErrorsSinceLastStat), 1.0)
	_ = ks.statter.Inc(kinesisStatsPrefix+"sent", int64(sb.RecordsSentSuccessfullySinceLastStat), 1.0)
	_ = ks.statter.Inc(kinesisStatsPrefix+"dropped", int64(sb.RecordsDroppedSinceLastStat), 1.0)

	ks.totalKinesisErrors += sb.KinesisErrorsSinceLastStat
	ks.totalRecordsSentSuccessfully += sb.RecordsSentSuccessfullySinceLastStat
	ks.totalRecordsDropped += sb.RecordsDroppedSinceLastStat
}

func (ks *kinesisStats) log() {
	log.Println("Kinesis Errors:", ks.totalKinesisErrors)
	log.Println("Records Sent:", ks.totalRecordsSentSuccessfully)
	log.Println("Records Dropped:", ks.totalRecordsDropped)
}
