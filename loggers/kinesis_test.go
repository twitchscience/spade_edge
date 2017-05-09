package loggers

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

func TestAdvancingPartitionKey(t *testing.T) {
	distkeys := [2]string{"distkey1", "distkey2"}
	records := make([]*kinesis.PutRecordsRequestEntry, 2)
	for i := range records {
		records[i] = &kinesis.PutRecordsRequestEntry{
			PartitionKey: aws.String(distkeys[i]),
			Data:         nil,
		}
	}
	for attempt := 1; attempt <= 10; attempt++ {
		for j := range records {
			advancePartitionKey(records[j], attempt)
			if len(*records[j].PartitionKey) != len(distkeys[j])+11 {
				t.Errorf("%s not correct length", *records[j].PartitionKey)
			}
		}
	}
}
