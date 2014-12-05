package spade

import (
	"encoding/json"
	"net"
	"testing"
	"time"
)

var exEvent = NewEvent(
	time.Unix(1397768380, 0),
	net.ParseIP("222.222.222.222"),
	"1",
	"blag",
)

// These exist to ensure that the impact of proxying the chosen
// serialization method is negligible.
//
// Current performance behaviour suggests these pose no significant
// issue:
// $ go test -bench=. github.com/twitchscience/scoop_protocol/spade
//
// Benchmark_Marshal        500000      4495 ns/op
// Benchmark_Unmarshal      500000      6245 ns/op
// Benchmark_MarshalJSON    500000      4547 ns/op
// Benchmark_UnmarshalJSON  500000      6383 ns/op

var byteHolder []byte
var eventHolder Event

func Benchmark_Marshal(b *testing.B) {
	var d []byte
	for i := 0; i < b.N; i++ {
		d, _ = Marshal(exEvent)
	}
	byteHolder = d
}

func Benchmark_Unmarshal(b *testing.B) {
	var e Event
	var d []byte
	d, _ = Marshal(exEvent)
	for i := 0; i < b.N; i++ {
		Unmarshal(d, &e)
	}
	eventHolder = e
}

func Benchmark_MarshalJSON(b *testing.B) {
	var d []byte
	for i := 0; i < b.N; i++ {
		d, _ = json.Marshal(exEvent)
	}
	byteHolder = d
}

func Benchmark_UnmarshalJSON(b *testing.B) {
	var e Event
	var d []byte
	d, _ = Marshal(exEvent)
	for i := 0; i < b.N; i++ {
		json.Unmarshal(d, &e)
	}
	eventHolder = e
}
