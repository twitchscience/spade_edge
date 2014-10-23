package main

import "flag"

var (
	topic string
)

func init() {
	flag.StringVar(&topic, "topic", "", "What topic are we measuring latency on")

	flag.Parse()
}

func main() {

}
