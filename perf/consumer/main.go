package main

import (
	"github.com/alecthomas/kong"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/perf"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("%+v\n", err)
	}
}

func run() error {
	cfg := &perf.ConsumerArgs{}
	kong.Parse(cfg)
	pc := &perf.Consumer{Args: cfg}
	return pc.Run()
}
