package main

import (
	"fmt"
	"github.com/alecthomas/kong"
	konghcl "github.com/alecthomas/kong-hcl/v2"
	"github.com/spirit-labs/tektite/agent"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

type arguments struct {
	Conf agent.CommandConf `embed:""`
	Log  log.Config        `help:"configuration for the logger" embed:"" prefix:"log-"`
}

func main() {
	if err := run(); err != nil {
		fmt.Printf("%v", err)
	}
}

func run() error {
	defer common.TektitePanicHandler()
	cfg := arguments{}
	parser, err := kong.New(&cfg, kong.Configuration(konghcl.Loader))
	if err != nil {
		return err
	}
	args := os.Args[1:]
	// Remove any empty args - as this can otherwise cause parser to fail with a non-descriptive error, and users often
	// have an extra space at end of command line
	var args2 []string
	for _, arg := range args {
		arg = strings.TrimSpace(arg)
		if arg != "" {
			args2 = append(args2, arg)
		}
	}
	_, err = parser.Parse(args2)
	if err != nil {
		return err
	}
	if err := cfg.Log.Configure(); err != nil {
		return err
	}
	ag, err := agent.CreateAgentFromCommandConf(cfg.Conf)
	if err != nil {
		return err
	}
	// Set up signal handler to cleanly stop it when SIGINT or SIGTERM occurs
	swg := sync.WaitGroup{}
	swg.Add(1)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-signals
		fmt.Println(fmt.Sprintf("signal: '%s' received. tektite agent will stop", sig.String()))
		// hard stop if server Stop() hangs
		tz := time.AfterFunc(30*time.Second, func() {
			common.DumpStacks()
			fmt.Println("tektite agent stop did not complete in time. system will exit")
			swg.Done()
			os.Exit(1)
		})
		if err := ag.Stop(); err != nil {
			fmt.Println(fmt.Sprintf("failure in stopping tektite agent: %v", err))
		}
		fmt.Println("tektite agent has stopped")
		tz.Stop()
		swg.Done()
	}()
	if err := ag.Start(); err != nil {
		return err
	}
	if cfg.Conf.TopicName != "" {
		ag.CreateTopicWithRetry(cfg.Conf.TopicName, 100)
	}
	fmt.Println(fmt.Sprintf("started tektite agent with kafka listener:%s and internal listener:%s",
		ag.KafkaListenAddress(), ag.ClusterListenAddress()))
	swg.Wait()
	return nil
}
