package main

import (
	"github.com/alecthomas/kong"
	konghcl "github.com/alecthomas/kong-hcl/v2"
	"github.com/spirit-labs/tektite/agent"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type arguments struct {
	Conf agent.CommandConf `embed:""`
	//AgentConf agent.Conf        `help:"agent configuration" embed:"" prefix:"conf-"`
	Log log.Config `help:"configuration for the logger" embed:"" prefix:"log-"`
}

func main() {
	if err := run(); err != nil {
		log.Errorf("failed to run agent: %v", err)
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
	_, err = parser.Parse(args)
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
	if err := ag.Start(); err != nil {
		return err
	}
	log.Infof("tektite agent is running")
	// Wait for ag to stop - and set up signal handler to cleanly stop it when SIGINT or SIGTERM occurs
	swg := sync.WaitGroup{}
	swg.Add(1)
	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
		sig := <-signals
		log.Warnf("signal: '%s' received. tektite agent will be stopped", sig.String())
		// hard stop if server Stop() hangs
		tz := time.AfterFunc(30*time.Second, func() {
			common.DumpStacks()
			log.Warn("ag stop did not complete in time. system will exit")
			swg.Done()
			os.Exit(1)
		})
		if err := ag.Stop(); err != nil {
			log.Warnf("failed to stop tektite ag: %v", err)
		}
		log.Infof("agent is stopped")
		tz.Stop()
		swg.Done()
	}()
	swg.Wait()
	return nil
}
