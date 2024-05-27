package main

import (
	"fmt"
	"github.com/alecthomas/kong"
	konghcl "github.com/alecthomas/kong-hcl/v2"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/errors"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/server"
	"github.com/spirit-labs/tektite/shutdown"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"sync"
	"syscall"
	"time"
)

type arguments struct {
	Config        kong.ConfigFlag `help:"Path to config file" type:"existingfile" required:""`
	Server        conf.Config     `help:"Server configuration" embed:"" prefix:""`
	Log           log.Config      `help:"Configuration for the logger" embed:"" prefix:"log-"`
	Shutdown      bool            `help:"Shut down the cluster"`
	ForceShutdown bool            `help:"Used with --shutdown flag. If true, then don't require all nodes to be contacted for shutdown to succeed"`
}

func logErrorAndExit(msg string) {
	log.Errorf(msg)
	os.Exit(1)
}

func main() {
	defer common.PanicHandler()

	r := &runner{}

	cfg, err := r.loadConfig(os.Args[1:])
	if err != nil {
		logErrorAndExit(err.Error())
	}

	var cpuFile *os.File
	if cfg.Server.CPUProfileEnabled {
		cpuFile, err = os.Create("cpu-profile.out")
		if err != nil {
			logErrorAndExit(err.Error())
		}
		if err := pprof.StartCPUProfile(cpuFile); err != nil {
			logErrorAndExit(err.Error())
		}
		defer func() {
			pprof.StopCPUProfile()
			if err := cpuFile.Close(); err != nil {
				logErrorAndExit(err.Error())
			}
		}()
	}

	if cfg.Server.MemProfileEnabled {
		defer func() {
			memProfileFile, err := os.Create("mem-profile.out")
			if err != nil {
				logErrorAndExit(err.Error())
			}
			runtime.GC()
			if err := pprof.WriteHeapProfile(memProfileFile); err != nil {
				logErrorAndExit(err.Error())
			}
			if err := memProfileFile.Close(); err != nil {
				logErrorAndExit(err.Error())
			}
		}()
	}

	if cfg.Shutdown {
		if err := shutdown.PerformShutdown(&cfg.Server, cfg.ForceShutdown); err != nil {
			log.Errorf(err.Error())
		}
		return
	}

	stopWG := sync.WaitGroup{}
	stopWG.Add(1)

	if err := r.run(&cfg.Server, true, &stopWG); err != nil {
		logErrorAndExit(err.Error())
	}

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
		sig := <-signals
		log.Warnf("signal: %s received. tektite server will be closed", sig.String())
		// hard stop if server Stop() hangs
		tz := time.AfterFunc(5*time.Second, func() {
			log.Warn("server.Stop() did not complete in time. system will exit.")
			os.Exit(1)
		})
		if err := r.server.Stop(); err != nil {
			log.Warnf("failure in stopping tektite server: %v", err)
		}
		tz.Stop()
	}()

	stopWG.Wait()

	if r.server.WasShutdown() {
		log.Infof("tektite server was shutdown cleanly")
	} else {
		log.Infof("tektite server was closed without shutdown")
	}

}

type runner struct {
	server *server.Server
}

func (r *runner) loadConfig(args []string) (*arguments, error) {
	hasLogCog := false
	for _, arg := range args {
		if arg == "--logconfig" {
			hasLogCog = true
		}
	}
	var cfgString string
	for i, arg := range args {
		if arg == "--config" {
			confFile := args[i+1]
			bytes, err := os.ReadFile(confFile)
			if err != nil {
				return nil, err
			}
			cfgString = string(bytes)
			if hasLogCog {
				// We log the config file to stdout to help in debugging config related issues, we don't use the logger as a problem
				// in config could prevent this being initialised properly
				fmt.Println("Tektite config file is:")
				fmt.Println(cfgString)
			}
		}
	}
	cfg := arguments{}
	parser, err := kong.New(&cfg, kong.Configuration(konghcl.Loader))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	_, err = parser.Parse(args)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if err := cfg.Log.Configure(); err != nil {
		return nil, errors.WithStack(err)
	}
	cfg.Server.ApplyDefaults()
	cfg.Server.Original = cfgString
	if cfg.Server.ClientType != conf.KafkaClientTypeConfluent {
		panic("only Confluent client type supported on real server")
	}
	return &cfg, nil
}

func (r *runner) run(cfg *conf.Config, start bool, stopWg *sync.WaitGroup) error {
	s, err := server.NewServer(*cfg)
	if err != nil {
		return errors.WithStack(err)
	}
	r.server = s
	if start {
		s.SetStopWaitGroup(stopWg)
		if err := s.Start(); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (r *runner) getServer() *server.Server {
	return r.server
}
