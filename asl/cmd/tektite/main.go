package main

import (
	"github.com/alecthomas/kong"
	konghcl "github.com/alecthomas/kong-hcl/v2"
	"github.com/spirit-labs/tektite/asl/cli"
	"github.com/spirit-labs/tektite/asl/cmd/tektite/commands"
	"github.com/spirit-labs/tektite/asl/errwrap"
	"github.com/spirit-labs/tektite/client"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"os"
)

type arguments struct {
	Address   string           `help:"Address of tektite server to connect to." default:"127.0.0.1:7770"`
	TLSConfig client.TLSConfig `help:"TLS client configuration" embed:"" prefix:""`
	Command   string           `help:"Single command to execute, non interactively"`
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("%+v\n", err)
	}
}

func run() error {
	defer common.TektitePanicHandler()
	cfg := &arguments{}
	parser, err := kong.New(cfg, kong.Configuration(konghcl.Loader))
	if err != nil {
		return err
	}
	_, err = parser.Parse(os.Args[1:])
	if err != nil {
		return err
	}
	cl := cli.NewCli(cfg.Address, cfg.TLSConfig)
	cl.SetExitOnError(true)
	if err := cl.Start(); err != nil {
		return errwrap.WithStack(err)
	}
	defer func() {
		if err := cl.Stop(); err != nil {
			log.Errorf("failed to close cli %+v", err)
		}
	}()
	shellCommand := &commands.ShellCommand{}
	if cfg.Command != "" {
		// execute single command
		return shellCommand.SendStatement(cfg.Command, cl)
	} else {
		// interactive session
		return shellCommand.Run(cl)
	}
}
