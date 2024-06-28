// Copyright 2024 The Tektite Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"github.com/alecthomas/kong"
	konghcl "github.com/alecthomas/kong-hcl/v2"
	"github.com/spirit-labs/tektite/cli"
	"github.com/spirit-labs/tektite/cmd/tektite/commands"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/errors"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/tekclient"
	"os"
)

type arguments struct {
	Address   string              `help:"Address of tektite server to connect to." default:"127.0.0.1:7770"`
	TLSConfig tekclient.TLSConfig `help:"TLS client configuration" embed:"" prefix:""`
	Command   string              `help:"Single command to execute, non interactively"`
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("%+v\n", err)
	}
}

func run() error {
	defer common.PanicHandler()
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
		return errors.WithStack(err)
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
