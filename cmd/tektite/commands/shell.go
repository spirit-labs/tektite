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

package commands

import (
	"fmt"
	"github.com/spirit-labs/tektite/cli"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/chzyer/readline"
	"github.com/spirit-labs/tektite/errors"
)

type ShellCommand struct {
	VI bool `help:"Enable VI mode."`
}

func (c *ShellCommand) Run(cl *cli.Cli) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return errors.WithStack(err)
	}

	rl, err := readline.NewEx(&readline.Config{
		HistoryFile:            filepath.Join(home, ".tektite.history"),
		DisableAutoSaveHistory: true,
		VimMode:                c.VI,
	})
	if err != nil {
		return errors.WithStack(err)
	}
	for {
		// Gather multi-line statement terminated by a ;
		rl.SetPrompt("tektite> ")
		var cmd []string
		for {
			line, err := rl.Readline()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				if err.Error() == "Interrupt" {
					// This occurs when CTRL-C is pressed - we should exit silently
					return nil
				}
				return errors.WithStack(err)
			}
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			cmd = append(cmd, line)
			if strings.HasSuffix(line, ";") {
				break
			}
			rl.SetPrompt("         ")
		}
		statement := strings.Join(cmd, " ")
		_ = rl.SaveHistory(statement)

		if err := c.SendStatement(statement, cl); err != nil {
			return errors.WithStack(err)
		}
	}
}

func (c *ShellCommand) SendStatement(statement string, cli *cli.Cli) error {
	ch, err := cli.ExecuteStatement(statement)
	if err != nil {
		return errors.WithStack(err)
	}
	for line := range ch {
		fmt.Println(line)
	}
	return nil
}
