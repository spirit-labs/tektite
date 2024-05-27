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
