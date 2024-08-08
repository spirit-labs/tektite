package commands

import (
	"fmt"
	"github.com/spirit-labs/tektite/asl/cli"
	"github.com/spirit-labs/tektite/asl/errwrap"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/chzyer/readline"
)

type ShellCommand struct {
	VI bool `help:"Enable VI mode."`
}

func (c *ShellCommand) Run(cl *cli.Cli) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return errwrap.WithStack(err)
	}

	rl, err := readline.NewEx(&readline.Config{
		HistoryFile:            filepath.Join(home, ".tektite.history"),
		DisableAutoSaveHistory: true,
		VimMode:                c.VI,
	})
	if err != nil {
		return errwrap.WithStack(err)
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
				return errwrap.WithStack(err)
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
			return errwrap.WithStack(err)
		}
	}
}

func (c *ShellCommand) SendStatement(statement string, cli *cli.Cli) error {
	ch, err := cli.ExecuteStatement(statement)
	if err != nil {
		return errwrap.WithStack(err)
	}
	for line := range ch {
		fmt.Println(line)
	}
	return nil
}
