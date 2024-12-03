package main

import (
	"fmt"
	"github.com/alecthomas/kong"
	konghcl "github.com/alecthomas/kong-hcl/v2"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/tekusers/tekusers"
	"os"
	"strings"
)

type Arguments struct {
	User tekusers.UserCommand `cmd:"" help:"create, update or delete a user"`
}

func main() {
	if err := run(); err != nil {
		fmt.Printf("%v", err)
	}
}

func run() error {
	defer common.TektitePanicHandler()
	args := os.Args[1:]
	_, ctx, err := parseArgs(args)
	if err != nil {
		fmt.Printf("%v", err)
		return nil
	}
	if err := ctx.Run(); err != nil {
		fmt.Printf("%v", err)
	}
	return nil
}

func parseArgs(args []string) (*Arguments, *kong.Context, error) {
	cfg := &Arguments{}
	parser, err := kong.New(cfg, kong.Configuration(konghcl.Loader))
	if err != nil {
		return nil, nil, err
	}
	// Remove any empty args - as this can otherwise cause parser to fail with a non-descriptive error, and users often
	// have an extra space at end of command line
	var args2 []string
	for _, arg := range args {
		arg = strings.TrimSpace(arg)
		if arg != "" {
			args2 = append(args2, arg)
		}
	}
	ctx, err := parser.Parse(args2)
	if err != nil {
		return nil, nil, err
	}
	return cfg, ctx, nil
}
