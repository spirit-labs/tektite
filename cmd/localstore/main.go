package main

import (
	"fmt"
	"github.com/alecthomas/kong"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/objstore/dev"
	"os"
)

var CLI struct {
	ListenAddr string `help:"IP address the local store will listen on" default:"127.0.0.1:6690"`
}

func main() {
	if err := run(); err != nil {
		fmt.Println(fmt.Errorf("failed to run local store %v\n", err))
	}
}

func run() error {
	defer common.TektitePanicHandler()
	kong.Parse(&CLI)
	localStore := dev.NewDevStore(CLI.ListenAddr)
	if err := localStore.Start(); err != nil {
		return err
	}
	fmt.Printf("local store listening on %s. press any key to exit", CLI.ListenAddr)
	//goland:noinspection GoUnhandledErrorResult
	os.Stdin.Read([]byte{0})
	return localStore.Stop()
}
