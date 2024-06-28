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
	defer common.PanicHandler()
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
