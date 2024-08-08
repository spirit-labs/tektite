#!/bin/zsh

protoc -I . --go_out=. remotingmsgs.proto