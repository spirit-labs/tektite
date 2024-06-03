#!/bin/zsh

protoc -I . --go_out=. clustermsgs.proto