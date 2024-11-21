#!/bin/zsh
go run agent/cmd/main.go --obj-store-username=minioadmin --obj-store-password=miniopassword --obj-store-url=127.0.0.1:9000 --cluster-name=test-cluster --location=az1