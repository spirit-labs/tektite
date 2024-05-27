PROTOS = $(shell find . -name '*.proto')
GENERATED = $(patsubst %.proto, %.pb.go, $(PROTOS))
GENERATED_DESCRIPTOR = $(patsubst %.proto, %.bin, $(PROTOS))

.PHONY: all
all: generators $(GENERATED) $(GENERATED_DESCRIPTOR)
	go build ./...

.PHONY: clean
clean:
	rm -f $(GENERATED)

%.pb.go: %.proto Makefile
	protoc --go_out=plugins=grpc:. $<
	rsync -r --remove-source-files ./github.com/spirit-labs/tektite/protos/ ./
	rm -rf ./github.com

%.bin: %.proto Makefile
	mkdir -p ./descriptors/$(dir $@)
	protoc --include_imports --descriptor_set_out=./descriptors/$@ $<

# Ensure the Go protoc generators are installed.
.PHONY: generators
generators: ../.hermit/go/bin/protoc-gen-go

../.hermit/go/bin/protoc-gen-go:
	go install github.com/golang/protobuf/protoc-gen-go@latest
