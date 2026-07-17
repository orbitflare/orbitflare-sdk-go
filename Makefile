.PHONY: proto build test vet tidy

# Regenerate Go code from the .proto files in protos/.
# Requires: buf, protoc-gen-go, protoc-gen-go-grpc on PATH.
proto:
	buf generate

build:
	go build ./...

vet:
	go vet ./...

test:
	go test ./... -count=1

tidy:
	go mod tidy
