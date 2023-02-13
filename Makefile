export CONFIG_FILE = "./redigo.yaml"
export ARCH = "amd64"
export PLATFORM = "linux"

build:env
	@GOOS=$(PLATFORM) GOARCH=$(ARCH) CGO_ENABLE=0 \
	go build -o ./target/redigo
run:build
	@./target/redigo --config="$(CONFIG_FILE)"
env:
	@go mod tidy
