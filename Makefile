# config file path
export CONFIG_FILE = "./redigo.yaml"

# compile options, GOARCH and GOOS
export ARCH = "amd64"
export PLATFORM = "linux"

# benchmark options
# server port
export BENCH_PORT = "6381"
# client conns
export BENCH_CLIS = 1000
# tests per round
export BENCH_N = 300000
# random keys count
export BENCH_RAND_KEYS = 100000
# benchmark commands
export BENCH_T = "set,get,lpush,lpop,rpush,rpop,hset,zadd,sadd"
# behchmark pipeline reqs
export BENCH_PIPELINE=1
build:env
	@GOOS=$(PLATFORM) GOARCH=$(ARCH) CGO_ENABLE=0 \
	go build -o ./target/redigo
run:build
	@./target/redigo --config="$(CONFIG_FILE)"
env:
	@go mod tidy
benchmark:
	@redis-benchmark -q -p $(BENCH_PORT) -r $(BENCH_RAND_KEYS) -c $(BENCH_CLIS) -n $(BENCH_N) -t $(BENCH_T) -P $(BENCH_PIPELINE)
