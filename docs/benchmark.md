# Benchmark 测试报告

## 测试环境

因为目前Redigo的服务器有基于epoll 和 go原生net库 的两种实现，所有以下分linux和windows两个平台进行测试。 测试将于原版的Redis进行对比，具体的测试环境如下。

### windows

- CPU：Intel Core i7-8750H 2.2GHz
- 内存：16GB
- Redis版本：Redis 5.0.9 (9414ab9b/0) 64 bit

### linux

- CPU：Intel(R) Xeon(R) Platinum 8255C CPU @ 2.50GHz
- 内存：4 GB
- 操作系统：Ubuntu 18.04.6 LTS
- Redis版本：Redis 7.0.3 64 bit

## Linux

### 测试用例1

1K 连接，500K次请求，100K随机key，get和set命令

```
redis-benchmark -c 1000 -n 500000 -r 100000 -t set,get -p 6381
```

**Redigo 测试结果**

```
Summary:
  throughput summary: 56908.71 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
       11.508     7.248     9.239    19.087    27.343    42.847
Summary:
  throughput summary: 58534.30 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        9.286     2.296     8.447    14.207    21.663    41.663
```

**Redis 测试结果**

```
Summary:
  throughput summary: 57783.43 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        8.748     2.576     8.639     9.471    11.839    32.559
Summary:
  throughput summary: 56593.09 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        8.892     2.648     8.751     9.983    10.959    21.471

```

### 测试用例2

200K次请求，100K随机key，测试命令：get,set,lpush,lpop,hset,hget,sadd,zadd,lrange

```
redis-benchmark -n 200000 -r 100000 -t set,get,lpush,lpop,hset,sadd,zadd,lrange -p 6381
```

**Redigo测试结果：**

```
SET: 61143.38 requests per second, p50=0.703 msec                   
GET: 64913.99 requests per second, p50=0.567 msec                   
LPUSH: 64683.05 requests per second, p50=0.527 msec                   
LPOP: 66423.12 requests per second, p50=0.423 msec                   
SADD: 63979.53 requests per second, p50=0.567 msec                   
HSET: 61500.61 requests per second, p50=0.663 msec                   
ZADD: 39880.36 requests per second, p50=1.223 msec                   
LPUSH (needed to benchmark LRANGE): 64766.84 requests per second, p50=0.487 msec                   
LRANGE_100 (first 100 elements): 26504.11 requests per second, p50=1.167 msec                   
LRANGE_300 (first 300 elements): 11647.55 requests per second, p50=1.727 msec                   
LRANGE_500 (first 500 elements): 7492.04 requests per second, p50=2.583 msec                  
LRANGE_600 (first 600 elements): 6298.42 requests per second, p50=2.879 msec 
```

**Redis测试结果：**

```
SET: 69783.67 requests per second, p50=0.367 msec                   
GET: 67842.61 requests per second, p50=0.375 msec                   
LPUSH: 67727.73 requests per second, p50=0.375 msec                   
LPOP: 68681.32 requests per second, p50=0.367 msec                   
SADD: 68681.32 requests per second, p50=0.367 msec                   
HSET: 69686.41 requests per second, p50=0.367 msec                   
ZADD: 69180.21 requests per second, p50=0.503 msec                   
LPUSH (needed to benchmark LRANGE): 68329.34 requests per second, p50=0.375 msec                   
LRANGE_100 (first 100 elements): 36798.53 requests per second, p50=0.695 msec                   
LRANGE_300 (first 300 elements): 14501.16 requests per second, p50=1.719 msec                   
LRANGE_500 (first 500 elements): 9917.68 requests per second, p50=2.503 msec                   
LRANGE_600 (first 600 elements): 8540.80 requests per second, p50=2.895 msec
```

