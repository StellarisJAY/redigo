# RediGO

RediGO是用Go语言实现的Redis服务器。目前实现了Redis的主要数据结构、网络协议、过期、Multi、发布订阅等功能。

关键功能：

- [x] 支持string、list、hash、set、sorted_set数据结构的主要命令
- [x] key过期功能（TTL、EXPIRE），时间轮定时删除策略+惰性删除策略
- [x] 无阻塞Keys命令
- [x] Bitmap数据结构
- [x] AOF持久化（fsync：暂不支持Always）
- [x] AOF重写（BGRewriteAOF）
- [x] RDB持久化（SAVE和BGSAVE）
- [x] multi事务功能
- [x] 发布订阅功能
- [x] Geo地理位置
- [ ] 主从、哨兵
- [ ] 集群模式



## 支持的命令

| 数据结构 | 已实现                                                       |
| -------- | ------------------------------------------------------------ |
| string   | GET, SET, SETNX, INCR, DECR, INCRYBY, DECRBY, APPEND, STRLEN, SETBIT, GETBIT |
| list     | LPUSH, LPOP, RPUSH, RPOP, LRANGE, LINDEX, LLEN, LPUSHRPOP    |
| hash     | HGET, HSET, HDEL, HEXISTS, HGETALL, HKEYS, HLEN, HMGET, HSETNX, HINCRBY, HSTRLEN, HVALS |
| set      | SADD, SMEMBERS ,SISMEMBER, SRANDMEMBER, SREM, SPOP, SDIFF, SINTER, SCARD, SDIFFSTORE, SINTERSTORE, SUNION |
| zset     | ZADD, ZSCORE, ZREM, ZRANK, ZPOPMIN, ZPOPMAX, ZCARD, ZRANGE, ZRANGEBYSCORE |
| key      | TTL, PTTL, EXPIRE, PERSIST, DEL, EXISTS, TYPE, KEYS, RENAME, RENAMENX, MOVE, RANDOMKEY |
| Geo      | GEOADD, GEOPOS, GEODIST, GEOHASH, GEORADIUS, GEORADIUSBYMEMBER |
| 事务     | MULTI, EXEC, DISCARD, WATCH, UNWATCH                         |
| 发布订阅 | SUBSCRIBE, PUBLISH, PSUBSCRIBE                               |
| 服务器   | PING                                                         |
| 数据库   | SELECT, FLUSHDB, DBSIZE, BGREWRITEAOF, SAVE, BGSAVE          |



## 运行RediGO

### 1. 编译运行

运行编译脚本，获得可执行文件

```shell
# linux系统
./build-linux.sh
# Windows系统
./build-windos.bat
```

在target目录下（可执行文件目录下）创建redigo.yaml配置文件

```yaml
# 端口号
port: 6381
# 数据库数量
databases: 16
# 是否开启 AOF
appendOnly: false
# AOF文件
aofFileName: appendonly-1.log
# RDB持久化文件
dbFileName: dump.rdb
# 本机的集群地址
self: 127.0.0.1:16381
# 对客户端开放的地址
address: 127.0.0.1:6381
# 开启集群模式
enableClusterMode: true
# 集群中的其他节点
peers:
  - 127.0.0.1:16382
  - 127.0.0.1:16383
```

运行可执行文件，并指定配置文件位置

```
./redigo.exe --config redigo.yaml
```

### 2. 使用Docker运行

从Docker仓库拉取镜像

```
docker pull xxjay/redigo:latest
```

创建容器并运行，注意端口的映射

```
docker run -d --name redigo -p 6381:6381 xxjay/redigo:latest
```

## 性能测试

详细结果见 **/docs/benchmark.md**

**测试环境**（腾讯云轻量级服务器 2核4G）：

CPU：Intel(R) Xeon(R) Platinum 8255C CPU @ 2.50GHz

内存：4GB

操作系统：Ubuntu 18.04.6 LTS

### 测试结果：

原版Redis（get，set详细报告）

```
====== SET ======                                                   
  500000 requests completed in 9.06 seconds // 共50w次请求
  500 parallel clients                      // 共500个客户端
  3 bytes payload
  keep alive: 1
 throughput summary: 55187.64 requests per second // 吞吐量：5.5W/s
  latency summary (msec):
          avg       min       p50       p95       p99       max
        4.594     1.232     4.423     5.391     8.327    18.575
 
 ====== GET ======                                                   
  500000 requests completed in 9.10 seconds  
  500 parallel clients                       
  3 bytes payload
  keep alive: 1
  throughput summary: 54957.14 requests per second // 吞吐量：5.5W/s
  latency summary (msec):
          avg       min       p50       p95       p99       max
        4.588     1.272     4.471     5.543     6.103    15.471
```

Redigo（get，set详细报告）

```
====== SET ======                                                    
  500000 requests completed in 10.07 seconds // 共50w次请求
  500 parallel clients                       // 共500个客户端
  3 bytes payload
  keep alive: 1
  throughput summary: 49667.23 requests per second // 吞吐量：4.97W/s
  latency summary (msec):
          avg       min       p50       p95       p99       max
        7.440     0.640     6.247    14.279    20.671    98.815
====== GET ======                                                    
  500000 requests completed in 9.41 seconds // 共50w次请求
  500 parallel clients                      // 共500个客户端
  3 bytes payload
  keep alive: 1
  throughput summary: 53123.67 requests per second // 吞吐量：5.3W/s
  latency summary (msec):
          avg       min       p50       p95       p99       max
        6.859     0.328     5.719    13.447    19.343    96.767

```

原版Redis测试结果汇总：

```
ubuntu@VM-0-10-ubuntu:~$ redis-benchmark -n 500000 -c 500 -t set,get,lpush,lpop,sadd,zadd,hset -p 6379 -q
SET: 55747.57 requests per second, p50=4.447 msec                   
GET: 52334.10 requests per second, p50=4.759 msec                   
LPUSH: 55791.12 requests per second, p50=4.407 msec                   
LPOP: 58031.57 requests per second, p50=4.271 msec                   
SADD: 57756.73 requests per second, p50=4.279 msec                   
HSET: 56135.62 requests per second, p50=4.415 msec                   
ZADD: 56053.81 requests per second, p50=4.367 msec
```

Redigo测试结果汇总：

```
ubuntu@VM-0-10-ubuntu:~$ redis-benchmark -n 500000 -c 500 -t set,get,lpush,lpop,sadd,zadd,hset -p 6381 -q
SET: 50125.31 requests per second, p50=6.207 msec                    
GET: 52132.21 requests per second, p50=5.919 msec                    
LPUSH: 51245.26 requests per second, p50=5.951 msec                    
LPOP: 52614.96 requests per second, p50=5.895 msec                    
SADD: 50342.33 requests per second, p50=5.991 msec                    
HSET: 51109.07 requests per second, p50=5.863 msec                    
ZADD: 51615.57 requests per second, p50=6.031 msec      
```

