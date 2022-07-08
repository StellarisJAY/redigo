# RediGO

RediGO是用Go语言实现的Redis服务器。通过该项目学习Redis原理，并实现Redis中的各种数据结构和命令。RediGO沿用了原版Redis的单线程模型，使用单个协程处理命令避免线程安全、死锁等问题。

关键功能：

- [x] 支持string、list、hash、set、sorted_set数据结构的主要命令
- [x] key过期功能（TTL、EXPIRE），时间轮定时删除策略+惰性删除策略
- [x] 无阻塞Keys命令
- [x] Bitmap数据结构
- [x] AOF持久化（fsync：暂不支持Always）
- [x] AOF重写（BGRewriteAOF）
- [ ] RDB持久化
- [x] multi事务功能
- [x] 发布订阅功能
- [ ] LRU内存淘汰策略
- [ ] Geo地理位置
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
| key      | TTL, PTTL, EXPIRE, PERSIST, DEL, EXISTS, TYPE, KEYS          |
| 事务     | MULTI, EXEC, DISCARD, WATCH, UNWATCH                         |
| 发布订阅 | SUBSCRIBE, PUBLISH, PSUBSCRIBE                               |
| 服务器   | PING                                                         |
| 数据库   | SELECT, FLUSHDB, DBSIZE, BGREWRITEAOF                        |



## 运行RediGO

### 编译运行

运行编译脚本，获得可执行文件

```shell
# linux系统
./build-linux.sh
# Windows系统
./build-windos.bat
```

在target目录下（可执行文件目录下）创建redis.conf配置文件

```
# 端口号（默认6380）
port 6399
# 数据库数量（默认16）
databases 16

# 是否开启AOF持久化（默认关闭）
appendonly true
# AOF持久化文件名
appendfilename appendonly.aof
# aof fsync策略（暂时不支持Always）
appendfsync everysec

# 启用过期key定时删除（默认关闭，避免定时任务占用CPU）
useScheduleExpire true
```

运行target目录下的可执行文件，显示如下信息后可使用Redis客户端访问

![](https://images-1257369645.cos.ap-chengdu.myqcloud.com/redigo/redigo_start.PNG)

## 性能测试

测试环境（腾讯云轻量级服务器 4核4G）：

CPU：AMD EPYC 7K62 2.6GHz

内存：4GB

操作系统：Ubuntu 18.04.6 LTS

### 测试结果：

RediGO:

```
$ redis-benchmark -n 200000 -r 200000 -q -t set,get,incr,lpush,lpop,rpush,rpop,hset,sadd,spop,lrange_100 -p 6380
SET: 82610.49 requests per second
GET: 82068.12 requests per second
INCR: 82101.80 requests per second
LPUSH: 83507.30 requests per second
RPUSH: 83402.84 requests per second
LPOP: 84709.87 requests per second
RPOP: 83682.01 requests per second
SADD: 83194.67 requests per second
HSET: 82576.38 requests per second
SPOP: 82781.46 requests per second
LPUSH (needed to benchmark LRANGE): 82747.20 requests per second
LRANGE_100 (first 100 elements): 41493.77 requests per second
```

原版Redis：

```
$ redis-benchmark -n 200000 -r 200000 -q -t set,get,incr,lpush,lpop,rpush,rpop,hset,sadd,spop,lrange_100 -p 6379
SET: 108459.87 requests per second
GET: 108636.61 requests per second
INCR: 107816.71 requests per second
LPUSH: 106382.98 requests per second
RPUSH: 107238.60 requests per second
LPOP: 112359.55 requests per second
RPOP: 109110.75 requests per second
SADD: 108049.70 requests per second
HSET: 113442.99 requests per second
SPOP: 115473.45 requests per second
LPUSH (needed to benchmark LRANGE): 113636.37 requests per second
LRANGE_100 (first 100 elements): 62873.31 requests per second
```

