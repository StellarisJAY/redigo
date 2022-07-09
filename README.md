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
| key      | TTL, PTTL, EXPIRE, PERSIST, DEL, EXISTS, TYPE, KEYS, RENAME, RENAMENX, MOVE, RANDOMKEY |
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
:~$ redis-benchmark -n 500000 -r 500000 -q -t set,get,lpush,lpop,rpush,rpop,lrange_100,lrange_300,hset,sadd,zadd -p 6380
SET: 113096.59 requests per second
GET: 112714.16 requests per second
LPUSH: 116441.54 requests per second
RPUSH: 117952.35 requests per second
LPOP: 117398.45 requests per second
RPOP: 119360.23 requests per second
SADD: 110913.93 requests per second
HSET: 108601.21 requests per second
LPUSH (needed to benchmark LRANGE): 113481.61 requests per second
LRANGE_100 (first 100 elements): 49603.18 requests per second
LRANGE_300 (first 300 elements): 19219.68 requests per second

```

原版Redis：

```
:~$ redis-benchmark -n 500000 -r 500000 -q -t set,get,lpush,lpop,rpush,rpop,lrange_100,lrange_300,hset,sadd,zadd -p 6379
SET: 158478.61 requests per second
GET: 159846.55 requests per second
LPUSH: 162495.94 requests per second
RPUSH: 159134.31 requests per second
LPOP: 154655.11 requests per second
RPOP: 156250.00 requests per second
SADD: 157977.89 requests per second
HSET: 156445.55 requests per second
LPUSH (needed to benchmark LRANGE): 150015.00 requests per second
LRANGE_100 (first 100 elements): 76651.85 requests per second
LRANGE_300 (first 300 elements): 25897.34 requests per second
```
