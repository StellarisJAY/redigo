# RediGO

RediGO是用Go语言实现的Redis服务器。通过该项目学习Redis原理，并实现Redis中的各种数据结构和命令。RediGO沿用了原版Redis的单线程模型，使用单个协程处理命令避免线程安全、死锁等问题。

关键功能：

- [x] 支持string、list、hash、set、sorted_set数据结构的主要命令
- [x] key过期功能（TTL、EXPIRE），时间轮定时删除
- [ ] Bitmap数据结构
- [ ] Geo地理位置
- [ ] RDB持久化
- [ ] AOF持久化和AOF重写
- [ ] 发布订阅功能
- [ ] multi事务功能
- [ ] 并发命令处理，避免如KEYS等命令阻塞导致其他命令阻塞



## 支持的命令

| 数据结构 | 已实现                                                       |
| -------- | ------------------------------------------------------------ |
| string   | GET, SET, SETNX, INCR, DECR, INCRYBY, DECRBY, APPEND, STRLEN |
| list     | LPUSH, LPOP, RPUSH, RPOP, LRANGE, LINDEX, LLEN, LPUSHRPOP    |
| hash     | HGET, HSET, HDEL, HEXISTS, HGETALL, HKEYS, HLEN, HMGET, HSETNX, HINCRBY, HSTRLEN, HVALS |
| set      | SADD, SMEMBERS ,SISMEMBER, SRANDMEMBER, SREM, SPOP, SDIFF, SINTER, SCARD, SDIFFSTORE, SINTERSTORE, SUNION |
| zset     | ZADD, ZSCORE, ZREM, ZRANK, ZPOPMIN, ZPOPMAX, ZCARD, ZRANGE, ZRANGEBYSCORE |
| 其他     | TTL, PTTL, EXPIRE, PERSIST, DEL, EXISTS, PING, TYPE, SELECT  |



## 性能测试

测试环境（腾讯云轻量级服务器 4核4G）：

CPU：AMD EPYC 7K62 2.6GHz

内存：4GB

操作系统：Ubuntu 18.04.6 LTS

### 测试结果：

RediGO:

```
$ redis-benchmark -n 200000 -r 100000 -t get,set,lpush,lpop,hset,sadd -p 6380 -q
SET: 85360.65 requests per second
GET: 83507.30 requests per second
LPUSH: 84925.69 requests per second
LPOP: 84853.62 requests per second
SADD: 85763.29 requests per second
HSET: 83263.95 requests per second
```

原版Redis：

```
$ redis-benchmark -n 200000 -r 100000 -t get,set,lpush,lpop,hset,sadd -p 6379 -q
SET: 109649.12 requests per second
GET: 111669.46 requests per second
LPUSH: 113765.64 requests per second
LPOP: 109950.52 requests per second
SADD: 109051.26 requests per second
HSET: 113507.38 requests per second
```

