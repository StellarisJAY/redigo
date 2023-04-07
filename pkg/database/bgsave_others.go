//go:build !linux

package database

import (
	"redigo/pkg/rdb"
	"redigo/pkg/redis"
	"redigo/pkg/util/log"
)

// 非linux平台使用goroutine加快照的方式bgsave
func BGSave(db *MultiDB, command redis.Command) *redis.RespCommand {
	if !db.aofHandler.RewriteStarted.CompareAndSwap(false, true) {
		return redis.NewErrorCommand(redis.BackgroundSaveInProgressError)
	}
	startTime := time.Now()
	// 复制当前存在的entries
	snapshot := make([][]*rdb.DataEntry, config.Properties.Databases)
	for i := 0; i < config.Properties.Databases; i++ {
		size := m.dbSet[i].Len(i)
		entries := make([]*rdb.DataEntry, size)
		snapshot[i] = entries
		j := 0
		m.ForEach(i, func(key string, entry *database.Entry, expire *time.Time) bool {
			entries[j] = &rdb.DataEntry{Key: key, Value: entry.Data, ExpireTime: expire}
			j++
			return true
		})
	}
	// run save in background
	go handleBgSave(snapshot, startTime)
	return redis.NewSingleLineCommand([]byte("Background saving started"))
}

func handleBgSave(entries [][]*rdb.DataEntry, startTime time.Time) {
	// release rewrite lock
	defer m.aofHandler.RewriteStarted.Store(false)
	err := rdb.BGSave(entries)
	if err != nil {
		log.Errorf("BGSave RDB error: %v", err)
	} else {
		log.Info("BGSave RDB finished: %d ms", time.Now().Sub(startTime).Milliseconds())
	}
}