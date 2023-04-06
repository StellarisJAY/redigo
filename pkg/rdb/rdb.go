package rdb

import (
	"fmt"
	"io/ioutil"
	"os"
	"redigo/pkg/config"
	"redigo/pkg/interface/database"
	"redigo/pkg/rdb/codec"
	"redigo/pkg/util/log"
	"time"
)

type DataEntry struct {
	Key        string
	Value      interface{}
	ExpireTime *time.Time
}

// Save generate a RDB file, write all data in memory to RDB
func Save(db database.DB) error {
	// open rdb file
	rdbFile, err := ioutil.TempFile("./", "dump-*.rdb")
	if err != nil {
		return err
	}
	defer rdbFile.Close()
	encoder := codec.NewEncoder(rdbFile)
	// write REDIS and VERSION
	err = writeHeader(encoder)
	if err != nil {
		return fmt.Errorf("rdb write header error: %v", err)
	}
	// for each single database
	for i := 0; i < config.Properties.Databases; i++ {
		// skip empty database
		if db.Len(i) == 0 {
			continue
		}
		// select DB
		err = encoder.WriteDBIndex(uint64(i))
		if err != nil {
			return fmt.Errorf("rdb write database index error: %v", err)
		}
		size := db.Len(i)
		err = encoder.WriteDBSize(uint64(size), 0)
		if err != nil {
			return fmt.Errorf("rdb write database size error: %v", err)
		}
		// for each key in current database
		db.ForEach(i, func(key string, entry *database.Entry, expire *time.Time) bool {
			// write key's expire time
			if expire != nil {
				ttlErr := encoder.WriteTTL(uint64(expire.UnixMilli()))
				if ttlErr != nil {
					log.Errorf("RDB write key expire time error: ", ttlErr)
					return false
				}
			}
			// write key and value
			kvErr := encoder.WriteKeyValue(key, entry.Data)
			if kvErr != nil {
				log.Errorf("RDB write key value error: %v", kvErr)
				return false
			}
			return true
		})
	}
	err = encoder.Write([]byte{codec.EOF})
	if err != nil {
		return fmt.Errorf("rdb write EOF error: %v", err)
	}
	rdbFile.Close()
	err = os.Rename(rdbFile.Name(), config.Properties.DBFileName)
	if err != nil {
		return fmt.Errorf("rename temp rdb file error: %v", err)
	}
	return nil
}

func BGSave(entries [][]*DataEntry) error {
	// open rdb file
	rdbFile, err := ioutil.TempFile("./", "dump-*.rdb")
	if err != nil {
		return err
	}
	defer rdbFile.Close()
	encoder := codec.NewEncoder(rdbFile)
	// write REDIS and VERSION
	err = writeHeader(encoder)
	if err != nil {
		return fmt.Errorf("rdb write header error: %v", err)
	}

	for i, entrySet := range entries {
		if entrySet != nil && len(entrySet) > 0 {
			// select DB
			err = encoder.WriteDBIndex(uint64(i))
			if err != nil {
				return fmt.Errorf("rdb write database index error: %v", err)
			}
			size := len(entrySet)
			err = encoder.WriteDBSize(uint64(size), 0)
			if err != nil {
				return fmt.Errorf("rdb write database size error: %v", err)
			}
			// write key-value pairs
			for _, entry := range entrySet {
				if entry.ExpireTime != nil {
					if entry.ExpireTime.Before(time.Now()) {
						continue
					} else {
						// write expire time
						err := encoder.WriteTTL(uint64(entry.ExpireTime.UnixMilli()))
						if err != nil {
							log.Errorf("rdb write expire time error: %v", err)
							break
						}
					}
				}
				// write entry's key value
				err := encoder.WriteKeyValue(entry.Key, entry.Value)
				if err != nil {
					log.Errorf("rdb write key value error: %v", err)
					break
				}
			}
		}
	}

	err = encoder.Write([]byte{codec.EOF})
	if err != nil {
		return fmt.Errorf("rdb write EOF error: %v", err)
	}
	rdbFile.Close()
	err = os.Rename(rdbFile.Name(), config.Properties.DBFileName)
	if err != nil {
		return fmt.Errorf("rename temp rdb file error: %v", err)
	}
	return nil
}

func writeHeader(encoder *codec.Encoder) error {
	err := encoder.Write(codec.MagicNum)
	if err != nil {
		return err
	}
	return encoder.Write(codec.Version)
}
