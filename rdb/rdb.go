package rdb

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"redigo/config"
	"redigo/interface/database"
	"redigo/rdb/codec"
	"time"
)

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
					log.Println("RDB write key expire time error: ", ttlErr)
					return false
				}
			}
			// write key and value
			kvErr := encoder.WriteKeyValue(key, entry.Data)
			if kvErr != nil {
				log.Println("RDB write key value error: ", kvErr)
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

func writeHeader(encoder *codec.Encoder) error {
	err := encoder.Write(codec.MagicNum)
	if err != nil {
		return err
	}
	return encoder.Write(codec.Version)
}
