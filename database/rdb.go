package database

import (
	"bufio"
	"fmt"
	"os"
	"redigo/config"
	"redigo/interface/database"
	"redigo/rdb/codec"
	"time"
)

// load RDB file
func loadRDB(db *MultiDB) error {
	rdbFile, err := os.Open(config.Properties.DBFileName)
	if err != nil {
		return fmt.Errorf("open RDB file error: %v", err)
	}
	defer rdbFile.Close()
	// create a file decoder
	decoder := codec.NewDecoder(bufio.NewReader(rdbFile))

	// check rdb file header for MagicNum and Version
	if isRedis, versionOK, err := checkHeader(decoder); err != nil {
		return fmt.Errorf("rdb file read header error: %v", err)
	} else if !isRedis {
		return fmt.Errorf("not valid rdb file format")
	} else if !versionOK {
		return fmt.Errorf("unsupported rdb file version")
	}

	currentDBIndex := -1
	for {
		// read the type byte
		b, err := decoder.ReadByte()
		if err != nil {
			return fmt.Errorf("rdb read type byte error: %v", err)
		}
		switch b {
		case codec.SelectDB:
			// select database
			index, err := decoder.ReadDBIndex()
			if err != nil {
				return err
			}
			currentDBIndex = index
		case codec.ReSizeDB:
			// read current database's size
			size, _, err := decoder.ReadDBSize()
			if err != nil {
				return err
			}
			// read size amount of key value pairs
			err = readKeyValues(decoder, db, currentDBIndex, size)
			if err != nil {
				return err
			}
		case codec.EOF:
			// end of RDB file
			return nil
		}
	}
}

// read key value pairs of current database
func readKeyValues(decoder *codec.Decoder, db *MultiDB, dbIndex int, size uint64) error {
	if dbIndex >= len(db.dbSet) || dbIndex < 0 {
		return fmt.Errorf("rdb read db index error: invalid db index")
	}
	singleDB := db.dbSet[dbIndex].(*SingleDB)
	var i uint64
	for i = 0; i < size; i++ {
		// read data type
		b, err := decoder.ReadByte()
		if err != nil {
			return fmt.Errorf("rdb read key value type error: %v", err)
		}
		hasExpire := false
		var expireTime uint64
		// parse expire time
		if b == codec.ExpireTimeMs {
			hasExpire = true
			ttl, err := decoder.ReadTTL()
			if err != nil {
				return fmt.Errorf("rdb read key expire time error: %v", err)
			}
			expireTime = ttl
			b, err = decoder.ReadByte()
			if err != nil {
				return fmt.Errorf("rdb read key value type error: %v", err)
			}
		}
		var key string
		var entry *database.Entry
		switch b {
		case codec.StringType:
			k, value, err := decoder.ReadStringObject()
			if err != nil {
				return fmt.Errorf("rdb read string object error: %v", err)
			}
			entry = &database.Entry{Data: value}
			key = k
		case codec.SetType:
			k, s, err := decoder.ReadSetObject()
			if err != nil {
				return fmt.Errorf("rdb read set object error: %v", err)
			}
			entry = &database.Entry{Data: s}
			key = k
		case codec.HashType:
			k, h, err := decoder.ReadHash()
			if err != nil {
				return fmt.Errorf("rdb read hash object error: %v", err)
			}
			entry = &database.Entry{Data: h}
			key = k
		case codec.ListType:
			k, l, err := decoder.ReadListObject()
			if err != nil {
				return fmt.Errorf("rdb read list object error: %v", err)
			}
			entry = &database.Entry{Data: l}
			key = k
		case codec.SortedSetType:
			k, zs, err := decoder.ReadZSetObject()
			if err != nil {
				return fmt.Errorf("rdb read zset object error: %v", err)
			}
			entry = &database.Entry{Data: zs}
			key = k
		default:
			continue
		}
		singleDB.data.Put(key, entry)
		// set key's expire time
		if hasExpire {
			expireAt := time.UnixMilli(int64(expireTime))
			if expireAt.Before(time.Now()) {
				// remove already expired key
				singleDB.data.Remove(key)
			}
			singleDB.ExpireAt(key, &expireAt)
		}
	}
	return nil
}

func checkHeader(decoder *codec.Decoder) (bool, bool, error) {
	header := make([]byte, 9)
	err := decoder.Read(header)
	if err != nil {
		return false, false, err
	}
	magicNum := header[0:5]
	version := header[5:]
	for i, b := range magicNum {
		if b != codec.MagicNum[i] {
			return false, false, nil
		}
	}
	for i, b := range version {
		if b != codec.Version[i] {
			return true, false, nil
		}
	}
	return true, true, nil
}
