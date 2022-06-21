package database

import "redigo/interface/database"

/*
	Entry holds a data of a key
*/
type Entry struct {
	Data     interface{}
	expireAt int64
}
type MultiDB struct {
	dbSet []database.DB
}
