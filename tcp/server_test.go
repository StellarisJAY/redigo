package tcp

import (
	"redigo/database"
	"testing"
)

func Test_Server(t *testing.T) {
	db := database.NewSingleDB(0, 2<<20)
	server := NewServer(":6380", db)
	err := server.Start()
	if err != nil {
		panic(err)
	}
}
