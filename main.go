package main

import (
	"fmt"
	"redigo/database"
	"redigo/tcp"
)

var banner = ` 
________     ____________________________ 
___  __ \__________  /__(_)_  ____/_  __ \
__  /_/ /  _ \  __  /__  /_  / __ _  / / /
_  _, _//  __/ /_/ / _  / / /_/ / / /_/ / 
/_/ |_| \___/\__,_/  /_/  \____/  \____/
                             v1.0-SNAPSHOT`

func main() {
	fmt.Println(banner)
	db := database.NewSingleDB(0, 102400)
	server := tcp.NewServer(":6380", db)
	err := server.Start()
	if err != nil {
		panic(err)
	}
}
