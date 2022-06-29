package main

import (
	"fmt"
	"log"
	"os"
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

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
}

func main() {
	fmt.Println(banner)
	configs := parseArgs()
	db := database.NewMultiDB(16, 1024)

	var server *tcp.Server
	if p, ok := configs["-p"]; ok {
		server = tcp.NewServer(":"+p, db)
	} else {
		server = tcp.NewServer(":6380", db)
	}
	err := server.Start()
	if err != nil {
		panic(err)
	}
}

func parseArgs() (configs map[string]string) {
	args := os.Args[1:]
	configs = make(map[string]string)
	length := len(args)
	for i := 0; i < length; {
		if i <= length-2 {
			configs[args[i]] = args[i+1]
			i += 2
		} else {
			break
		}
	}
	return
}
