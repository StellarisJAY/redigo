package main

import (
	"fmt"
	"log"
	"os"
	"redigo/cluster"
	"redigo/config"
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
	if len(os.Args) > 1 {
		config.LoadConfigs(os.Args[1])
	} else {
		config.LoadConfigs("./redis.conf")
	}
	db := database.NewMultiDB(config.Properties.Databases, 1024)
	peer := cluster.NewCluster(db, config.Properties.Address, config.Properties.Peers)
	server := tcp.NewServer(":"+config.Properties.Port, peer)
	err := server.Start()
	if err != nil {
		panic(err)
	}
}
