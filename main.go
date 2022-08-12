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
		config.LoadConfigs("./redis.yaml")
	}
	db := database.NewMultiDB(config.Properties.Databases, 1024)
	var server *tcp.Server
	if config.Properties.EnableClusterMode {
		peer := cluster.NewCluster(db, config.Properties.Self, config.Properties.Peers)
		server = tcp.NewServer(config.Properties.Address, peer)
	} else {
		server = tcp.NewServer(config.Properties.Address, db)
	}
	err := server.Start()
	if err != nil {
		panic(err)
	}
}
