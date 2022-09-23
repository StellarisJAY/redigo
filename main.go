package main

import (
	"flag"
	"fmt"
	"redigo/cluster"
	"redigo/config"
	"redigo/database"
	"redigo/tcp"
	"redigo/util/log"
)

var banner = ` 
________     ____________________________ 
___  __ \__________  /__(_)_  ____/_  __ \
__  /_/ /  _ \  __  /__  /_  / __ _  / / /
_  _, _//  __/ /_/ / _  / / /_/ / / /_/ / 
/_/ |_| \___/\__,_/  /_/  \____/  \____/
                                   
                                    v1.0.0`

func init() {
	log.SetLevel(log.LevelError)
	parseConfigs()
}

func parseConfigs() {
	configFileName := flag.String("config", "./redis.yaml", "custom config filename")
	debug := flag.Bool("debug_mode", false, "enable debug mode")
	flag.Parse()
	config.LoadConfigs(*configFileName)
	if *debug {
		log.SetLevel(log.LevelDebug)
	} else {
		log.SetLevel(log.LevelError)
	}
}

func main() {
	fmt.Println(banner)
	db := database.NewMultiDB(config.Properties.Databases, 1024)
	if config.Properties.EnableClusterMode {
		log.Info("starting Redigo server in cluster mode...\n")
		peer := cluster.NewCluster(db, config.Properties.Self, config.Properties.Peers)
		server := tcp.NewServer(config.Properties.Address, peer)
		err := server.Start()
		if err != nil {
			panic(err)
		}
	} else {
		log.Info("starting Redigo server in standalone mode...")
		server := tcp.NewServer(config.Properties.Address, db)
		err := server.Start()
		if err != nil {
			panic(err)
		}
	}
}
