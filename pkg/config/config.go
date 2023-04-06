package config

import (
	"flag"
	"github.com/ghodss/yaml"
	"io/ioutil"
	"os"
	"redigo/pkg/util/log"
	"strings"
)

type ServerProperties struct {
	Databases         int      `yaml:"databases"`
	AppendOnly        bool     `yaml:"appendOnly"`
	UseScheduleExpire bool     `yaml:"useScheduleExpire"`
	AppendFsync       string   `yaml:"appendFsync"`
	AofFileName       string   `yaml:"aofFileName"`
	MaxMemory         int64    `yaml:"maxMemory"`
	DBFileName        string   `yaml:"dbFileName"`
	Address           string   `yaml:"address"`
	EnableClusterMode bool     `yaml:"enableClusterMode"`
	Peers             []string `yaml:"peers"`
	Self              string   `yaml:"self"`
	DebugMode         bool     `yaml:"debugMode"`
	RdbThreshold      int      `yaml:"rdbThreshold"`
	RdbTime           int      `yaml:"rdbTime"`
}

var Properties *ServerProperties

const (
	FsyncEverySec = "everysec"
	FsyncNo       = "no"

	AppendOnlyOn     = "on"
	AppendOnlyOff    = "off"
	EvictAllLRU      = "all-lru"
	EvictVolatileLRU = "volatile-lru"
)

func init() {
	Properties = &ServerProperties{
		Databases:         16,
		AppendOnly:        false,
		UseScheduleExpire: false,
		AppendFsync:       FsyncNo,
		AofFileName:       "appendonly.aof",
		DBFileName:        "dump.rdb",
		MaxMemory:         -1,
		EnableClusterMode: false,
		Peers:             []string{},
		Self:              "127.0.0.1:16380",
		DebugMode:         true,
	}
	var appendOnly string
	flag.IntVar(&Properties.Databases, "databases", 16, "count of databases")
	flag.StringVar(&appendOnly, "appendonly", "off", "enable aof")
	flag.StringVar(&Properties.AofFileName, "appendFilename", "appendonly.aof", "aof filename")
	flag.StringVar(&Properties.DBFileName, "dbFileName", "dump.rdb", "RDB filename")
	flag.Int64Var(&Properties.MaxMemory, "maxMemory", -1, "max memory option")
	flag.BoolVar(&Properties.EnableClusterMode, "clusterMode", false, "enable cluster")
	flag.BoolVar(&Properties.DebugMode, "debugMode", false, "enable debug mode")
	flag.StringVar(&Properties.Address, "address", "0.0.0.0:6381", "redigo server address")
	configFileName := flag.String("config", "./redis.yaml", "custom config filename")
	flag.Parse()
	Properties.AppendOnly = strings.ToLower(appendOnly) == AppendOnlyOn
	loadConfigs(*configFileName)
	if Properties.DebugMode {
		log.SetLevel(log.LevelDebug)
	} else {
		log.SetLevel(log.LevelError)
	}
}

func parseYAML(file *os.File) *ServerProperties {
	configs := Properties
	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		panic(err)
	}
	err = yaml.Unmarshal(bytes, configs)
	if err != nil {
		panic(err)
	}
	return configs
}

func loadConfigs(configFilePath string) {
	file, err := os.Open(configFilePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	Properties = parseYAML(file)
}

func DisplayConfigs() {
	if Properties.AppendOnly {
		log.Info("append-only enabled, fsync: %s, aof file: %s", Properties.AppendFsync, Properties.AofFileName)
	} else {
		log.Info("append-only off")
	}
	log.Info("RDB file name: %s", Properties.DBFileName)
	log.Info("server address: %s", Properties.Address)
}
