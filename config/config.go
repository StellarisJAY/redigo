package config

import (
	"bufio"
	"github.com/ghodss/yaml"
	"io"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
)

type ServerProperties struct {
	Port              string   `cfg:"port" yaml:"port"`
	Databases         int      `cfg:"databases" yaml:"databases"`
	AppendOnly        bool     `cfg:"appendOnly" yaml:"appendOnly"`
	UseScheduleExpire bool     `cfg:"useScheduleExpire" yaml:"useScheduleExpire"`
	AppendFsync       string   `cfg:"appendfsync" yaml:"appendFsync"`
	AofFileName       string   `cfg:"appendfilename" yaml:"aofFileName"`
	MaxMemory         int64    `cfg:"maxmemory" yaml:"maxMemory"`
	MaxMemorySamples  int      `cfg:"maxmemory-samples" yaml:"maxMemorySamples"`
	EvictPolicy       string   `yaml:"evictPolicy"`
	DBFileName        string   `cfg:"dbfilename" yaml:"dbFileName"`
	Address           string   `yaml:"address"`
	EnableClusterMode bool     `yaml:"enableClusterMode"`
	Peers             []string `yaml:"peers"`
	Self              string   `yaml:"self"`
	DebugMode         bool     `yaml:"debugMode"`
}

var Properties *ServerProperties

const (
	FsyncEverySec = "everysec"
	FsyncNo       = "no"

	EvictAllLRU      = "all-lru"
	EvictVolatileLRU = "volatile-lru"
)

func init() {
	Properties = &ServerProperties{
		Port:              "6380",
		Databases:         16,
		AppendOnly:        false,
		UseScheduleExpire: false,
		AppendFsync:       FsyncNo,
		AofFileName:       "appendonly.aof",
		DBFileName:        "dump.rdb",
		MaxMemorySamples:  5,
		MaxMemory:         -1,
		EnableClusterMode: false,
		Peers:             []string{},
		EvictPolicy:       EvictAllLRU,
		Self:              "127.0.0.1:16380",
		DebugMode:         true,
	}
}

func parse(reader io.Reader) *ServerProperties {
	configs := Properties
	cfgMap := make(map[string]string)
	scanner := bufio.NewScanner(reader)
	// scan config file
	for scanner.Scan() {
		line := scanner.Text()
		// skip comments
		if len(line) > 0 && line[0] == '#' {
			continue
		}
		// get gap between key and value
		idx := strings.IndexAny(line, " ")
		if idx > 0 && idx < len(line)-1 {
			key := line[0:idx]
			value := strings.Trim(line[idx+1:], " ")
			// put key value into temp map
			cfgMap[strings.ToLower(key)] = value
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatalln(err)
	}

	t := reflect.TypeOf(configs)
	v := reflect.ValueOf(configs)
	n := t.Elem().NumField()
	for i := 0; i < n; i++ {
		// use reflection to get fields
		field := t.Elem().Field(i)
		fieldValue := v.Elem().Field(i)
		key, ok := field.Tag.Lookup("cfg")
		if !ok {
			key = field.Name
		}
		value, ok := cfgMap[strings.ToLower(key)]
		if !ok {
			continue
		}
		switch field.Type.Kind() {
		case reflect.String:
			fieldValue.SetString(value)
		case reflect.Int:
			num, err := strconv.ParseInt(value, 10, 64)
			if err == nil {
				fieldValue.SetInt(num)
			}
		case reflect.Bool:
			boolVal, err := strconv.ParseBool(value)
			if err == nil {
				fieldValue.SetBool(boolVal)
			}
		}
	}
	return configs
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

func LoadConfigs(configFilePath string) {
	file, err := os.Open(configFilePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	Properties = parseYAML(file)
}
