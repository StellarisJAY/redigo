package config

import (
	"bufio"
	"io"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
)

type ServerProperties struct {
	Port              string `cfg:"port"`
	Databases         int    `cfg:"databases"`
	AppendOnly        bool   `cfg:"appendOnly"`
	UseScheduleExpire bool   `cfg:"useScheduleExpire"`
}

var Properties *ServerProperties

func init() {
	Properties = &ServerProperties{
		Port:              "6380",
		Databases:         16,
		AppendOnly:        false,
		UseScheduleExpire: false,
	}
}

func parse(reader io.Reader) *ServerProperties {
	configs := &ServerProperties{}
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
			num, err := strconv.ParseInt(value, 0, 64)
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

func LoadConfigs(configFilePath string) {
	file, err := os.Open(configFilePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	Properties = parse(file)
}
