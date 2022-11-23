package database

import (
	"fmt"
	"redigo/datastruct/zset"
	"redigo/redis"
	"redigo/util/geo"
	"strconv"
)

func init() {
	RegisterCommandExecutor("GEOADD", execGeoAdd, -4)
	RegisterCommandExecutor("GEOPOS", execGeoPos, -2)
}

func execGeoAdd(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) || (len(args)-1)%3 != 0 {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("GEOADD"))
	}
	key := string(args[0])
	members := args[1:]
	elements := make([]*zset.Element, len(members)/3)
	for i := 0; i < len(elements); i++ {
		j := 3 * i
		longitude, err := strconv.ParseFloat(string(members[j]), 64)
		if err != nil {
			return redis.NewErrorCommand(redis.ValueNotFloatError)
		}
		latitude, err := strconv.ParseFloat(string(members[j+1]), 64)
		if err != nil {
			return redis.NewErrorCommand(redis.ValueNotFloatError)
		}
		if (longitude < -180 || longitude > 180) || (latitude < -90 || latitude > 90) {
			return redis.NewErrorCommand(redis.CreateInvalidCoordinatePairError(longitude, latitude))
		}
		geoHashUint64 := geo.FormatUint64(geo.Encode(latitude, longitude, geo.MaxPrecision))
		elements[i] = &zset.Element{
			Member: string(members[j+2]),
			Score:  float64(geoHashUint64),
		}
	}
	sortedSet, err := getOrInitSortedSet(db, key)
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	result := 0
	for _, elem := range elements {
		result += sortedSet.Add(elem.Member, elem.Score)
	}
	return redis.NewNumberCommand(result)
}

func execGeoPos(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("GEOPOS"))
	}
	key := string(args[0])
	members := args[1:]
	sortedSet, err := getSortedSet(db, key)
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	if sortedSet == nil {
		return redis.NilCommand
	}
	result := make([][]byte, len(members))
	for i, member := range members {
		element, ok := sortedSet.GetScore(string(member))
		if !ok {
			result[i] = redis.Encode(redis.NilCommand)
		} else {
			latitude, longitude, _ := geo.Decode(geo.FromUint64(uint64(element.Score)))
			reply := redis.NewArrayCommand([][]byte{
				[]byte(fmt.Sprintf("%.6f", longitude)),
				[]byte(fmt.Sprintf("%.6f", latitude)),
			})
			result[i] = redis.Encode(reply)
		}
	}
	return redis.NewNestedArrayCommand(result)
}
