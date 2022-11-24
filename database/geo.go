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
	RegisterCommandExecutor("GEODIST", execGeoDist, -3)
	RegisterCommandExecutor("GEOHASH", execGeoHash, -2)
	RegisterCommandExecutor("GEORADIUS", execGeoRadius, -4)
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
		geoHashUint64 := geo.FormatUint64(geo.Encode(latitude, longitude))
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
			// 解码出经纬度，按照数组的形式返回给客户端
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

func execGeoDist(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	// 参数数量最多为4
	if !ValidateArgCount(command.Name(), len(args)) || len(args) > 4 {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("GEODIST"))
	}
	key := string(args[0])
	member1, member2 := string(args[1]), string(args[2])
	unitOption := "m"
	if len(args) == 4 {
		unitOption = string(args[3])
	}
	// 单位换算因子
	var unitFactor float64
	switch unitOption {
	case "m":
		unitFactor = 1
	case "km":
		unitFactor = 0.001
	case "mi":
		unitFactor = 0.00062137
	case "ft":
		unitFactor = 3.2808399
	default:
		return redis.NewErrorCommand(redis.DistanceUnitError)
	}
	sortedSet, err := getSortedSet(db, key)
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	if sortedSet == nil {
		return redis.NilCommand
	}
	if element1, ok := sortedSet.GetScore(member1); !ok {
		return redis.NilCommand
	} else if element2, ok := sortedSet.GetScore(member2); !ok {
		return redis.NilCommand
	} else {
		lat1, lng1, _ := geo.Decode(geo.FromUint64(uint64(element1.Score)))
		lat2, lng2, _ := geo.Decode(geo.FromUint64(uint64(element2.Score)))
		distance := geo.Distance(lat1, lng1, lat2, lng2) * unitFactor
		return redis.NewSingleLineCommand([]byte(fmt.Sprintf("%.4f", distance)))
	}
}

func execGeoHash(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("GEOHASH"))
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
		if element, ok := sortedSet.GetScore(string(member)); !ok {
			result[i] = redis.Encode(redis.NilCommand)
		} else {
			geoHash := geo.ToString(geo.FromUint64(uint64(element.Score)))
			result[i] = redis.Encode(redis.NewSingleLineCommand([]byte(geoHash)))
		}
	}
	return redis.NewNestedArrayCommand(result)
}

func execGeoRadius(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("GEORADIUS"))
	}
	key := string(args[0])
	// 解析经纬度数值 和 范围数值
	longitude, err := strconv.ParseFloat(string(args[1]), 64)
	if err != nil {
		return redis.NewErrorCommand(redis.ValueNotFloatError)
	}
	latitude, err := strconv.ParseFloat(string(args[2]), 64)
	if err != nil {
		return redis.NewErrorCommand(redis.ValueNotFloatError)
	}
	radius, err := strconv.ParseFloat(string(args[3]), 64)
	if err != nil {
		return redis.NewErrorCommand(redis.ValueNotFloatError)
	}
	// 经纬度不符合范围，返回坐标错误
	if (longitude < -180 || longitude > 180) || (latitude < -90 || latitude > 90) {
		return redis.NewErrorCommand(redis.CreateInvalidCoordinatePairError(longitude, latitude))
	}
	var unitFactor float64 = 1
	withCoord, withDist, withHash, _ := false, false, false, false
	if len(args) > 4 {
		// 额外参数解析
		extraArgs := args[4:]
		for _, arg := range extraArgs {
			switch string(arg) {
			// 单位换算因子
			case "m":
				unitFactor = 1
			case "km":
				unitFactor = 1000
			case "mi":
				unitFactor = 1 / 0.00062137
			case "ft":
				unitFactor = 1 / 3.2808399
			// 返回额外信息参数
			case "WITHCOORD":
				withCoord = true
			case "WITHDIST":
				withDist = true
			case "WITHHASH":
				withHash = true
			case "DESC":
				// todo 结果排序
			default:
				return redis.NewErrorCommand(redis.SyntaxError)
			}
		}
	}
	radius = radius * unitFactor
	sortedSet, err := getSortedSet(db, key)
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	if sortedSet == nil {
		return redis.NilCommand
	}
	// 获取周围的元素
	elements := geoRadius0(sortedSet, latitude, longitude, radius)
	result := make([][]byte, len(elements))
	for i, element := range elements {
		// 单个element的结果是一个嵌套的数组
		var nestedResult [][]byte
		buf := geo.FromUint64(uint64(element.Score))
		// 解码geoHash值
		lat, lng, _ := geo.Decode(buf)
		nestedResult = append(nestedResult, redis.Encode(redis.NewBulkStringCommand([]byte(element.Member))))
		if withDist {
			// 计算距离，写入结果数组
			distance := geo.Distance(latitude, longitude, lat, lng)
			nestedResult = append(nestedResult, redis.Encode(redis.NewSingleLineCommand([]byte(fmt.Sprintf("%.4f", distance)))))
		}
		if withHash {
			// 获取Base32字符串，写入结果数组
			nestedResult = append(nestedResult, redis.Encode(redis.NewBulkStringCommand([]byte(geo.ToString(buf)))))
		}
		if withCoord {
			// 格式化经纬度
			nestedResult = append(nestedResult, redis.Encode(redis.NewArrayCommand([][]byte{
				[]byte(fmt.Sprintf("%.6f", lng)),
				[]byte(fmt.Sprintf("%.6f", lat)),
			})))
		}
		res := redis.Encode(redis.NewNestedArrayCommand(nestedResult))
		result[i] = res
	}
	return redis.NewNestedArrayCommand(result)
}

func geoRadius0(sortedSet *zset.SortedSet, latitude, longitude float64, radiusMeters float64) []zset.Element {
	var result []zset.Element
	ranges := geo.AroundRadius(latitude, longitude, radiusMeters)
	for _, rg := range ranges {
		elements := sortedSet.RangeByScore(float64(rg[0]), float64(rg[1]), 0, sortedSet.Size(), false, false)
		result = append(result, elements...)
	}

	return result
}
