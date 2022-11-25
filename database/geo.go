package database

import (
	"fmt"
	"redigo/datastruct/zset"
	"redigo/redis"
	"redigo/util/geo"
	"sort"
	"strconv"
	"strings"
)

type radiusOptions struct {
	unitFactor float64
	withCoord  bool
	withDist   bool
	withHash   bool
	sortAsc    bool
	sortDesc   bool
}

type radiusResult struct {
	encoded  []byte
	distance float64
}

func init() {
	RegisterCommandExecutor("GEOADD", execGeoAdd, -4)
	RegisterCommandExecutor("GEOPOS", execGeoPos, -2)
	RegisterCommandExecutor("GEODIST", execGeoDist, -3)
	RegisterCommandExecutor("GEOHASH", execGeoHash, -2)
	RegisterCommandExecutor("GEORADIUS", execGeoRadius, -4)
	RegisterCommandExecutor("GEORADIUSBYMEMBER", execGeoRadiusByMember, -3)
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
	var options radiusOptions
	if len(args) > 4 {
		// 额外参数解析
		opt, err := parseRadiusArguments(args[4:])
		if err != nil {
			return redis.NewErrorCommand(err)
		}
		options = opt
	} else {
		options = radiusOptions{unitFactor: 1.0}
	}
	radius = radius * options.unitFactor
	sortedSet, err := getSortedSet(db, key)
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	if sortedSet == nil {
		return redis.NilCommand
	}
	// 获取周围的元素
	return geoRadius0(sortedSet, latitude, longitude, radius, options)
}

func execGeoRadiusByMember(db *SingleDB, command redis.Command) *redis.RespCommand {
	args := command.Args()
	if !ValidateArgCount(command.Name(), len(args)) {
		return redis.NewErrorCommand(redis.CreateWrongArgumentNumberError("GEORADIUS"))
	}
	key := string(args[0])
	member := string(args[1])
	// 解析范围半径参数
	radius, err := strconv.ParseFloat(string(args[2]), 64)
	if err != nil {
		return redis.NewErrorCommand(redis.ValueNotFloatError)
	}
	var options radiusOptions
	if len(args) > 3 {
		// 额外参数解析
		opt, err := parseRadiusArguments(args[3:])
		if err != nil {
			return redis.NewErrorCommand(err)
		}
		options = opt
	} else {
		options = radiusOptions{unitFactor: 1.0}
	}
	radius = radius * options.unitFactor
	// 获取sortedSet
	sortedSet, err := getSortedSet(db, key)
	if err != nil {
		return redis.NewErrorCommand(err)
	}
	if sortedSet == nil {
		return redis.NilCommand
	}
	// 从zset获取member的geoHash值，decode出member的坐标
	if element, ok := sortedSet.GetScore(member); !ok {
		return redis.NilCommand
	} else {
		latitude, longitude, _ := geo.Decode(geo.FromUint64(uint64(element.Score)))
		return geoRadius0(sortedSet, latitude, longitude, radius, options)
	}
}

// 解析radius命令参数，返回选项和解析错误
func parseRadiusArguments(args [][]byte) (radiusOptions, error) {
	options := radiusOptions{}
	for _, arg := range args {
		switch strings.ToLower(string(arg)) {
		// 单位换算因子
		case "m":
			options.unitFactor = 1
		case "km":
			options.unitFactor = 1000
		case "mi":
			options.unitFactor = 1 / 0.00062137
		case "ft":
			options.unitFactor = 1 / 3.2808399
		// 返回额外信息参数
		case "withcoord":
			options.withCoord = true
		case "withdist":
			options.withDist = true
		case "withhash":
			options.withHash = true
		case "desc":
			options.sortDesc = true
		case "asc":
			options.sortAsc = true
		default:
			return options, redis.SyntaxError
		}
	}
	return options, nil
}

func geoRadius0(sortedSet *zset.SortedSet, latitude, longitude float64, radiusMeters float64, options radiusOptions) *redis.RespCommand {
	// 获得坐标周围的geoHash块
	ranges := geo.AroundRadius(latitude, longitude, radiusMeters)
	elements := make(map[string]float64)
	// 从zset中找到所有符合ranges块范围的成员
	for _, rg := range ranges {
		elems := sortedSet.RangeByScore(float64(rg[0]), float64(rg[1]), 0, sortedSet.Size(), false, false)
		for _, elem := range elems {
			elements[elem.Member] = elem.Score
		}
	}
	result := make([]radiusResult, 0, len(elements))
	for member, score := range elements {
		// 单个element的结果是一个嵌套的数组
		var nestedResult [][]byte
		buf := geo.FromUint64(uint64(score))
		// 解码geoHash值
		lat, lng, _ := geo.Decode(buf)
		nestedResult = append(nestedResult, redis.Encode(redis.NewBulkStringCommand([]byte(member))))
		var distance float64
		if options.withDist || options.sortAsc || options.sortDesc {
			// 计算距离
			distance = geo.Distance(latitude, longitude, lat, lng) / options.unitFactor
			if options.withDist {
				nestedResult = append(nestedResult, redis.Encode(redis.NewSingleLineCommand([]byte(fmt.Sprintf("%.4f", distance)))))
			}
		}
		if options.withHash {
			// 获取Base32字符串，写入结果数组
			nestedResult = append(nestedResult, redis.Encode(redis.NewBulkStringCommand([]byte(geo.ToString(buf)))))
		}
		if options.withCoord {
			// 格式化经纬度
			nestedResult = append(nestedResult, redis.Encode(redis.NewArrayCommand([][]byte{
				[]byte(fmt.Sprintf("%f", lng)),
				[]byte(fmt.Sprintf("%f", lat)),
			})))
		}
		res := redis.Encode(redis.NewNestedArrayCommand(nestedResult))
		result = append(result, radiusResult{distance: distance, encoded: res})
	}
	// 按照距离值升序或降序排序
	if options.sortAsc {
		sort.Slice(result, func(i, j int) bool {
			return result[i].distance < result[j].distance
		})
	} else if options.sortDesc {
		sort.Slice(result, func(i, j int) bool {
			return result[i].distance > result[j].distance
		})
	}
	arrElements := make([][]byte, len(result))
	for i, res := range result {
		arrElements[i] = res.encoded
	}
	return redis.NewNestedArrayCommand(arrElements)
}
