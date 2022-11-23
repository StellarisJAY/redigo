package geo

import (
	"bytes"
	"encoding/binary"
)

const (
	minLatitude float64 = -90
	maxLatitude float64 = 90

	minLongitude float64 = -180
	maxLongitude float64 = 180
	base32Table          = "0123456789bcdefghjkmnpqrstuvwxyz"

	// MaxPrecision 最大精度
	MaxPrecision = 20
	// 纬度的最小单位跨度，即最小精度下的一个geoHash二分的纬度跨度
	latitudeUnit float64 = 180.0 / (1 << MaxPrecision)
	// 经度的最小单位跨度
	longitudeUnit float64 = 360.0 / (1 << MaxPrecision)
)

var (
	base32TableReverse = make(map[byte]byte)
)

func init() {
	for i := 0; i < 32; i++ {
		base32TableReverse[base32Table[i]] = byte(i)
	}
}

// Encode 将坐标转换成geoHash
func Encode(latitude, longitude float64, precision int) []byte {
	if precision > MaxPrecision {
		precision = MaxPrecision
	}
	latitudes := convert(minLatitude, maxLatitude, latitude, precision)
	longitudes := convert(minLongitude, maxLongitude, longitude, precision)
	buffer := &bytes.Buffer{}
	for i := 0; i < precision; i++ {
		buffer.WriteByte(longitudes[i])
		buffer.WriteByte(latitudes[i])
	}
	value := buffer.Bytes()
	var result []byte
	for i := 0; i < len(value); i += 5 {
		var sum byte = 0
		for j := 0; j < 5; j++ {
			sum += value[i+4-j] * (1 << j)
		}
		result = append(result, sum)
	}
	return result
}

func Decode(value []byte) (float64, float64, error) {
	var encoded []byte
	for _, num := range value {
		encoded = append(encoded, numberToBinary(num)...)
	}
	// 奇偶位分离出经度和纬度
	var lats, lngs []byte
	for i := 0; i < len(encoded); i++ {
		if i&1 == 1 {
			lats = append(lats, encoded[i])
		} else {
			lngs = append(lngs, encoded[i])
		}
	}
	// 转换回坐标值
	return convertBack(minLatitude, maxLatitude, lats, len(lats)), convertBack(minLongitude, maxLongitude, lngs, len(lngs)), nil
}

func ToString(buffer []byte) string {
	result := &bytes.Buffer{}
	for _, b := range buffer {
		result.WriteByte(base32Table[b])
	}
	return string(result.Bytes())
}

func FormatUint64(buffer []byte) uint64 {
	return binary.BigEndian.Uint64(buffer)
}

func FromUint64(value uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, value)
	return buf
}

// convert 将坐标值转换成对应精度的geoHash
func convert(min, max, value float64, length int) []byte {
	var converter func(max, min, value float64)
	var result []byte
	converter = func(min, max, value float64) {
		if len(result) == length {
			return
		}
		mid := (max + min) / 2
		if value < mid {
			result = append(result, 0)
			converter(min, mid, value)
		} else {
			result = append(result, 1)
			converter(mid, max, value)
		}
	}
	converter(min, max, value)
	return result
}

func convertBack(min, max float64, value []byte, length int) float64 {
	for i := 0; i < length; i++ {
		mid := (min + max) / 2
		if v := value[i]; v == 0 {
			max = mid
		} else {
			min = mid
		}
	}
	return (min + max) / 2
}

// Around 坐标所在的geoHash块的周围最小精度范围内的所有块
func Around(latitude, longitude float64) [][]byte {
	var result [][]byte
	result = append(result, Encode(latitude, longitude, MaxPrecision))
	result = append(result, Encode(latitude+latitudeUnit, longitude, MaxPrecision))
	result = append(result, Encode(latitude-latitudeUnit, longitude, MaxPrecision))
	result = append(result, Encode(latitude, longitude+longitudeUnit, MaxPrecision))
	result = append(result, Encode(latitude, longitude-longitudeUnit, MaxPrecision))
	result = append(result, Encode(latitude+latitudeUnit, longitude+longitudeUnit, MaxPrecision))
	result = append(result, Encode(latitude+latitudeUnit, longitude-longitudeUnit, MaxPrecision))
	result = append(result, Encode(latitude-latitudeUnit, longitude+longitudeUnit, MaxPrecision))
	result = append(result, Encode(latitude-latitudeUnit, longitude-longitudeUnit, MaxPrecision))
	return result
}

func AroundRadius(latitude, longitude float64, radius float64) [][]byte {
	//angle := radius / (math.Pi * EarthRadius)
	return nil
}

func numberToBinary(num byte) []byte {
	result := make([]byte, 5)
	i := 4
	for num > 0 {
		result[i] = num & 1
		num = num >> 1
		i--
	}
	return result
}
