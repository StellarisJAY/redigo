package geo

import (
	"bytes"
)

const (
	MinLatitude float64 = -90
	MaxLatitude float64 = 90

	MinLongitude float64 = -180
	MaxLongitude float64 = 180
	base32Table          = "0123456789bcdefghjkmnpqrstuvwxyz"

	// 最大精度
	maxPrecision = 15
	// 纬度的最小单位跨度，即最小精度下的一个geoHash二分的纬度跨度
	latitudeUnit float64 = 180.0 / (1 << maxPrecision)
	// 经度的最小单位跨度
	longitudeUnit float64 = 360.0 / (1 << maxPrecision)
)

// Encode 将坐标转换成geoHash
func Encode(latitude, longitude float64, precision int) []byte {
	if precision > maxPrecision {
		precision = maxPrecision
	}
	latitudes := convert(MinLatitude, MaxLatitude, latitude, precision)
	longitudes := convert(MinLongitude, MaxLongitude, longitude, precision)
	buffer := &bytes.Buffer{}
	for i := 0; i < precision; i++ {
		buffer.WriteByte(longitudes[i])
		buffer.WriteByte(latitudes[i])
	}
	result := buffer.Bytes()
	return formatBase32(result)
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

func formatBase32(value []byte) []byte {
	var result []byte
	for i := 0; i < len(value); i += 5 {
		var sum byte = 0
		for j := 0; j < 5; j++ {
			sum += value[i+4-j] * (1 << j)
		}
		result = append(result, base32Table[sum])
	}
	return result
}

// Around 坐标所在的geoHash块的周围最小精度范围内的所有块
func Around(latitude, longitude float64) [][]byte {
	var result [][]byte
	result = append(result, Encode(latitude, longitude, maxPrecision))
	result = append(result, Encode(latitude+latitudeUnit, longitude, maxPrecision))
	result = append(result, Encode(latitude-latitudeUnit, longitude, maxPrecision))
	result = append(result, Encode(latitude, longitude+longitudeUnit, maxPrecision))
	result = append(result, Encode(latitude, longitude-longitudeUnit, maxPrecision))
	result = append(result, Encode(latitude+latitudeUnit, longitude+longitudeUnit, maxPrecision))
	result = append(result, Encode(latitude+latitudeUnit, longitude-longitudeUnit, maxPrecision))
	result = append(result, Encode(latitude-latitudeUnit, longitude+longitudeUnit, maxPrecision))
	result = append(result, Encode(latitude-latitudeUnit, longitude-longitudeUnit, maxPrecision))
	return result
}
