package geo

import (
	"bytes"
	"encoding/base32"
	"encoding/binary"
	"math"
)

const (
	minLatitude float64 = -90
	maxLatitude float64 = 90

	minLongitude float64 = -180
	maxLongitude float64 = 180

	// MaxPrecision 最大精度
	MaxPrecision = 32
)

var (
	enc = base32.NewEncoding("0123456789bcdefghjkmnpqrstuvwxyz").WithPadding(base32.NoPadding)
)

// Encode 将坐标转换成geoHash
func Encode(latitude, longitude float64) []byte {
	return encode0(latitude, longitude, MaxPrecision)
}

func encode0(latitude, longitude float64, precision int) []byte {
	latitudes := convert(minLatitude, maxLatitude, latitude, precision)
	longitudes := convert(minLongitude, maxLongitude, longitude, precision)
	buffer := &bytes.Buffer{}
	for i := 0; i < precision; i += 4 {
		var sum byte = 0
		k := 0
		for j := 7; j >= 0 && i+k < len(latitudes); j -= 2 {
			sum += longitudes[i+k] * (1 << j)
			sum += latitudes[i+k] * (1 << (j - 1))
			k++
		}
		buffer.WriteByte(sum)
	}
	return buffer.Bytes()
}

func Decode(value []byte) (float64, float64, error) {
	// 奇偶位分离出经度和纬度
	var lats, lngs []byte
	for _, num := range value {
		for i := 7; i >= 0; i -= 2 {
			lngs = append(lngs, num&(1<<i))
			lats = append(lats, num&(1<<(i-1)))
		}
	}
	return convertBack(minLatitude, maxLatitude, lats, len(lats)), convertBack(minLongitude, maxLongitude, lngs, len(lngs)), nil
}

func ToString(buffer []byte) string {
	return enc.EncodeToString(buffer)
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

// around 获取坐标所处区块以及周围8个区块，总共9个块，返回geoHash数组
func around(latitude, longitude float64, latitudeUnit, longitudeUnit float64) [][]byte {
	var result [][]byte
	// 坐标所处块
	result = append(result, Encode(latitude, longitude))
	// 上下右左
	result = append(result, Encode(latitude+latitudeUnit, longitude))
	result = append(result, Encode(latitude-latitudeUnit, longitude))
	result = append(result, Encode(latitude, longitude+longitudeUnit))
	result = append(result, Encode(latitude, longitude-longitudeUnit))
	// 右上，左上，右下，左下
	result = append(result, Encode(latitude+latitudeUnit, longitude+longitudeUnit))
	result = append(result, Encode(latitude+latitudeUnit, longitude-longitudeUnit))
	result = append(result, Encode(latitude-latitudeUnit, longitude+longitudeUnit))
	result = append(result, Encode(latitude-latitudeUnit, longitude-longitudeUnit))
	return result
}

// AroundRadius 坐标周围一定范围内的所有geoHash块
func AroundRadius(latitude, longitude float64, radius float64) [][2]uint64 {
	precision := estimatePrecision(radius)
	shift := 1 << precision
	var latUnit, lngUnit = 180.0 / float64(shift), 360.0 / float64(shift)
	result := make([][2]uint64, 9)
	blocks := around(latitude, longitude, latUnit, lngUnit)
	for i, block := range blocks {
		result[i] = hashToRange(block, precision)
	}
	return result
}

func hashToRange(geoHash []byte, precision int) [2]uint64 {
	low := FormatUint64(geoHash)
	high := low + (1 << ((MaxPrecision - precision) * 2))
	return [2]uint64{low, high}
}

// estimatePrecision 通过范围值估计geoHash精度（二分次数）
func estimatePrecision(radius float64) int {
	if radius == 0 {
		return MaxPrecision
	}
	precision := 0
	// 从范围值开始，每次二乘逼近地球赤道长度的一半 (因为precision是二分次数，所以这里使用赤道长度的一半)
	for radius < EarthRadius*math.Pi {
		radius = radius * 2
		precision++
	}
	if precision > MaxPrecision {
		precision = MaxPrecision
	}
	if precision < 1 {
		return 1
	}
	return precision
}
