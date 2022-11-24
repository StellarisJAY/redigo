package geo

import (
	"bytes"
	"encoding/base32"
	"encoding/binary"
)

const (
	minLatitude float64 = -90
	maxLatitude float64 = 90

	minLongitude float64 = -180
	maxLongitude float64 = 180
	base32Table          = "0123456789bcdefghjkmnpqrstuvwxyz"

	// MaxPrecision 最大精度
	MaxPrecision = 32
	// 纬度的最小单位跨度，即最小精度下的一个geoHash二分的纬度跨度
	latitudeUnit float64 = 180.0 / (1 << MaxPrecision)
	// 经度的最小单位跨度
	longitudeUnit float64 = 360.0 / (1 << MaxPrecision)
)

var (
	base32TableReverse = make(map[byte]byte)
	enc                = base32.NewEncoding("0123456789bcdefghjkmnpqrstuvwxyz").WithPadding(base32.NoPadding)
)

func init() {
	for i := 0; i < 32; i++ {
		base32TableReverse[base32Table[i]] = byte(i)
	}
}

// Encode 将坐标转换成geoHash
func Encode(latitude, longitude float64) []byte {
	latitudes := convert(minLatitude, maxLatitude, latitude, MaxPrecision)
	longitudes := convert(minLongitude, maxLongitude, longitude, MaxPrecision)
	buffer := &bytes.Buffer{}
	for i := 0; i < MaxPrecision; i += 4 {
		var sum byte = 0
		k := 0
		for j := 7; j >= 0; j -= 2 {
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

// Around 坐标所在的geoHash块的周围最小精度范围内的所有块
func Around(latitude, longitude float64) [][]byte {
	var result [][]byte
	result = append(result, Encode(latitude, longitude))
	result = append(result, Encode(latitude+latitudeUnit, longitude))
	result = append(result, Encode(latitude-latitudeUnit, longitude))
	result = append(result, Encode(latitude, longitude+longitudeUnit))
	result = append(result, Encode(latitude, longitude-longitudeUnit))
	result = append(result, Encode(latitude+latitudeUnit, longitude+longitudeUnit))
	result = append(result, Encode(latitude+latitudeUnit, longitude-longitudeUnit))
	result = append(result, Encode(latitude-latitudeUnit, longitude+longitudeUnit))
	result = append(result, Encode(latitude-latitudeUnit, longitude-longitudeUnit))
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
